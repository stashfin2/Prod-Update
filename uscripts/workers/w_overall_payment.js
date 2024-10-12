const path = require("path");
const mysql = require("../../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../../logger/logger");
const {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} = require("worker_threads");
const os = require("os");

const BATCH_SIZE = Number(process.env.STREAM_BATCH_SIZE || 1000);
const NUM_WORKERS = 20 || os.cpus().length;
console.log("Number of initial workers", NUM_WORKERS);

if (isMainThread) {
  let activeWorkers = 0;
  const updateActiveWorkers = (delta) => {
    activeWorkers += delta;
    logger.info(`Active workers: ${activeWorkers}`);
  };

  const processJob = async () => {
    try {
      //const conn = await mysql.getConnections();
      const [totalCount] = await mysql.query(
        "SELECT COUNT(*) as count FROM op_t WHERE is_done = 0"
      );
      console.log("Total count:", totalCount);
      const total = totalCount.count;

      const workerPromises = [];
      for (let i = 0; i < NUM_WORKERS; i++) {
        const worker = new Worker(__filename, {
          workerData: {
            workerId: i,
            batchSize: Math.ceil(total / NUM_WORKERS),
            offset: i * Math.ceil(total / NUM_WORKERS),
          },
        });

        worker.on("online", () => updateActiveWorkers(1));
        worker.on("exit", () => updateActiveWorkers(-1));

        workerPromises.push(
          new Promise((resolve, reject) => {
            worker.on("message", resolve);
            worker.on("error", reject);
            worker.on("exit", (code) => {
              if (code !== 0)
                reject(new Error(`Worker stopped with exit code ${code}`));
            });
          })
        );
      }

      await Promise.all(workerPromises);

      logger.info("All workers completed successfully");

      process.exit(0);
    } catch (error) {
      logger.error("Error in main thread:", error);
      process.exit(1);
    }
  };

  processJob();
} else {
  const processWorker = async () => {
    const { workerId, batchSize, offset } = workerData;

    try {
      const rows = await mysql.query(
        "SELECT * FROM op_t WHERE is_done = 0 LIMIT ? OFFSET ?",
        [batchSize, offset]
      );

      const chunks = [];
      for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        chunks.push(rows.slice(i, i + BATCH_SIZE));
      }

      for (const chunk of chunks) {
        await processChunk(chunk);
      }

      logger.info(
        `Worker ${workerId} completed processing ${rows.length} rows`
      );
      parentPort.postMessage("done");
    } catch (error) {
      logger.error(`Error in worker ${workerId}:`, error);
      parentPort.postMessage("error");
    } finally {
      //if (conn) conn.release();
    }
  };

  const processChunk = async (data) => {
    const loanIds = data.map((item) => item.loan_id).join(",");
    const whereClauses = data
      .map(
        (item) =>
          `(loan_id = ${item.loan_id} AND customer_id = ${item.customer_id})`
      )
      .join(" OR ");

    //await conn.beginTransaction();

    try {
      const [loanTapePayments, deleteOp] = await Promise.all([
        mysql.query(
          `SELECT * FROM overall_payment WHERE ${whereClauses} ORDER BY id asc`,
          [],
          "loan-tape"
        ),
        deleteDataFromOverallPayment(whereClauses, {}),
      ]);

      const paymentsData =
        createOverallPaymentCompatibleArray(loanTapePayments);
      logger.info("Starting insertion into overall_payment table");

      const result = await mysql.query(
        `INSERT INTO overall_payment (customer_id, loan_id, amt_payment, received_date, cheque_number, urm_no, payment_channel, transaction_id, ref_no, utr_no, neft_bank, presentation_status, bounce_reason, presentation_date, create_date, extra_amount, extra_amount_pif, remarks, add_user_id, update_user_id, is_delete, is_refund, update_date, transaction_commit_status) VALUES ?`,
        [paymentsData],
        "prod"
      );
      logger.info(
        `Inserted ${result.affectedRows} rows into overall_payment table`
      );

      logger.info("updating loan_tape_data table for committing status");
      await mysql.query(
        `update op_t set is_done = 1 where ${whereClauses}`,
        [],
        "loan-tape"
      );

      logger.info(
        "Successfully transfered the batch to the overall_payments table in the prod"
      );
    } catch (error) {
      //await conn.rollback();
      logger.error(`Error processing chunk:`, error);
      throw error;
    }
  };

  const deleteDataFromOverallPayment = async (whereClauses, connection) => {
    try {
      //logger.info(`Deleting payments with loan_ids: ${whereClauses}`);
      const result = await mysql.query(
        `DELETE FROM overall_payment WHERE ${whereClauses}`,
        [],
        "prod"
      );
      logger.info(
        `Deleted ${result.affectedRows} rows from overall_payment table`
      );
      return result;
    } catch (error) {
      logger.error(
        "Error deleting payments from overall_payments table: ",
        error
      );
      throw error;
    }
  };

  const createOverallPaymentCompatibleArray = (payments) => {
    try {
      return payments.map((payment) => [
        payment.customer_id,
        payment.loan_id,
        payment.amt_payment,
        payment.received_date,
        payment.cheque_number,
        payment.urm_no,
        payment.payment_channel,
        payment.transaction_id,
        payment.ref_no,
        payment.utr_no,
        payment.neft_bank,
        payment.presentation_status,
        payment.bounce_reason,
        payment.presentation_date,
        payment.create_date,
        payment.extra_amount,
        payment.extra_amount_pif,
        "others",
        payment.add_user_id,
        payment.update_user_id,
        payment.is_delete,
        payment.is_refund,
        payment.update_date,
        payment.transaction_commit_status,
      ]);
    } catch (error) {
      logger.error("Error creating compatible array for payments: ", error);
      throw error;
    }
  };

  processWorker();
}
