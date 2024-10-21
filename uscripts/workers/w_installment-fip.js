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
        "SELECT COUNT(*) as count FROM fip_t WHERE is_done = 0"
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
        "SELECT * FROM fip_t WHERE is_done = 0 LIMIT ? OFFSET ?",
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
    const loanIds = data.map((each) => each.loan_id);
    const loanIdString = loanIds.join(",");
    const whereClauses = data
      .map(
        (item) =>
          `(customer_id = ${item.customer_id} AND loan_id = ${item.loan_id})`
      )
      .join(" OR ");

    //await conn.beginTransaction();

    try {
      const [loanTapeInstallments, deleteDataFromProd] = await Promise.all([
        getInstallments(loanIdString),
        deleteInstallmentsFromProd(loanIdString),
      ]);

      await insertInstallments(loanTapeInstallments);

      logger.info("updating loan_tape_data table for committing status");
      await mysql.query(
        `UPDATE fip_t SET is_done = 1 WHERE ${whereClauses}`,
        [],
        "loan-tape"
      );

      logger.info(
        `Successfully performed all the operations and starting with another batch`
      );
    } catch (error) {
      //await conn.rollback();
      logger.error(`Error processing chunk:`, error);
      throw error;
    }
  };

  const deleteInstallmentsFromProd = async (loanIdString) => {
    try {
      logger.info(
        "Initiating the installments deletion process from installment_fip table in the production database"
      );
      const deleteDataFromProd = `DELETE FROM installment_fip WHERE loan_id IN (${loanIdString})`;
      const deleteOp = await mysql.query(deleteDataFromProd, [], "prod");
      logger.info(
        `Successfully completed the installments deletion process from the production database`
      );
      return deleteOp;
    } catch (error) {
      logger.error(
        `Error while deleting installments from the installment-fip table for (${loanIdString})`
      );
      logger.error("Cause of the error is:>", error);
      throw error;
    }
  };

  const getInstallments = async (loanIdString) => {
    try {
      logger.info(
        `Getting the installments from the Loan-Tape database for inserting`
      );
      const loanTapeInstallments = await mysql.query(
        `SELECT * FROM installment_fip WHERE loan_id IN (${loanIdString}) AND is_delete = 0`,
        [],
        "loan-tape"
      );
      logger.info(`Got the installments from the Loan-Tape database`);
      return loanTapeInstallments;
    } catch (error) {
      logger.error(
        `Error while getting installments from the Loan-tape database for (${loanIdString}).`
      );
      logger.error(
        `Error which is Encountered while getting the installments from the Loan-tape database is ${error}`
      );
      throw error;
    }
  };

  const insertInstallments = async (loanTapeInstallments) => {
    try {
      const insertInstallmentsQuery = `INSERT INTO installment_fip (
        entity_id, customer_id, loan_id, inst_number, inst_amount, inst_principal, inst_interest, inst_fee, 
        inst_discount, received_principal, received_interest, received_fee, amount_outstanding_interest, 
        amount_outstanding_principal, received_extra_charges, inst_date, create_date, last_paying_date, 
        starting_balance, ending_balance, days_past_due, days_past_due_tolerance, emi_status_id, foreclose_status, 
        foreclose_date, update_date, add_user_id, update_user_id, inst_status, is_delete, customer_facing, 
        amort_adjusment, payment_id, cashback, version) VALUES ?`;
      const installments = mapDataToInsertArray(loanTapeInstallments);
      const insertOp = await mysql.query(
        insertInstallmentsQuery,
        [installments],
        "prod"
      );
      return insertOp;
    } catch (error) {
      logger.error(
        `Error while inserting the installments in the prod database:`,
        error
      );
      throw error;
    }
  };

  const mapDataToInsertArray = (instalments) => {
    try {
      return instalments.map((data) => [
        data.entity_id,
        data.customer_id,
        data.loan_id,
        data.inst_number,
        data.inst_amount,
        data.inst_principal,
        data.inst_interest,
        data.inst_fee,
        data.inst_discount,
        data.received_principal,
        data.received_interest,
        data.received_fee,
        data.amount_outstanding_interest,
        data.amount_outstanding_principal,
        data.received_extra_charges,
        data.inst_date,
        data.create_date,
        data.last_paying_date,
        data.starting_balance,
        data.ending_balance,
        data.days_past_due,
        data.days_past_due_tolerance,
        data.emi_status_id,
        data.foreclose_status,
        data.foreclose_date,
        data.update_date,
        data.add_user_id,
        data.update_user_id,
        data.inst_status,
        data.is_delete,
        data.customer_facing,
        data.amort_adjusment,
        data.payment_id,
        data.cashback,
        data.version,
      ]);
    } catch (error) {
      logger.error(
        `Error while creating the installments compatible array for installment-fip table:`,
        error
      );
      throw error;
    }
  };

  processWorker();
}
