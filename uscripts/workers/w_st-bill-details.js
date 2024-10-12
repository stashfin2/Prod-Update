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
        "SELECT COUNT(*) as count FROM sbd_t WHERE is_done = 0"
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
        "SELECT * FROM sbd_t WHERE is_done = 0 LIMIT ? OFFSET ?",
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
    try {
      const loanIds = data.map((item) => item.loan_id).join(",");
      let copyCreds = JSON.parse(JSON.stringify(data));
      const whereClauses = copyCreds
        .map(
          (item) =>
            `(loan_id = ${item.loan_id} AND customer_id = ${item.customer_id} AND  paid_status= 0)`
        )
        .join(" OR ");
      const whereClausesForUpdate = copyCreds
        .map(
          (item) =>
            `(loan_id = ${item.loan_id} AND customer_id = ${item.customer_id})`
        )
        .join(" OR ");
      console.log("loan_ids and customer_ids:", whereClauses);
      const loanTapePenalties = await mysql.query(
        `SELECT * FROM st_bill_detail WHERE ${whereClauses}`,
        [],
        "loan-tape"
      );
      const deleteOp = await deleteDataFromBillDetail(
        loanIds,
        whereClausesForUpdate
      );
      const insertOp = await insertStBillDetails(loanTapePenalties, loanIds);
      const lpfp = await mysql.query(
        `update sbd_t set is_done = 1 where ${whereClausesForUpdate}`,
        [],
        "loan-tape"
      );
      logger.info(
        "All operations for data transfer for st_bill_detail from loan-tape to prod has been done"
      );
    } catch (error) {
      //await conn.rollback();
      logger.error(`Error processing chunk:`, error);
      throw error;
    }
  };

  const insertStBillDetails = async (data, loanIds) => {
    try {
      logger.info(
        `Initiating the insertion process in the st_bill_detail table`
      );
      const insertQuery = `INSERT INTO st_bill_detail (customer_id, loan_id, approved_amount, bill_amount, processing_fee, credit_shield_fee, cgst, igst, sgst, gst, upfront, bill_date, paid_amount, outstanding_amount, emi_paid, tenure, create_date, update_date, status, paid_status, processing_fees_rate) VALUES  ?`;
      const penaltyArray = createBillCompatibleArray(data);
      if (penaltyArray.length === 0) {
        logger.info(
          "there are no bills present to insert with the given loanIds:",
          loanIds
        );
        return true;
      }
      const result = await mysql.query(insertQuery, [penaltyArray], "prod");
      logger.info(
        `Successfully inserted ${result.affectedRows} bills inside the st_bill_detail table`
      );
      return result;
    } catch (error) {
      logger.error(
        `Error while inserting the bills inside the st_bill_detail table:-`,
        error
      );
      throw error;
    }
  };

  const deleteDataFromBillDetail = async (loanIds, whereClauses) => {
    try {
      logger.info(
        `Deleting bills from st_bill_detail table for loan_ids and paid_status = 0: ${loanIds}`
      );
      const result = await mysql.query(
        `DELETE FROM st_bill_detail WHERE ${whereClauses}`,
        [],
        "prod"
      );
      logger.info(
        `Deleted ${result.affectedRows} rows from st_bill_detail table`
      );
      return result;
    } catch (error) {
      logger.error("Error deleting payments: ", error);
      throw error;
    }
  };

  const createBillCompatibleArray = (arrayOfObjects) => {
    return arrayOfObjects.map((obj) => [
      obj.customer_id,
      obj.loan_id,
      obj.approved_amount,
      obj.bill_amount,
      obj.processing_fee,
      obj.credit_shield_fee,
      obj.cgst,
      obj.igst,
      obj.sgst,
      obj.gst,
      obj.upfront,
      obj.bill_date,
      obj.paid_amount,
      obj.outstanding_amount,
      obj.emi_paid,
      obj.tenure,
      obj.create_date,
      obj.update_date,
      obj.status,
      obj.paid_status,
      obj.processing_fees_rate,
    ]);
  };

  processWorker();
}
