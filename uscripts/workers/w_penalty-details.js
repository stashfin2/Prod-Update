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
        "SELECT COUNT(*) as count FROM pd_t WHERE is_done = 0"
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
        "SELECT * FROM pd_t WHERE is_done = 0 LIMIT ? OFFSET ?",
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
    let copyCreds = JSON.parse(JSON.stringify(data));
    const whereClauses = copyCreds
      .map(
        (item) =>
          `(loan_id = ${item.loan_id} AND customer_id = ${item.customer_id} AND is_paid = 0)`
      )
      .join(" OR ");
    const whereClausesForUpdate = copyCreds
      .map(
        (item) =>
          `(loan_id = ${item.loan_id} AND customer_id = ${item.customer_id})`
      )
      .join(" OR ");

    //await conn.beginTransaction();

    try {
      const [loanTapePenalties, deleteOp] = await Promise.all([
        mysql.query(
          `SELECT * FROM penalty_details WHERE ${whereClauses}`,
          [],
          "loan-tape"
        ),
        deleteDataFromPenaltyDetails(loanIds, whereClauses),
      ]);

      const insertOp = await insertPenalties(loanTapePenalties, loanIds);
      const lpdp = await mysql.query(
        `update pd_t set is_done = 1 where ${whereClausesForUpdate}`,
        [],
        "loan-tape"
      );

      logger.info(
        "All operations for data tranasfer for penalty details from loan-tape to prod has been done"
      );
    } catch (error) {
      //await conn.rollback();
      logger.error(`Error processing chunk:`, error);
      throw error;
    }
  };

  const insertPenalties = async (data, loanIds) => {
    try {
      logger.info(
        `Initiating the insertion process in the penalty_details table`
      );
      const insertQuery = `INSERT INTO penalty_details (customer_id,loan_id,instalment_id,instalment_number,penalty_amount,penalty_date,penalty_received,penalty_received_date,outstanding_penalty,is_paid,panalty_type_id,payment_id,foreclose_status,status,create_date) VALUES ?`;
      const penaltyArray = createPenaltyCompatibleArray(data);
      if (penaltyArray.length === 0) {
        logger.info(
          "there are no penalties present to insert with the given loanIds:",
          loanIds
        );
        return true;
      }
      const result = await mysql.query(insertQuery, [penaltyArray], "prod");
      logger.info(
        `Successfully inserted ${result.affectedRows} penalties inside the penalty details table`
      );
      return result;
    } catch (error) {
      logger.error(
        `Error while inserting the penalties inside the penalty details table:-`,
        error
      );
      throw error;
    }
  };

  const deleteDataFromPenaltyDetails = async (loanIds, whereClauses) => {
    try {
      logger.info(
        `Deleting penalties from penalty-details for loan_ids and is_paid = 0: ${loanIds}`
      );
      const result = await mysql.query(
        `DELETE FROM penalty_details WHERE ${whereClauses}`,
        [],
        "prod"
      );
      logger.info(
        `Deleted ${result.affectedRows} rows from penalty_details table`
      );
      return result;
    } catch (error) {
      logger.error("Error deleting payments: ", error);
      throw error;
    }
  };

  const createPenaltyCompatibleArray = (penalties) => {
    try {
      return penalties.map((penalty) => [
        penalty.customer_id,
        penalty.loan_id,
        penalty.instalment_id,
        penalty.instalment_number,
        penalty.penalty_amount,
        penalty.penalty_date,
        penalty.penalty_received,
        penalty.penalty_received_date,
        penalty.outstanding_penalty,
        penalty.is_paid,
        penalty.panalty_type_id, // Assuming there's a typo in "penalty_type_id"
        penalty.payment_id,
        penalty.foreclose_status,
        penalty.status,
        penalty.create_date,
      ]);
    } catch (error) {
      logger.error("Error creating compatible array for penalties: ", error);
      throw error;
    }
  };

  processWorker();
}
