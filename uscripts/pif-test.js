const path = require("path");
const mysql = require("../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../logger/logger");

let batchCount = 0;
let inProgressCount = 0;
let dataCount = 0;

const processJob = async () => {
  try {
    const conn = await mysql.getConnections();
    const dataStream = conn
      .query("SELECT * FROM pif_t WHERE is_done = 0", [])
      .stream();
    await pipeline(
      dataStream,
      batchStream(Number(process.env.STREAM_BATCH_SIZE || 100)),
      createProcessStream()
    );
    if (conn) conn.release();
    process.exit(0);
  } catch (error) {
    logger.error("Error while initiating the amortization process:", error);
  }
};

const batchStream = (batchSize) =>
  new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      try {
        this.buffer = this.buffer || [];
        this.buffer.push(chunk);
        if (this.buffer.length >= batchSize) {
          dataCount += this.buffer.length;
          batchCount++;
          logger.info(
            `Batch count:>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>${batchCount}`
          );
          this.push(this.buffer);
          this.buffer = [];
        }
        callback();
      } catch (error) {
        logger.error("Error in the transform function: ", error);
        callback(error);
      }
    },
    flush(callback) {
      try {
        if (this.buffer && this.buffer.length > 0) {
          this.push(this.buffer);
        }
        callback();
      } catch (error) {
        logger.error("Error in the flush function: ", error);
        callback(error);
      }
    },
  });

const createProcessStream = () =>
  new Transform({
    objectMode: true,
    async transform(data, encoding, callback) {
      inProgressCount++;
      try {
        const loanIds = data.map((each) => each.loan_id);
        const loanIdString = loanIds.join(",");
        const whereClauses = data
          .map(
            (item) =>
              `(customer_id = ${item.customer_id} AND loan_id = ${item.loan_id})`
          )
          .join(" OR ");

        const [loanTapeInstallments, deleteDataFromProd] = await Promise.all([
          getInstallments(loanIdString),
          deleteInstallmentsFromProd(loanIdString),
        ]);

        await insertInstallments(loanTapeInstallments);

        logger.info("Updating pif_t table for committing status");
        await mysql.query(
          `UPDATE pif_t SET is_done = 1 WHERE ${whereClauses}`,
          [],
          "loan-tape"
        );

        logger.info(`Successfully performed all operations for batch`);
      } catch (error) {
        logger.error(`Error while processing batch:`, error);
      }
      callback();
    },
  });

const deleteInstallmentsFromProd = async (loanIdString) => {
  try {
    logger.info(
      "Initiating installments deletion from installment_pif table in production database"
    );
    const deleteDataFromProd = `DELETE FROM installment_pif WHERE loan_id IN (${loanIdString})`;
    const deleteOp = await mysql.query(deleteDataFromProd, [], "prod");
    logger.info(
      `Successfully completed installments deletion from production database`
    );
    return deleteOp;
  } catch (error) {
    logger.error(
      `Error while deleting installments from installment_pif table for (${loanIdString}):`,
      error
    );
    throw error;
  }
};

const getInstallments = async (loanIdString) => {
  try {
    logger.info(`Getting installments from Loan-Tape database for inserting`);
    const loanTapeInstallments = await mysql.query(
      `SELECT * FROM installment_pif WHERE loan_id IN (${loanIdString}) AND is_delete = 0`,
      [],
      "loan-tape"
    );
    logger.info(`Got installments from Loan-Tape database`);
    console.log("xyz");
    return loanTapeInstallments;
  } catch (error) {
    logger.error(
      `Error while getting installments from Loan-tape database for (${loanIdString}):`,
      error
    );
    //throw error;
  }
};

const insertInstallments = async (loanTapeInstallments) => {
  try {
    const insertInstallmentsQuery = `INSERT INTO installment_pif (
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
    logger.error(`Error while inserting installments in prod database:`, error);
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
      `Error while creating installments compatible array for installment_pif table:`,
      error
    );
    throw error;
  }
};

processJob();
