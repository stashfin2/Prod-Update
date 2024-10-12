const path = require("path");
const mysql = require("../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../logger/logger");

let batchCount = 0;
let inProgressCount = 0;
let dataCount = 0;
let streamcount = 0;

const processJob = async () => {
  try {
    const conn = await mysql.getConnections();
    const dataStream = conn
      .query("SELECT * FROM fip_t WHERE is_done = 0", [])
      .stream();

    const batchSize = Number(process.env.STREAM_BATCH_SIZE || 100);
    console.log("BATCH SIZE", batchSize);
    await pipeline(dataStream, batchStream(batchSize), createProcessStream());

    if (conn) conn.release();
    process.exit(0);
  } catch (error) {
    logger.error(
      "Error while initiating the insertion process in installmentfip:",
      error
    );
    throw error;
  }
};

const batchStream = (batchSize) =>
  new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      streamcount += 1;
      console.log("stream count:", streamcount);
      try {
        this.buffer = this.buffer || [];
        this.buffer.push(chunk);
        if (this.buffer.length >= batchSize) {
          dataCount += this.buffer.length;
          batchCount += 1;
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
      inProgressCount += 1;
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

        logger.info("updating loan_tape_data table for committing status");
        await mysql.query(
          `UPDATE fip_t SET is_done = 1 WHERE ${whereClauses}`,
          [],
          "loan-tape"
        );

        logger.info(
          `Successfully performed all the operations and starting with another batch`
        );
        callback();
      } catch (error) {
        logger.error(
          `Error while getting and inserting in installmentfip:`,
          error
        );
        callback();
      }
    },
  });

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

processJob();
