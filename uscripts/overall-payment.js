const path = require("path");
const mysql = require("../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../logger/logger");

let batchCount = 0
let inProgressCount = 0
let dataCount = 0

const processJob = async () => {
    let conn;
    try {
        conn = await mysql.getConnections();
        const dataStream = conn.query('SELECT * FROM st_ksf_customer where loan_id = 417638').stream();
        await pipeline(dataStream, batchStream, createProcessStream());
        process.exit(0);
    } catch (error) {
        logger.error("Error while initiating the amortization process");
        
    } finally {
        if (conn) conn.release();
    }
};

const batchStream = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
        try {
            this.buffer = this.buffer || [];
            this.buffer = this.buffer.concat(chunk);
            if (this.buffer.length >= Number(process.env.STREAM_BATCH_SIZE || 100)) {
                dataCount = dataCount + this.buffer.length;
                batchCount = batchCount + 1;
                logger.info(`Batch count:>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>${batchCount}`);
                this.push(this.buffer);
                this.buffer = [];
            }

            callback(); // Proceed to the next chunk
        } catch (error) {
            logger.error("Error in the transform function: ", error);
            callback(error); // Pass the error to the callback
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


const createProcessStream = () => new Transform({
    objectMode: true,
    async transform(data, encoding, callback) {
        try {
            await getAndUpdatePayments(data);
        } catch (error) {
            logger.error("Error in processing payments: ", error);
        }
    //    callback();
    }
});

const getAndUpdatePayments = async (data) => {
    const loanIds = data.map(item => item.loan_id).join(',');
    // let connection = await mysql.getConnections('prod')
    // connection = await mysql.beginTransaction(connection)
    try {
        const loanTapePayments = await mysql.query(
            `SELECT * FROM overall_payment WHERE loan_id IN (${loanIds}) ORDER BY id asc`, 
            [], 
            'loan-tape'
        );
        await deleteDataFromOverallPayment(loanIds, {});
        const paymentsData = createOverallPaymentCompatibleArray(loanTapePayments);
        logger.info('Starting insertion into overall_payment table');
        const result = await mysql.query(
            `INSERT INTO overall_payment (customer_id, loan_id, amt_payment, received_date, cheque_number, urm_no, payment_channel, transaction_id, ref_no, utr_no, neft_bank, presentation_status, bounce_reason, presentation_date, create_date, extra_amount, extra_amount_pif, remarks, add_user_id, update_user_id, is_delete, is_refund, update_date, transaction_commit_status) VALUES ?`,
            [paymentsData],
            'prod'
        );
        logger.info(`Inserted ${result.affectedRows} rows into overall_payment table`);
        // await mysql.rollback(connection)
        return result;
    } catch (error) {
        logger.error("Error while getting and updating payments: ", error);
        // await mysql.rollback(connection)
        throw error;
    }
};

const deleteDataFromOverallPayment = async (loanIds, connection) => {
    try {
        logger.info(`Deleting payments with loan_ids: ${loanIds}`);
        const result = await mysql.query(
            `DELETE FROM overall_payment WHERE loan_id IN (${loanIds})`, 
            [], 
            'prod'
        );
        logger.info(`Deleted ${result.affectedRows} rows from overall_payment table`);
        return result;
    } catch (error) {
        logger.error("Error deleting payments from overall_payments table: ", error);
        throw error;
    }
};

const createOverallPaymentCompatibleArray = (payments) => {
    try {
        return payments.map(payment => [
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
            payment.remarks,
            payment.add_user_id,
            payment.update_user_id,
            payment.is_delete,
            payment.is_refund,
            payment.update_date,
            payment.transaction_commit_status
        ]);
    } catch (error) {
        logger.error("Error creating compatible array for payments: ", error);
        throw error;
    }
};

processJob();
