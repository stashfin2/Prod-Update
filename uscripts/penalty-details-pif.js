const path = require("path");
const mysql = require("../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../logger/logger");

const processJob = async () => {
    let conn;
    try {
        conn = await mysql.getConnections();
        const dataStream = conn.query('SELECT * FROM st_ksf_customer').stream();
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
        this.buffer = (this.buffer || []).concat(chunk);
        if (this.buffer.length >= Number(process.env.STREAM_BATCH_SIZE || 100)) {
            logger.info(`Processing batch with size: ${this.buffer.length}`);
            this.push(this.buffer);
            this.buffer = [];
        }
        callback();
    },
    flush(callback) {
        if (this.buffer.length > 0) this.push(this.buffer);
        callback();
    }
});

const createProcessStream = () => new Transform({
    objectMode: true,
    async transform(data, encoding, callback) {
        try {
            await getAndUpdatePayments(data);
        } catch (error) {
            logger.error("Error in processing payments: ", error);
        }
        callback();
    }
});

const getAndUpdatePayments = async (data) => {
    const loanIds = data.map(item => item.loan_id).join(',');
    try {
        const loanTapePenalties = await mysql.query(
            `SELECT * FROM penalty_details_pif WHERE loan_id IN (${loanIds})  and is_paid = 0`, 
            [], 
            'loan-tape'
        );
        await deleteDataFromPenaltyDetailsPif(loanIds);
        await insertPenaltiesPif(loanTapePenalties)
    
        return result;
    } catch (error) {
        logger.error("Error while getting and updating payments: ", error);
        throw error;
    }
};

const insertPenaltiesPif = async (data) => {
    try{
        logger.info(`Initiating the insertion process in the penalty_details table`)
        const insertQuery = `INSERT INTO penalty_details_pif (customer_id,loan_id,instalment_id,instalment_number,penalty_amount,penalty_date,penalty_received,penalty_received_date,outstanding_penalty,is_paid,panalty_type_id,payment_id,foreclose_status,status,create_date) VALUES ?`
        const penaltyArray = createPenaltyCompatibleArray(data)
        const result = await mysql.query(insertQuery, [penaltyArray], 'prod')
        logger.info(`Successfully inserted ${result.affectedRows} penalties inside the penalty_details_pif table`)
        return result
    }catch(error){
        logger.error(`Error while inserting the penalties inside the penalty details table:-`, error)
        throw error
    }
}

const deleteDataFromPenaltyDetailsPif = async (loanIds) => {
    try {
        logger.info(`Deleting penalties from penalty_details table for loan_ids and is_paid = 0: ${loanIds}`);
        const result = await mysql.query(
            `DELETE FROM penalty_details_pif WHERE loan_id IN (${loanIds}) and is_paid = 0`, 
            [], 
            'prod'
        );
        logger.info(`Deleted ${result.affectedRows} rows from penalty_details_pif table`);
        return result;
    } catch (error) {
        logger.error("Error deleting payments: ", error);
        throw error;
    }
};

const createPenaltyCompatibleArray = (penalties) => {
    try {
        return penalties.map(penalty => [
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
            penalty.panalty_type_id,   // Assuming there's a typo in "penalty_type_id"
            penalty.payment_id,
            penalty.foreclose_status,
            penalty.status,
            penalty.create_date
        ]);
    } catch (error) {
        logger.error("Error creating compatible array for penalties: ", error);
        throw error;
    }
};


processJob();
