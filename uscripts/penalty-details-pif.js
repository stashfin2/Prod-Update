const path = require("path");
const mysql = require("../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../logger/logger");

const processJob = async () => {
    let conn;
    try {
        conn = await mysql.getConnections();
        const dataStream = conn.query('select * from pdpif_t where is_done = 0').stream();
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
            const loanIds = data.map(item => item.loan_id).join(',');
            let copyCreds = JSON.parse(JSON.stringify(data))
            const whereClauses = copyCreds.map(item => `(loan_id = ${item.loan_id} AND customer_id = ${item.customer_id} AND is_paid = 0)`).join(' OR ')
            const whereClausesForUpdate = copyCreds.map(item => `(loan_id = ${item.loan_id} AND customer_id = ${item.customer_id})`).join(' OR ')
            console.log("loan_ids and customer_ids:", whereClauses)
            const loanTapePenalties = await mysql.query(
                `SELECT * FROM penalty_details_pif WHERE ${whereClauses}`, 
                [], 
                'loan-tape'
            );
            const deleteOp = await deleteDataFromPenaltyDetailsPif(loanIds, whereClauses);
            const insertOp =  await insertPenaltiesPif(loanTapePenalties)
            const lpfp = await mysql.query(`update pdpif_t set is_done = 1 where ${whereClausesForUpdate}`, [], 'loan-tape')
            await Promise.all([loanTapePenalties, deleteOp, insertOp]).then((res) =>{
                logger.info("All operations for data tranasfer for penalty_details_pif from loan-tape to prod has been done")
            }).catch((error) => {
                logger.error("Error while transfering the above batch in the prod db:", error)
            })
           
        } catch (error) {
            logger.error("Error in processing the penalty-details-pif: ", error);
        }
        callback();
    }
});


const insertPenaltiesPif = async (data) => {
    try{
        logger.info(`Initiating the insertion process in the penalty_details table`)
        const insertQuery = `INSERT INTO penalty_details_pif (customer_id,loan_id,instalment_id,instalment_number,penalty_amount,penalty_date,penalty_received,penalty_received_date,outstanding_penalty,is_paid,panalty_type_id,payment_id,foreclose_status,status,create_date) VALUES ?`
        const penaltyArray = createPenaltyCompatibleArray(data)
        if(penaltyArray.length === 0){
            logger.info("there are no penalties present to insert with the given loanIds:", loanIds)
            return true
        }
        const result = await mysql.query(insertQuery, [penaltyArray], 'prod')
        logger.info(`Successfully inserted ${result.affectedRows} penalties inside the penalty_details_pif table`)
        return result
    }catch(error){
        logger.error(`Error while inserting the penalties inside the penalty details table:-`, error)
        throw error
    }
}

const deleteDataFromPenaltyDetailsPif = async (loanIds, whereClauses) => {
    try {
        logger.info(`Deleting penalties from penalty_details table for loan_ids and is_paid = 0: ${loanIds}`);
        const result = await mysql.query(
            `DELETE FROM penalty_details_pif WHERE ${whereClauses}`, 
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
