const path = require("path");
const mysql = require("../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../logger/logger");

const processJob = async () => {
    let conn;
    try {
        conn = await mysql.getConnections();
        const dataStream = conn.query('select * from sbd_t where is_done = 0').stream();
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
            const whereClauses = copyCreds.map(item => `(loan_id = ${item.loan_id} AND customer_id = ${item.customer_id} AND  paid_status= 0)`).join(' OR ')
            const whereClausesForUpdate = copyCreds.map(item => `(loan_id = ${item.loan_id} AND customer_id = ${item.customer_id})`).join(' OR ')
            console.log("loan_ids and customer_ids:", whereClauses)
            const loanTapePenalties = await mysql.query(
                `SELECT * FROM st_bill_detail WHERE ${whereClauses}`, 
                [], 
                'loan-tape'
            );
            const deleteOp = await deleteDataFromBillDetail(loanIds, whereClausesForUpdate);
            const insertOp =  await insertStBillDetails(loanTapePenalties, loanIds)
            const lpfp = await mysql.query(`update sbd_t set is_done = 1 where ${whereClausesForUpdate}`, [], 'loan-tape')
            await Promise.all([loanTapePenalties, deleteOp, insertOp, lpfp]).then((res) =>{
                logger.info("All operations for data transfer for st_bill_detail from loan-tape to prod has been done")
            }).catch((error) => {
                logger.error("Error while transfering the above batch in the prod db:", error)
            })
           
        } catch (error) {
            logger.error("Error in processing the st_bill_detail: ", error);
        }
        callback();
    }
});


const insertStBillDetails = async (data, loanIds) => {
    try{
        logger.info(`Initiating the insertion process in the st_bill_detail table`)
        const insertQuery = `INSERT INTO st_bill_detail (customer_id, loan_id, approved_amount, bill_amount, processing_fee, credit_shield_fee, cgst, igst, sgst, gst, upfront, bill_date, paid_amount, outstanding_amount, emi_paid, tenure, create_date, update_date, status, paid_status, processing_fees_rate) VALUES  ?`
        const penaltyArray = createBillCompatibleArray(data)
        if(penaltyArray.length === 0){
            logger.info("there are no bills present to insert with the given loanIds:", loanIds)
            return true
        }
        const result = await mysql.query(insertQuery, [penaltyArray], 'prod')
        logger.info(`Successfully inserted ${result.affectedRows} bills inside the st_bill_detail table`)
        return result
    }catch(error){
        logger.error(`Error while inserting the bills inside the st_bill_detail table:-`, error)
        throw error
    }
}

const deleteDataFromBillDetail = async (loanIds, whereClauses) => {
    try {
        logger.info(`Deleting bills from st_bill_detail table for loan_ids and paid_status = 0: ${loanIds}`);
        const result = await mysql.query(
            `DELETE FROM st_bill_detail WHERE ${whereClauses}`, 
            [], 
            'prod'
        );
        logger.info(`Deleted ${result.affectedRows} rows from st_bill_detail table`);
        return result;
    } catch (error) {
        logger.error("Error deleting payments: ", error);
        throw error;
    }
};

const createBillCompatibleArray = (arrayOfObjects) => {
    return arrayOfObjects.map(obj => [
        obj.customer_id,              
        obj.loan_id ,                  
        obj.approved_amount ,           
        obj.bill_amount ,               
        obj.processing_fee ,            
        obj.credit_shield_fee,            
        obj.cgst ,                      
        obj.igst ,                      
        obj.sgst ,                      
        obj.gst ,                       
        obj.upfront ,                   
        obj.bill_date ,                 
        obj.paid_amount,                  
        obj.outstanding_amount ,        
        obj.emi_paid ,                  
        obj.tenure ,                    
        obj.create_date ,               
        obj.update_date ,               
        obj.status,                       
        obj.paid_status,                  
        obj.processing_fees_rate       
    ]);
}



processJob();
