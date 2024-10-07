const path = require("path");
const mysql = require("../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../logger/logger");


const processJob = async () => {
    try {
        const conn = await mysql.getConnections();
        const dataStream = conn.query('select * from st_ksf_customer', []).stream();
        await pipeline(dataStream, batchStream, createProcessStream(companyEntity));
        if (conn) conn.release();
        process.exit(0);
    } catch (error) {
        logger.error("Error while initiating the amortization process");
        
    }
};


const batchStream = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
        this.buffer = (this.buffer || []).concat(chunk);
        if (this.buffer.length >= Number(process.env.STREAM_BATCH_SIZE || 100)) {
            dataCount = dataCount + this.buffer.length;
            batchCount = batchCount + 1;
            logger.info(`Batch count:>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>${batchCount}`);
            this.push(this.buffer);
            this.buffer = [];
        }
        callback();
    },
    flush(callback) {
        if (this.buffer.length > 0) this.push(this.buffer);
        callback();
    },
});

const createProcessStream = () =>
    new Transform({
        objectMode: true,
        async transform(data, encoding, callback) {
            inProgressCount = inProgressCount + 1;
            await getAndInsertInstallmentPif(data);
            callback()
        },
    });

const getAndInsertInstallmentPif = (data) =>{
    return new Promise(async (resolve, reject) => {
        try{
            const loanIds = data.map((each)=> each.loan_id)
            const loanIdString = loanIds.join(',')
            const loanTapeInstallments = await getInstallments(loanIdString)
            const deleteDataFromProd = await deleteInstallmentsFromProd(loanIdString)
            const insertOp = await insertInstallments(loanIdString)
            await Promise.all([loanTapeInstallments, deleteDataFromProd, insertOp]).then((res) => {
                logger.info(`Successfully performed all the operations and starting with another batch`)
                resolve()
            }).catch((error) => {
                logger.error(`Error while performing all the operations:`, error)
                reject()
            })
        }catch(error){
            logger.error(`Error while getting and inserting in installmentpif:`,error)
            reject(error)
        }
    })
}

const deleteInstallmentsFromProd = (loanIdString) => {
    return new Promise(async (resolve, reject ) => {
        try{
            logger.info('Initiating the installments deletion process from installment_pif table in the production database')
            const deleteDataFromProd =  `DELETE FROM installment_pif WHERE loan_id in (${loanIdString})`
            const deleteOp = await mysql.query(deleteDataFromProd, [], 'prod')
            logger.info(`Successfully completed the installments deletion process from the production database`)
            resolve(deleteOp)
        }catch(error){
            logger.error(`Error while deleting installments from the installment-pif table for (${loanIdString})`)
            logger.error('Cause of the error is:>', error)
            reject(error)
        }
    })
}

const getInstallments = (loanIdString) => {
    return new Promise(async(resolve , reject) => {
        try{
            logger.info(`Getting the installments from the Loan-Tape database for inserting`)
            const loanTapeInstallments = await mysql.query(`Select * from installment_pif where loan_id in (${loanIdString})`, [], 'loan-tape')
            logger.info(`Got the installments from the Loan-Tape database`)
            resolve(loanTapeInstallments)
        }catch(error){
            logger.error(`Error while getting installments from the Loan-tape database for (${loanIdString}).`)
            logger.error(`Error which is Encountered while getting the installments from the Loan-tape database is ${error}`)
            reject(error)
        }
    })
}

const insertInstallments = (loanTapeInstallments) => {
    return new Promise(async (resolve, reject) => {
        try{
            const insertInstallmentsQuery = `INSERT INTO installment_pif (
                entity_id, customer_id, loan_id, inst_number, inst_amount, inst_principal, inst_interest, inst_fee, 
                inst_discount, received_principal, received_interest, received_fee, amount_outstanding_interest, 
                amount_outstanding_principal, received_extra_charges, inst_date, create_date, last_paying_date, 
                starting_balance, ending_balance, days_past_due, days_past_due_tolerance, emi_status_id, foreclose_status, 
                foreclose_date, update_date, add_user_id, update_user_id, inst_status, is_delete, customer_facing, 
                amort_adjusment, payment_id, cashback, version) VALUES ?`
            const installments = mapDataToInsertArray(loanTapeInstallments)
            const insertOp = await mysql.query(insertInstallmentsQuery, [installments], 'prod')
            resolve(insertOp)
        }catch(error){
            logger.error(`Error while inserting the installments in the prod database:`, error)
            reject(error)
        }
    })
}

const mapDataToInsertArray = (data) => {
    try{
        const valueArrays = instalments.map((data) => [
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
            data.version
        ])
        return valueArrays
    }catch(error){
        logger.error(`Error while creating the installments compatible array:`, error)
        throw error
    }
};

processJob()