const path = require("path");
const mysql = require("../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../logger/logger");


const processJob = async () => {
    try {
        const conn = await mysql.getConnections();
        const dataStream = conn.query('select * from st_ksf_customer', []).stream();
        await pipeline(dataStream, batchStream, createProcessStream());
        if (conn) conn.release();
        process.exit(0);
    } catch (error) {
        logger.error("Error while updating the st_loan table in the production database:" ,error);
        throw error
        
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
            await getAndUpdateLoans(data);
            callback()
        },
    });


const getAndUpdateLoans = async (data) => {
    try{
        logger.info(`Started with the loan updation process for batch size ${data.length}`)
        const loanIds = data.map((each) => each.loan_id)
        const loanIdStrings = loanIds.join(',')
        const loanRecords = await mysql.query(`Select * from st_loan where id in (${loanIdStrings})`, [], 'loan-tape')
        const updateRecords = loanRecords.map((each) => {
            return mysql.query(`update st_loan set loan_tenure = ?, loan_rate = ? where id = ?`,[each.loan_id, each.loan_rate, each.id], 'prod')
        })
        await Promise.all(updateRecords).then((res) => {
            logger.info(`Updated ${res.affectedRows} in the st_loan in the prod db.`)
        }).catch((error) => {
            logger.error(`Error while updating the st_loan table on prod db:`, error)
        })
    }catch(error) {
        logger.error(`Error while getting loans:`, error)
        throw error
    }
}

processJob();
