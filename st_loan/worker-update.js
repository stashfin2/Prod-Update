const { parentPort, workerData } = require('worker_threads');
const mysql = require("./connector/mysql");
const logger = require("./logger/logger");

const getAndUpdateLoans = async (data) => {
    try {
        const loanIds = data.map((each) => each.loan_id);
        const loanIdStrings = loanIds.join(',');
        const loanRecords = await mysql.query(`Select * from st_loan where id in (${loanIdStrings})`, [], 'loan-tape');
        const updateRecords = loanRecords.map((each) => {
            return mysql.query(`update st_loan set loan_tenure = ?, loan_rate = ? where id = ?`, [each.loan_id, each.loan_rate, each.id], 'prod');
        });
        await Promise.all(updateRecords);
        parentPort.postMessage(`Updated loans for batch`); // Notify the main thread
    } catch (error) {
        logger.error(`Error in worker processing:`, error);
        throw error;
    }
};

// Execute when the worker starts
getAndUpdateLoans(workerData);
