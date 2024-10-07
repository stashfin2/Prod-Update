const { parentPort, workerData } = require('worker_threads');
const mysql = require('./connector/mysql');
const logger = require('./logger/logger');

const getAndInsertInstallmentfip = async (data) => {
    try {
        const loanIds = data.map((each) => each.loan_id);
        const loanIdString = loanIds.join(',');

        const loanTapeInstallments = await getInstallments(loanIdString);
        await deleteInstallmentsFromProd(loanIdString);
        await insertInstallments(loanTapeInstallments);

        logger.info(`Successfully processed the batch for loan IDs: ${loanIdString}`);
        parentPort.postMessage({ success: true });
    } catch (error) {
        logger.error(`Error processing batch for loan IDs: ${data.map(e => e.loan_id).join(', ')}. Cause: ${error.message}`);
        parentPort.postMessage({ success: false, error: error.message });
    }
};

// Functions for database operations
const getInstallments = async (loanIdString) => {
    try {
        const loanTapeInstallments = await mysql.query(`SELECT * FROM installment_fip WHERE loan_id in (${loanIdString})`, [], 'loan-tape');
        return loanTapeInstallments;
    } catch (error) {
        logger.error(`Error fetching installments from Loan-tape for loan IDs (${loanIdString}): ${error.message}`);
        throw error;
    }
};

const deleteInstallmentsFromProd = async (loanIdString) => {
    try {
        const deleteDataFromProd = await mysql.query(`DELETE FROM installment_fip WHERE loan_id in (${loanIdString})`, [], 'prod');
        return deleteDataFromProd;
    } catch (error) {
        logger.error(`Error deleting installments from prod for loan IDs (${loanIdString}): ${error.message}`);
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
            amort_adjusment, payment_id, cashback, version
        ) VALUES ?`;

        const installments = mapDataToInsertArray(loanTapeInstallments);
        const insertOp = await mysql.query(insertInstallmentsQuery, [installments], 'prod');
        return insertOp;
    } catch (error) {
        logger.error(`Error inserting installments into prod for loan IDs: ${loanTapeInstallments.map(e => e.loan_id).join(', ')}. Cause: ${error.message}`);
        throw error;
    }
};

const mapDataToInsertArray = (data) => {
    return data.map((data) => [
        data.entity_id, data.customer_id, data.loan_id, data.inst_number, data.inst_amount, data.inst_principal, 
        data.inst_interest, data.inst_fee, data.inst_discount, data.received_principal, data.received_interest, 
        data.received_fee, data.amount_outstanding_interest, data.amount_outstanding_principal, 
        data.received_extra_charges, data.inst_date, data.create_date, data.last_paying_date, data.starting_balance, 
        data.ending_balance, data.days_past_due, data.days_past_due_tolerance, data.emi_status_id, 
        data.foreclose_status, data.foreclose_date, data.update_date, data.add_user_id, data.update_user_id, 
        data.inst_status, data.is_delete, data.customer_facing, data.amort_adjusment, data.payment_id, 
        data.cashback, data.version
    ]);
};

// Start processing the batch data
getAndInsertInstallmentfip(workerData);
