const { parentPort, workerData } = require('worker_threads');
const mysql = require("./connector/mysql");
const logger = require("./logger/logger");

const getAndUpdatePayments = async (data) => {
    try {
        const loanIds = data.map((each) => each.loan_id);
        const loanIdStrings = loanIds.join(',');
        const loanTapePayments = await mysql.query(`select * from overall_payment where loan_id in (${loanIdStrings}) order by desc id`, [], 'loan-tape');
        const deleteData = await deleteDataFromOverallPayment(loanIdStrings);
        const paymentsData = createOverallPaymentCompatibleArray(loanTapePayments);
        logger.info('Started with the insertion process in overall payment');
        const overallPaymentsLoanTapeData = await mysql.query(`INSERT INTO overall_payment (id, customer_id, loan_id, amt_payment, received_date, cheque_number, urm_no, payment_channel, transaction_id, ref_no, utr_no, neft_bank, presentation_status, bounce_reason, presentation_date, create_date, extra_amount, extra_amount_pif, remarks, add_user_id, update_user_id, is_delete, is_refund, update_date, transaction_commit_status) VALUES ? `, [paymentsData], 'prod');
        logger.info(`Successfully inserted ${overallPaymentsLoanTapeData.affectedRows} rows in the overall_payments table`);
        parentPort.postMessage('Batch processed successfully'); // Notify main thread
    } catch (error) {
        logger.error(`Error while processing payments:`, error);
        throw error;
    }
};

const deleteDataFromOverallPayment = async (loanIdString) => {
    try {
        logger.info(`Initiated the deletion process for loans: ${loanIdString}`);
        const result = await mysql.query(`DELETE FROM overall_payment WHERE loan_id in (${loanIdString})`, [], 'prod');
        logger.info(`Successfully deleted ${result.affectedRows} records from overall_payment`);
        return result;
    } catch (error) {
        logger.error(`Error while deleting payments:`, error);
        throw error;
    }
};

const createOverallPaymentCompatibleArray = (payments) => {
    return payments.map(payment => [
        payment.id,
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
};

// Execute the worker function
getAndUpdatePayments(workerData);
