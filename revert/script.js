const path = require("path");
const mysql = require("../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../logger/logger");
const {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} = require("worker_threads");
const os = require("os");

const BATCH_SIZE = Number(process.env.STREAM_BATCH_SIZE || 1000);
const NUM_WORKERS = 20 || os.cpus().length;
console.log("Number of initial workers", NUM_WORKERS);

if (isMainThread) {
  let activeWorkers = 0;
  const updateActiveWorkers = (delta) => {
    activeWorkers += delta;
    logger.info(`Active workers: ${activeWorkers}`);
  };

  const processJob = async () => {
    try {
      //const conn = await mysql.getConnections();
      const [totalCount] = await mysql.query(
        "SELECT customer_id from loan_tape_copy where is_done = 0"
      );
      console.log("Total count:", totalCount);
      const total = totalCount.count;

      const workerPromises = [];
      for (let i = 0; i < NUM_WORKERS; i++) {
        const worker = new Worker(__filename, {
          workerData: {
            workerId: i,
            batchSize: Math.ceil(total / NUM_WORKERS),
            offset: i * Math.ceil(total / NUM_WORKERS),
          },
        });

        worker.on("online", () => updateActiveWorkers(1));
        worker.on("exit", () => updateActiveWorkers(-1));

        workerPromises.push(
          new Promise((resolve, reject) => {
            worker.on("message", resolve);
            worker.on("error", reject);
            worker.on("exit", (code) => {
              if (code !== 0)
                reject(new Error(`Worker stopped with exit code ${code}`));
            });
          })
        );
      }

      await Promise.all(workerPromises);

      logger.info("All workers completed successfully");

      process.exit(0);
    } catch (error) {
      logger.error("Error in main thread:", error);
      process.exit(1);
    }
  };

  processJob();
} else {
  const processWorker = async () => {
    const { workerId, batchSize, offset } = workerData;

    try {
      const rows = await mysql.query(
        "SELECT distinct customer_id from loan_tape_copy where is_done = 0",
        [batchSize, offset]
      );

      const chunks = [];
      for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        chunks.push(rows.slice(i, i + BATCH_SIZE));
      }

      for (const chunk of chunks) {
        await processChunk(chunk);
      }

      logger.info(
        `Worker ${workerId} completed processing ${rows.length} rows`
      );
      parentPort.postMessage("done");
    } catch (error) {
      logger.error(`Error in worker ${workerId}:`, error);
      parentPort.postMessage("error");
    } finally {
      //if (conn) conn.release();
    }
  };

  const processChunk = async (data) => {
    const customerId = data.map((each) => each.customer_id);
    const customerIdString = customerId.join(",");
    try {
        const loanRecords = await mysql.query(`Select * from st_loan where customer_id in (${customerIdString})`, [], 'prod')
                logger.info("Initializing the updation process of st_loan----")
                const query = `UPDATE st_loan
                SET 
                add_user_id = ?,
                advance_emi_tenure = ?,
                advance_emi_total = ?,
                agent_code = ?,
                approval_comment = ?,
                approved_amount = ?,
                approved_by = ?,
                approved_disbursal_date = ?,
                approved_emi_start_date = ?,
                approved_rate = ?,
                approved_tenure = ?,
                auto_processing = ?,
                browser = ?,
                closed_by = ?,
                colender = ?,
                colender_agreement = ?,
                create_date = ?,
                customer_id = ?,
                device = ?,
                disbursal_amount = ?,
                disbursal_loan_rate = ?,
                disbursal_loan_tenure = ?,
                disbursal_mode = ?,
                disbursed_by = ?,
                dmi_eligible = ?,
                el_form = ?,
                extra_params = ?,
                final_disbursed_amount = ?,
                first_approval_date = ?,
                fullerton_eligible = ?,
                incred_eligible = ?,
                ip_address = ?,
                lead_assign_id = ?,
                lead_status = ?,
                loan_amount = ?,
                loan_approval_date = ?,
                loan_closure_date = ?,
                loan_creation_date = ?,
                loan_disbursal_date = ?,
                loan_processing_mode = ?,
                loan_rate = ?,
                loan_reason_id = ?,
                loan_status = ?,
                loan_tenure = ?,
                loan_type = ?,
                loc_mandatory_loan = ?,
                loc_request = ?,
                looking_for = ?,
                organization_id = ?,
                origin = ?,
                original_utm_source = ?,
                other_calculate_gst = ?,
                other_fees = ?,
                processing_fees_rate = ?,
                product_code = ?,
                random_allotment = ?,
                random_loan_id = ?,
                rate_per_day = ?,
                rejected_by = ?,
                spdc_amount = ?,
                state_capture = ?,
                status = ?,
                update_date = ?,
                update_user_id = ?,
                utm_campaign = ?,
                utm_content = ?,
                utm_medium = ?,
                utm_source = ?,
                utm_source_changed = ?,
                utm_term = ?,
                utm_url = ?
            WHERE id = ?`
                const loanUpd = loanRecords.map(each => mysql.query(query, [
                    each.add_user_id,
                    each.advance_emi_tenure,
                    each.advance_emi_total,
                    each.agent_code,
                    each.approval_comment,
                    each.approved_amount,
                    each.approved_by,
                    each.approved_disbursal_date,
                    each.approved_emi_start_date,
                    each.approved_rate,
                    each.approved_tenure,
                    each.auto_processing,
                    each.browser,
                    each.closed_by,
                    each.colender,
                    each.colender_agreement,
                    each.create_date,
                    each.customer_id,
                    each.device,
                    each.disbursal_amount,
                    each.disbursal_loan_rate,
                    each.disbursal_loan_tenure,
                    each.disbursal_mode,
                    each.disbursed_by,
                    each.dmi_eligible,
                    each.el_form,
                    each.extra_params,
                    each.final_disbursed_amount,
                    each.first_approval_date,
                    each.fullerton_eligible,
                    each.incred_eligible,
                    each.ip_address,
                    each.lead_assign_id,
                    each.lead_status,
                    each.loan_amount,
                    each.loan_approval_date,
                    each.loan_closure_date,
                    each.loan_creation_date,
                    each.loan_disbursal_date,
                    each.loan_processing_mode,
                    each.loan_rate,
                    each.loan_reason_id,
                    each.loan_status,
                    each.loan_tenure,
                    each.loan_type,
                    each.loc_mandatory_loan,
                    each.loc_request,
                    each.looking_for,
                    each.organization_id,
                    each.origin,
                    each.original_utm_source,
                    each.other_calculate_gst,
                    each.other_fees,
                    each.processing_fees_rate,
                    each.product_code,
                    each.random_allotment,
                    each.random_loan_id,
                    each.rate_per_day,
                    each.rejected_by,
                    each.spdc_amount,
                    each.state_capture,
                    each.status,
                    each.update_date,
                    each.update_user_id,
                    each.utm_campaign,
                    each.utm_content,
                    each.utm_medium,
                    each.utm_source,
                    each.utm_source_changed,
                    each.utm_term,
                    each.utm_url,
                    each.id
                ], 'prod'))
                const upsl = await mysql.query(`update loan_tape_data set sl = 1 where loan_id in (${loanIdStrings})`, [], 'loan-tape')
                await Promise.all([loanUpd, upsl]).then((res) => {
                    logger.info(`Updated ${loanRecords.length} in the st_loan in the prod db.`)
                }).catch((error) => {
                    logger.error(`Error while updating the st_loan table on prod db:`, error)
                })

                await deleteLoanTapeInstallmentsFromProdFip(customerIdString)
                const installmentsFip = await getInstallmentsFip(customerIdString)
                await insertInstallmentsFip(installmentsFip)

                await deleteLoanTapeInstallmentsFromProdPif(customerIdString)
                const installmentsPif = await getInstallmentsPif(customerIdString)
                await insertInstallmentsPif(installmentsPif)

                await deleteDataFromBillDetail(customerIdString)
                const stBills = await getBills(customerIdString)
                await insertStBillDetails(stBills)

                await deleteInstallmentsFromProdPaymentFip(customerIdString)
                const paymentsFip = await getInstallmentsPaymentFip(customerIdString)
                await insertIntoInstallmentPaymentFip(paymentsFip)

                await deleteInstallmentsFromProdPaymentPif(customerIdString)
                const paymentsPif = await getInstallmentsPaymentPif(customerIdString)
                await insertIntoInstallmentPaymentPif(paymentsPif)

                await deleteDataFromOverallPayment(customerIdString)
                const overallPayments = await getOverallPayments(customerIdString)
                await insertIntoOverallPayments(overallPayments)

                await deletePenaltyDetailsFip(customerIdString)
                const penaltiesFip = await getPenaltiesDetailsFip(customerIdString)
                await insertPenaltiesFip(penaltiesFip)

                await deletePenaltyDetailsPif(customerIdString)
                const penaltiesPif = await getPenaltiesDetailsPif(customerIdString)
                await insertPenaltiesPif(penaltiesPif)
                const upd = await mysql.query(`update loan_tape_copy set is_done = 1 where customer_id in (${customerIdString})`, [], 'prod')
                const delLtData = await mysql.query(`Delete * from loan_tape_data where customer_id in (${customerIdString})`, [], 'prod')
                await Promise.all([upd, delLtData]).then((res) =>{
                    logger.info(`Successfully completed the one cycles of data migration`)
                }).catch(error => {
                    logger.error(`Error while performing some operations on one cycle:`, error)
                })

    } catch (error) {
      //await conn.rollback();
      logger.error(`Error processing chunk:`, error);
      throw error;
    }
  };
  // installments fip
  const deleteLoanTapeInstallmentsFromProdFip = async (customerIdString) => {
    try {
      logger.info(
        "Initiating the installments deletion process from installment_fip table in the production database"
      );
      const deleteDataFromProd = `DELETE FROM installment_fip WHERE customer_id IN (${customerIdString})`;
      const deleteOp = await mysql.query(deleteDataFromProd, [], "prod");
      logger.info(
        `Successfully completed the installments deletion process from the production database`
      );
      return deleteOp;
    } catch (error) {
      logger.error(
        `Error while deleting installments from the installment-fip table for (${customerIdString})`
      );
      logger.error("Cause of the error is:>", error);
      throw error;
    }
  };

  const getInstallmentsFip = async (customerIdString) => {
    try {
      logger.info(
        `Getting the installments from the Loan-Tape database for inserting`
      );
      const loanTapeInstallments = await mysql.query(
        `SELECT * FROM installment_fip_cl WHERE customer_id IN (${customerIdString}) AND is_delete = 0`,
        [],
        "loan-tape"
      );
      logger.info(`Got the installments from the Loan-Tape database`);
      return loanTapeInstallments;
    } catch (error) {
      logger.error(
        `Error while getting installments from the Loan-tape database for (${customerIdString}).`
      );
      logger.error(
        `Error which is Encountered while getting the installments from the Loan-tape database is ${error}`
      );
      throw error;
    }
  };

  const insertInstallmentsFip = async (loanTapeInstallments) => {
    try {
      const insertInstallmentsQuery = `INSERT INTO installment_fip (
        id,entity_id, customer_id, loan_id, inst_number, inst_amount, inst_principal, inst_interest, inst_fee, 
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

 // installments pif
  const deleteLoanTapeInstallmentsFromProdPif = async (customerIdString) => {
    try {
      logger.info(
        "Initiating the installments deletion process from installment_fip table in the production database"
      );
      const deleteDataFromProd = `DELETE FROM installment_pif WHERE customer_id IN (${customerIdString})`;
      const deleteOp = await mysql.query(deleteDataFromProd, [], "prod");
      logger.info(
        `Successfully completed the installments deletion process from the production database`
      );
      return deleteOp;
    } catch (error) {
      logger.error(
        `Error while deleting installments from the installment-fip table for (${customerIdString})`
      );
      logger.error("Cause of the error is:>", error);
      throw error;
    }
  };


  const getInstallmentsPif = async (customerIdString) => {
    try {
      logger.info(
        `Getting the installments from the Loan-Tape database for inserting`
      );
      const loanTapeInstallments = await mysql.query(
        `SELECT * FROM installment_pif_cl WHERE customer_id IN (${customerIdString}) AND is_delete = 0`,
        [],
        "prod"
      );
      logger.info(`Got the installments from the Loan-Tape database`);
      return loanTapeInstallments;
    } catch (error) {
      logger.error(
        `Error while getting installments from the Loan-tape database for (${customerIdString}).`
      );
      logger.error(
        `Error which is Encountered while getting the installments from the Loan-tape database is ${error}`
      );
      throw error;
    }
  };

  const insertInstallmentsPif = async (loanTapeInstallments) => {
    try {
      const insertInstallmentsQuery = `INSERT INTO installment_pif (
        id,entity_id, customer_id, loan_id, inst_number, inst_amount, inst_principal, inst_interest, inst_fee, 
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
        data.id,
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
  

  // st_bill_details
  const deleteDataFromBillDetail = async (customerIdString) => {
    try {
      logger.info(
        `Deleting bills from st_bill_detail table for customerIds: ${customerIdString}`
      );
      const result = await mysql.query(
        `DELETE FROM st_bill_detail WHERE customer_id in (${customerIdString})`,
        [],
        "prod"
      );
      logger.info(
        `Deleted ${result.affectedRows} rows from st_bill_detail table`
      );
      return result;
    } catch (error) {
      logger.error("Error occured while deleting data from st_bill_details: ", error);
      throw error;
    }
  };

  const getBills = async (customerIdString) => {
    try{
        const bills = await mysql.query(`select * from st_bill_detail_cl where customer_id in (${customerIdString})`, [], 'prod')
        return bills
    }catch(error){
        logger.error(`Error while getting bill details from st_bill_details_cl table for customerIds: (${customerIdString})`)
        throw error
    }
  }

  const createBillCompatibleArray = (arrayOfObjects) => {
    return arrayOfObjects.map((obj) => [
      obj.id,
      obj.customer_id,
      obj.loan_id,
      obj.approved_amount,
      obj.bill_amount,
      obj.processing_fee,
      obj.credit_shield_fee,
      obj.cgst,
      obj.igst,
      obj.sgst,
      obj.gst,
      obj.upfront,
      obj.bill_date,
      obj.paid_amount,
      obj.outstanding_amount,
      obj.emi_paid,
      obj.tenure,
      obj.create_date,
      obj.update_date,
      obj.status,
      obj.paid_status,
      obj.processing_fees_rate,
    ]);
  };


  const insertStBillDetails = async (data) => {
    try {
      logger.info(
        `Initiating the insertion process in the st_bill_detail table`
      );
      const insertQuery = `INSERT INTO st_bill_detail (id,customer_id, loan_id, approved_amount, bill_amount, processing_fee, credit_shield_fee, cgst, igst, sgst, gst, upfront, bill_date, paid_amount, outstanding_amount, emi_paid, tenure, create_date, update_date, status, paid_status, processing_fees_rate) VALUES  ?`;
      const penaltyArray = createBillCompatibleArray(data);
      if (penaltyArray.length === 0) {
        logger.info(
          "there are no bills present to insert");
        return true;
      }
      const result = await mysql.query(insertQuery, [penaltyArray], "prod");
      logger.info(
        `Successfully inserted ${result.affectedRows} bills inside the st_bill_detail table`
      );
      return result;
    } catch (error) {
      logger.error(
        `Error while inserting the bills inside the st_bill_detail table:-`,
        error
      );
      throw error;
    }
  };


  // paymentsFip
  const deleteInstallmentsFromProdPaymentFip = (customerIdString) => {
    return new Promise(async (resolve, reject) => {
      try {
        logger.info(
          "Initiating the installments deletion process from installment_payment_fip table in the production database"
        );
        const deleteDataFromProd = `DELETE FROM installment_payment_fip WHERE customer_id in (${customerIdString})`;
        const deleteOp = await mysql.query(deleteDataFromProd, [], "prod");
        logger.info(
          `Successfully deleted the ${deleteOp.affectedRows} from the installment_payment_fip table`
        );
        resolve(deleteOp);
      } catch (error) {
        logger.error(
          `Error while deleting installments from the installment-fip table for (${loanIdString})`
        );
        logger.error("Cause of the error is:>", error);
        reject(error);
      }
    });
  };

  const getInstallmentsPaymentFip = (customerIdString) => {
    return new Promise(async(resolve, reject) => {
        try{
            logger.info(`Getting the installments_payments_fip_cl from the cl_tables for customerids = ${customerIdString}`)
            const installments = await mysql.query(`Select * from installment_payment_fip_cl where customer_id in (${customerIdString})`, [], 'prod')
            resolve(installments)
        }catch(error){
            logger.error(`Error while getting payments_fip_cl for customerIds:(${customerIdString})`)
            reject(error)
        }
    })
  }

  const insertIntoInstallmentPaymentFip = async (paymentsArray, customerIdString) => {
    try {
      logger.info(
        "Initializing the insertion process in the installment_payment_fip table."
      );
      paymentsArray = createPaymentInstPif(paymentsArray)
      const insertQuery =
        "Insert into installment_payment_fip (id, customer_id, loan_id, inst_id, inst_number, code_payment_type, amount_payment, payment_status, payment_pairing_date, payment_date, payment_id) VALUES ?";
      if (paymentsArray.length === 0) {
        logger.info(
          "No payment present to insert in installment-payment-fip table"
        );
        return true;
      }
      const result = await mysql.query(insertQuery, [paymentsArray], "prod");
      logger.info(
        `Successfully inserted ${result.affectedRows} in the installment_payments_fip table prod`
      );
      return result;
    } catch (error) {
      logger.error(
        `Error while inserting in installment_payment_fip table for loanIds (${customerIdString})`
      );
      logger.error(`Complete error description is:`, error);
      throw error;
    }
  };


  // payments Pif
  const deleteInstallmentsFromProdPaymentPif = (customerIdString) => {
    return new Promise(async (resolve, reject) => {
      try {
        logger.info(
          "Initiating the installments deletion process from installment_payment_fip table in the production database"
        );
        const deleteDataFromProd = `DELETE FROM installment_payment_pif WHERE customer_id in (${customerIdString})`;
        const deleteOp = await mysql.query(deleteDataFromProd, [], "prod");
        logger.info(
          `Successfully deleted the ${deleteOp.affectedRows} from the installment_payment_fip table`
        );
        resolve(deleteOp);
      } catch (error) {
        logger.error(
          `Error while deleting installments from the installment-fip table for (${loanIdString})`
        );
        logger.error("Cause of the error is:>", error);
        reject(error);
      }
    });
  };

  const getInstallmentsPaymentPif = (customerIdString) => {
    return new Promise(async(resolve, reject) => {
        try{
            logger.info(`Getting the installments_payments_pif_cl from the cl_tables for customerids = ${customerIdString}`)
            const installments = await mysql.query(`Select * from installment_payment_fip_cl where customer_id in (${customerIdString})`, [], 'prod')
            resolve(installments)
        }catch(error){
            logger.error(`Error while getting payments_fip_cl for customerIds:(${customerIdString})`)
            reject(error)
        }
    })
  }


  const insertIntoInstallmentPaymentPif = async (paymentsArray, customerIdString) => {
    try {
      logger.info(
        "Initializing the insertion process in the installment_payment_pif table."
      );
      const insertQuery =
        "Insert into installment_payment_pif (id, customer_id, loan_id, inst_id, inst_number, code_payment_type, amount_payment, payment_status, payment_pairing_date, payment_date, payment_id) VALUES ?";
      if (paymentsArray.length === 0) {
        logger.info(
          "No payment present to insert in installment-payment-pif table"
        );
        return true;
      }
      const result = await mysql.query(insertQuery, [paymentsArray], "prod");
      logger.info(
        `Successfully inserted ${result.affectedRows} in the installment_payments_pif table prod`
      );
      return result;
    } catch (error) {
      logger.error(
        `Error while inserting in installment_payment_pif table for loanIds (${customerIdString})`
      );
      logger.error(`Complete error description is:`, error);
      throw error;
    }
  };


  const createPaymentInstPif = (array) => {
    try {
      const compatibleArray = array.map((item) => [
        item.id,
        item.customer_id,
        item.loan_id,
        item.inst_id,
        item.inst_number,
        item.code_payment_type,
        item.amount_payment,
        item.payment_status,
        item.payment_pairing_date,
        item.payment_date,
        item.payment_id,
      ]);
      return compatibleArray;
    } catch (error) {
      logger.error(`Error while creating payments installments:`, error);
      throw error;
    }
  };

  // overall payments

  const deleteDataFromOverallPayment = async (customerIdString) => {
    try {
      logger.info(`Deleting payments with customerIds: (${customerIdString})`);
      const result = await mysql.query(
        `DELETE FROM overall_payment WHERE customer_id in (${customerIdString})`,
        [],
        "prod"
      );
      logger.info(
        `Deleted ${result.affectedRows} rows from overall_payment table`
      );
      return result;
    } catch (error) {
      logger.error(
        "Error deleting payments from overall_payments table: ",
        error
      );
      throw error;
    }
  };

  const getOverallPayments = async(customerIdString) =>{
    return new Promise(async(resolve, reject) => {
        try{
            logger.info(`Fetching payments from overall_payments_cl for customerids: (${customerIdString})`)
            const ovPayments = await mysql.query(`Select * from overall_payment_cl where customer_id in (${customerIdString})`, [], 'prod')
            resolve(ovPayments)
        }catch(error){
            logger.error(`Error while getting payments from overall_payments for customerid:(${customerIdString})`)
            throw error
        }
    })
  }

  const createOverallPaymentCompatibleArray = (payments) => {
    try {
      return payments.map((payment) => [
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
        "others",
        payment.add_user_id,
        payment.update_user_id,
        payment.is_delete,
        payment.is_refund,
        payment.update_date,
        payment.transaction_commit_status,
      ]);
    } catch (error) {
      logger.error("Error creating compatible array for payments: ", error);
      throw error;
    }
  };

  const insertIntoOverallPayments = async (paymentsArray, customerIdString) => {
    try{
        const paymentsData =
            createOverallPaymentCompatibleArray(paymentsArray);
        logger.info("Starting insertion into overall_payment table");

        const result = await mysql.query(
            `INSERT INTO overall_payment (id,customer_id, loan_id, amt_payment, received_date, cheque_number, urm_no, payment_channel, transaction_id, ref_no, utr_no, neft_bank, presentation_status, bounce_reason, presentation_date, create_date, extra_amount, extra_amount_pif, remarks, add_user_id, update_user_id, is_delete, is_refund, update_date, transaction_commit_status) VALUES ?`,
            [paymentsData],
            "prod"
        );

        return result
    }catch(error){
        logger.error(`Error while inserting into overall payments:`,error)
        throw error
    }
  }

// penalty_details_fip
  const deletePenaltyDetailsFip = async(customerIdString) => {
    try{
        logger.info(`Initiating the deletion process from penalty details for customerId in (${customerIdString})`)
        const delst = await mysql.query(`DELETE FROM penalty_details WHERE customer_id in (${customerIdString})`, [], 'prod')
        return delst
    }catch(error){
        logger.error(`Error while deleting penalty-details for customerIds:(${customerIdString})`)
        throw error
    }
  }

  const getPenaltiesDetailsFip = async(customerIdString) => {
    try{
        const penalties = await mysql.query(`Select * from penalty_details_cl where customer_id in (${customerIdString})`, [], 'prod')
        return penalties
    }catch(error){
        logger.error(`Error while fetching the penalty details for customerIds :(${customerIdString})`)
        throw error
    }
  }


  const insertPenaltiesFip = async (data, customerIdString) => {
    try {
      logger.info(
        `Initiating the insertion process in the penalty_details table`
      );
      const insertQuery = `INSERT INTO penalty_details (id, customer_id,loan_id,instalment_id,instalment_number,penalty_amount,penalty_date,penalty_received,penalty_received_date,outstanding_penalty,is_paid,panalty_type_id,payment_id,foreclose_status,status,create_date) VALUES ?`;
      const penaltyArray = createPenaltyCompatibleArray(data);
      if (penaltyArray.length === 0) {
        logger.info(
          "there are no penalties present to insert with the given loanIds:",
          loanIds
        );
        return true;
      }
      const result = await mysql.query(insertQuery, [penaltyArray], "prod");
      logger.info(
        `Successfully inserted ${result.affectedRows} penalties inside the penalty details table`
      );
      return result;
    } catch (error) {
      logger.error(
        `Error while inserting the penalties inside the penalty details table:-`,
        error
      );
      throw error;
    }
  };

  // penalty_details_pif
  const deletePenaltyDetailsPif = async(customerIdString) => {
    try{
        logger.info(`Initiating the deletion process from penalty_details_PIf for customerId in (${customerIdString})`)
        const delst = await mysql.query(`DELETE FROM penalty_details_pif WHERE customer_id in (${customerIdString})`, [], 'prod')
        return delst
    }catch(error){
        logger.error(`Error while deleting penalty-details for customerIds:(${customerIdString})`)
        throw error
    }
  }

  const getPenaltiesDetailsPif = async(customerIdString) => {
    try{
        const penalties = await mysql.query(`Select * from penalty_details_pif_cl where customer_id in (${customerIdString})`, [], 'prod')
        return penalties
    }catch(error){
        logger.error(`Error while fetching the penalty details for customerIds :(${customerIdString})`)
        throw error
    }
  }

  const insertPenaltiesPif = async (data, customerIdString) => {
    try {
      logger.info(
        `Initiating the insertion process in the penalty_details table`
      );
      const insertQuery = `INSERT INTO penalty_details_pif (id, customer_id,loan_id,instalment_id,instalment_number,penalty_amount,penalty_date,penalty_received,penalty_received_date,outstanding_penalty,is_paid,panalty_type_id,payment_id,foreclose_status,status,create_date) VALUES ?`;
      const penaltyArray = createPenaltyCompatibleArray(data);
      if (penaltyArray.length === 0) {
        logger.info(
          "there are no penalties present to insert with the given loanIds:",
          loanIds
        );
        return true;
      }
      const result = await mysql.query(insertQuery, [penaltyArray], "prod");
      logger.info(
        `Successfully inserted ${result.affectedRows} penalties inside the penalty details table`
      );
      return result;
    } catch (error) {
      logger.error(
        `Error while inserting the penalties inside the penalty details table:-`,
        error
      );
      throw error;
    }
  };

  const createPenaltyCompatibleArray = (penalties) => {
    try {
      return penalties.map((penalty) => [
        penalty.id,
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
        penalty.panalty_type_id, // Assuming there's a typo in "penalty_type_id"
        penalty.payment_id,
        penalty.foreclose_status,
        penalty.status,
        penalty.create_date,
      ]);
    } catch (error) {
      logger.error("Error creating compatible array for penalties: ", error);
      throw error;
    }
  };

  processWorker();
}


