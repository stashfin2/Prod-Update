const path = require("path");
const mysql = require("../../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../../logger/logger");
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
        "SELECT COUNT(*) as count FROM payfip WHERE is_done = 0"
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
        "SELECT * FROM payfip WHERE is_done = 0 LIMIT ? OFFSET ?",
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
    //await conn.beginTransaction();

    try {
      const loanIds = data.map((each) => each.loan_id);
      const loanIdString = loanIds.join(",");
      let copyCreds = JSON.parse(JSON.stringify(data));
      const whereClauses = copyCreds
        .map(
          (item) =>
            `(customer_id = ${item.customer_id} AND loan_id = ${item.loan_id})`
        )
        .join(" OR ");
      let paymentRecords = await getOverallPayments(loanIdString, whereClauses);
      paymentRecords = groupBy(paymentRecords, "loan_id");
      let installmentsFip = await getInstallments(loanIdString);
      installmentsFip = installmentsFip.map((each) => {
        each["amount_outstanding_principal"] = each.inst_principal;
        each["amount_outstanding_interest"] = each.inst_interest;
        each["received_principal"] = 0;
        each["received_interest"] = 0;
        return each;
      });
      const groupedFip = groupBy(installmentsFip, "loan_id");
      let installmentPaymentFipInsertionArray = [];
      for (let loanId in paymentRecords) {
        if (paymentRecords[loanId].length > 0) {
          for (let record of paymentRecords[loanId]) {
            let [updatedInstallmentFIPArray, insFip, bucketAmountpif] =
              generatePayments(
                record["amt_payment"],
                record,
                groupedFip[loanId],
                "fip",
                "col",
                0
              );
            groupedFip[loanId] = updatedInstallmentFIPArray;
            installmentPaymentFipInsertionArray.push(...insFip);
          }
        }
      }
      const paymentsArray = createPaymentInst(
        installmentPaymentFipInsertionArray
      );

      const [del, result, upd] = await Promise.all([
        deleteInstallmentsFromProd(loanIdString, whereClauses),
        insertPaymentsArray(paymentsArray, loanIdString),
        mysql.query(
          `update payfip set is_done = 1 where ${whereClauses}`,
          [],
          "loan-tape"
        ),
      ]);

      logger.info("updated installment_payment_fip for this batch");
    } catch (error) {
      //await conn.rollback();
      logger.error(`Error processing chunk:`, error);
      throw error;
    }
  };

  const generatePayments = (
    bucketAmount,
    payment,
    installments,
    table,
    col,
    instNumber
  ) => {
    try {
      let installmentsFacing;
      let installmentsNonFacing;
      let nonFacingUpdation = [];
      let insertionArray = [];

      if (!installments) {
        return [[], insertionArray, bucketAmount];
      }
      if (installments && col == "col" && installments.length > 0) {
        installmentsFacing = installments.filter(
          (inst) => inst.is_delete === 0 && inst.customer_facing === 1
        );
        installmentsNonFacing = installments.filter(
          (inst) => inst.is_delete === 0 && inst.customer_facing === 0
        );
      } else if (installments && col !== "col" && installments.length > 0) {
        installmentsFacing = installments;
        installmentsNonFacing = installments.filter(
          (each) => each.instNumber === instNumber
        );
      }

      if (!installmentsFacing) {
        return [installments, insertionArray, bucketAmount];
      }

      for (let principal of installmentsFacing) {
        if (col === "col") {
          let [colUp, colIn, colBucketAmount] = generatePayments(
            bucketAmount,
            payment,
            installmentsNonFacing.filter(
              (inst) => inst.inst_number === principal.inst_number
            ),
            table,
            "",
            principal.inst_number
          );
          nonFacingUpdation.push(...colUp);
          insertionArray.push(...colIn);
        }

        let [interestObj, installmentObject, bucketAmountRet] =
          calculateInterest(principal, payment, bucketAmount, table);
        principal = interestObj;
        if (Object.keys(installmentObject).length > 0) {
          insertionArray.push(installmentObject);
        }
        bucketAmount = bucketAmountRet;

        if (bucketAmount > 0) {
          let [principalObj, installmentObject, bucketAmountRet] =
            calculatePricipal(principal, payment, bucketAmount, table);
          principal = principalObj;
          bucketAmount = bucketAmountRet;
          if (Object.keys(installmentObject).length > 0) {
            insertionArray.push(installmentObject);
          }
        }

        if (bucketAmount <= 0) {
          break;
        }
      }
      if (nonFacingUpdation.length > 0) {
        installmentsNonFacing = mergeArrays(
          nonFacingUpdation,
          installmentsNonFacing
        );
        installments = sortByInstDateAndNumber([
          ...installmentsFacing,
          ...installmentsNonFacing,
        ]);
      }
      installments = mergeArrays(nonFacingUpdation, installments);
      return [installments, insertionArray, bucketAmount];
    } catch (error) {
      if (col === "col") {
        logger.error(
          "Error while generating payments for installment_payment_fip table for colenders>>>>",
          error
        );
      } else {
        logger.error(
          "Error while generating payments for installment_payment_fip table for cutomer facing>>>>",
          error
        );
      }
      logger.error(
        `Error occured while generating payments installment_payment_fip array:`,
        error
      );
      throw error;
    }
  };

  const mergeArrays = (array1, array2) => {
    try {
      const map = new Map();
      array1.forEach((obj) => {
        const key = `${obj.id}_${obj.inst_number}_${obj.inst_date}`;
        map.set(key, obj);
      });
      array2.forEach((obj) => {
        const key = `${obj.id}_${obj.inst_number}_${obj.inst_date}`;
        if (map.has(key)) {
          const index = array2.findIndex(
            (o) =>
              o.id === obj.id &&
              o.inst_number === obj.inst_number &&
              o.inst_date === obj.inst_date
          );
          array2[index] = map.get(key);
        }
      });
      return array2;
    } catch (error) {
      logger.error(`Error occured while merging two arrays:`, error);
      console.log("array1>>>>>>>>>>>>>>", JSON.stringify(array1));
      console.log("array2>>>>>>>>>>>>>>", JSON.stringify(array2));
      throw error;
    }
  };

  const sortByInstDateAndNumber = (arr) => {
    try {
      return arr.sort((a, b) => {
        const dateComparison = new Date(a.inst_date) - new Date(b.inst_date);
        if (dateComparison !== 0) {
          return dateComparison;
        }
        return a.inst_number - b.inst_number;
      });
    } catch (error) {
      logger.error(`Error while sorting installments by number :`, error);
      logger.error("Array in which the error occured:", JSON.stringify(arr));
      throw error;
    }
  };

  const calculatePricipal = (principal, payment, bucketAmount, table) => {
    try {
      let commonIpInsObject = {};
      let availableBalancePrincipal = bucketAmount;
      let outstandingPrincipal = principal["amount_outstanding_principal"];
      if (outstandingPrincipal > 0) {
        availableBalancePrincipal =
          availableBalancePrincipal - outstandingPrincipal;
        if (availableBalancePrincipal >= 0) {
          let recievedPrincipal = principal["inst_principal"];
          principal["received_principal"] = recievedPrincipal;
          principal["amount_outstanding_principal"] = 0;
          principal["last_paying_date"] = payment["received_date"];
          principal["updated"] = true;
          if (table === "fip") {
            principal["emi_status_id"] = 2;
          }
          commonIpInsObject = createInsertionObject(
            principal,
            outstandingPrincipal,
            payment,
            "pri"
          );
          bucketAmount = bucketAmount - outstandingPrincipal;
        } else {
          let recievedPrincipal =
            bucketAmount + principal["received_principal"];
          let outstandingAmountPrincipal =
            principal["amount_outstanding_principal"] - bucketAmount;
          principal["received_principal"] = recievedPrincipal;
          principal["amount_outstanding_principal"] =
            outstandingAmountPrincipal;
          principal["last_paying_date"] = payment["received_date"];
          principal["updated"] = true;
          commonIpInsObject = createInsertionObject(
            principal,
            bucketAmount,
            payment,
            "pri"
          );
          bucketAmount = 0;
        }
      }
      return [principal, commonIpInsObject, bucketAmount];
    } catch (error) {
      logger.error(
        `Error while calculating the pricipal payments in installment_payment-fip table:`,
        error
      );
      logger.error("Creds>>>>>>>>>>>>>>>>>>>>>>>>>>:");
      logger.error("principal>>>>", JSON.stringify(principal));
      logger.error("payment>>>>>>", JSON.stringify(payment));
      logger.error("bucketAmout>>>", bucketAmount);
      throw error;
    }
  };

  const calculateInterest = (principal, payment, bucketAmount, table) => {
    try {
      let availableBalanceInterest = bucketAmount;
      let outstandingInterest = principal["amount_outstanding_interest"];
      let commonIpInsObject = {};
      if (outstandingInterest > 0) {
        availableBalanceInterest =
          availableBalanceInterest - outstandingInterest;
        if (availableBalanceInterest >= 0) {
          let recievedInterest = principal["inst_interest"];
          (principal["received_interest"] = recievedInterest),
            (principal["amount_outstanding_interest"] = 0);
          principal["last_paying_date"] = payment["received_date"];
          principal["updated"] = true;
          if (table === "pif") {
            principal["emi_status_id"] = 2;
          }
          commonIpInsObject = createInsertionObject(
            principal,
            outstandingInterest,
            payment,
            "int"
          );
          bucketAmount = bucketAmount - outstandingInterest;
        } else {
          let receivedInterest = bucketAmount + principal["received_interest"];
          let outstandingAmountInterst =
            principal["amount_outstanding_interest"] - bucketAmount;
          principal["received_interest"] = receivedInterest;
          principal["amount_outstanding_interest"] = outstandingAmountInterst;
          principal["last_paying_date"] = payment["received_date"];
          principal["updated"] = true;
          commonIpInsObject = createInsertionObject(
            principal,
            bucketAmount,
            payment,
            "int"
          );
          bucketAmount = 0;
        }
      }

      return [principal, commonIpInsObject, bucketAmount];
    } catch (error) {
      logger.error(
        `Error while calculating the INTEREST payments in installment_payment-fip table:`,
        error
      );
      logger.error("Creds>>>>>>>>>>>>>>>>>>>>>>>>>>:");
      logger.error("principal>>>>", JSON.stringify(principal));
      logger.error("payment>>>>>>", JSON.stringify(payment));
      logger.error("bucketAmout>>>", bucketAmount);
      throw error;
    }
  };

  const createInsertionObject = (principal, amount, payment, type) => {
    try {
      let commonIpInsObject = {
        customer_id: principal["customer_id"],
        loan_id: principal["loan_id"],
        inst_id: principal["id"],
        inst_number: principal["inst_number"],
        code_payment_type: type === "pri" ? 2 : 3,
        amount_payment: amount,
        payment_status: 1,
        payment_pairing_date: new Date(),
        payment_date: payment["received_date"],
        payment_id: payment["id"],
      };
      return commonIpInsObject;
    } catch (error) {
      logger.error(
        `Error while calculating the pricipal payments in installment_payment-fip table:`,
        error
      );
      logger.error("Creds>>>>>>>>>>>>>>>>>>>>>>>>>>:");
      logger.error("principal>>>>", JSON.stringify(principal));
      logger.error("payment>>>>>>", JSON.stringify(payment));
      logger.error("bucketAmout>>>", bucketAmount);
      throw error;
    }
  };

  const insertPaymentsArray = async (paymentsArray, loanIdString) => {
    try {
      logger.info(
        "Initializing the insertion process in the installment_payment_fip table."
      );
      const insertQuery = `INSERT INTO installment_payment_fip (customer_id, loan_id, inst_id, inst_number, code_payment_type, amount_payment, payment_status, payment_pairing_date, payment_date, payment_id) VALUES ?`;
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
        `Error while inserting in installment_payment_fip table for loanIds (${loanIdString})`
      );
      logger.error(`Complete error description is:`, error);
      throw error;
    }
  };

  const createPaymentInst = (array) => {
    try {
      const compatibleArray = array.map((item) => [
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

  const getFormattedDate = () => {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, "0");
    const day = String(today.getDate()).padStart(2, "0");
    const hours = String(today.getHours()).padStart(2, "0");
    const minutes = String(today.getMinutes()).padStart(2, "0");
    const seconds = String(today.getSeconds()).padStart(2, "0");

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  };

  const deleteInstallmentsFromProd = (loanIdString, whereClauses) => {
    return new Promise(async (resolve, reject) => {
      try {
        logger.info(
          "Initiating the installments deletion process from installment_payment_fip table in the production database"
        );
        const deleteDataFromProd = `DELETE FROM installment_payment_fip WHERE ${whereClauses} and code_payment_type in (2,3) `;
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

  const getInstallments = (loanIdString) => {
    return new Promise(async (resolve, reject) => {
      try {
        logger.info(
          `Getting the installments from the Loan-Tape database for inserting`
        );
        const loanTapeInstallments = await mysql.query(
          `Select * from installment_fip where loan_id in (${loanIdString}) and is_delete = 0 order by inst_number asc`,
          [],
          "prod"
        );
        logger.info(`Got the installments from the Loan-Tape database`);
        resolve(loanTapeInstallments);
      } catch (error) {
        logger.error(
          `Error while getting installments from the Loan-tape database for (${loanIdString}).`
        );
        logger.error(
          `Error which is Encountered while getting the installments from the Loan-tape database is ${error}`
        );
        reject(error);
      }
    });
  };

  const getOverallPayments = (loanIdString, whereClauses) => {
    return new Promise(async (resolve, reject) => {
      try {
        logger.info(
          "Getting all the payments on the following loanids from the production database"
        );
        const overallPayments = await mysql.query(
          `Select * from overall_payment where ${whereClauses} order by id asc`,
          [],
          "prod"
        );
        logger.info("Retrived all the payments for the given loanids");
        resolve(overallPayments);
      } catch (error) {
        logger.error(
          `Error while getting the payments from the overall payments db for loan_ids in (${loanIdString})`
        );
        logger.error(`Description of the error is :`, error);
        reject(error);
      }
    });
  };

  const groupBy = (array, key) => {
    return array.reduce((result, currentValue) => {
      const keyValue = currentValue[key];
      if (!result[keyValue]) {
        result[keyValue] = [];
      }
      result[keyValue].push(currentValue);
      return result;
    }, {});
  };

  processWorker();
}
