const path = require("path");
const mysql = require("../connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("../logger/logger");

let batchCount = 0
let inProgressCount = 0
let dataCount = 0

const processJob = async () => {
    try {
        const conn = await mysql.getConnections();
        const dataStream = conn.query('select * from st_ksf_customer where loan_id = 417638', []).stream();
        await pipeline(dataStream, batchStream, createProcessStream());
        if (conn) conn.release();
        process.exit(0);
    } catch (error) {
        logger.error("Error while initiating data transfer for installment_payment_fip process");
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
            await startOperations(data);
            callback()
        },
    });


const startOperations = async (data) =>{
    try{
        const loanIds = data.map((each)=> each.loan_id)
        const loanIdString = loanIds.join(',')
        let paymentRecords = await getOverallPayments(loanIdString)
        paymentRecords = groupBy(paymentRecords, 'loan_id')
        let installmentsFip = await mysql.query(`Select * from installment_fip where loan_id in (${loanIdString}) and is_delete = 0 order by inst_number asc`, [], 'prod')
        installmentsFip = installmentsFip.  map((each) => {
            each['amount_outstanding_principal'] = each.inst_principal
            each['amount_outstanding_interest'] = each.inst_interest
            each['received_principal'] = 0
            each['received_interest'] = 0
            return each
        })
        const groupedFip = groupBy(installmentsFip, 'loan_id') 
        let installmentPaymentFipInsertionArray = []
        for(let loanId in paymentRecords){ 
            if(paymentRecords[loanId].length > 0){
                for (let record of paymentRecords[loanId]) {
                    let [updatedInstallmentFIPArray, insFip, bucketAmountpif] = generatePayments(record['amt_payment'], record, groupedFip[loanId], 'fip', 'col', 0)
                    groupedFip[loanId] = updatedInstallmentFIPArray
                    installmentPaymentFipInsertionArray.push(...insFip)
                }
            }
        }
        const paymentsArray = createPaymentInst(installmentPaymentFipInsertionArray)
        const result = await insertPaymentsArray(paymentsArray, loanIdString)
        return result
    }catch(error){
        logger.error(`Error while performing the operation:`, error)
        throw error
    }
}

const generatePayments = (bucketAmount, payment, installments, table, col, instNumber) => {
    try {

        let installmentsFacing
        let installmentsNonFacing
        let nonFacingUpdation = []
        let insertionArray = []

        if(!installments){
            return [[], insertionArray, bucketAmount]
        }
        if (installments && col == 'col' && installments.length > 0) {
            installmentsFacing = installments.filter((inst) => inst.is_delete === 0 && inst.customer_facing === 1)
            installmentsNonFacing = installments.filter((inst) => inst.is_delete === 0 && inst.customer_facing === 0)

        } else if (installments && col !== 'col' && installments.length > 0){
            installmentsFacing = installments
            installmentsNonFacing = installments.filter((each) => each.instNumber === instNumber)
        }

        if(!installmentsFacing){
            return [installments, insertionArray, bucketAmount]
        }

        for (let principal of installmentsFacing) {
            if (col === 'col') {
                let [colUp, colIn, colBucketAmount] = generatePayments(bucketAmount, payment, installmentsNonFacing.filter((inst) => inst.inst_number === principal.inst_number), table ,'', principal.inst_number)
                nonFacingUpdation.push(...colUp)
                insertionArray.push(...colIn)
            }

            
            let [interestObj, installmentObject, bucketAmountRet] = calculateInterest(principal, payment, bucketAmount, table)
            principal = interestObj
            if (Object.keys(installmentObject).length > 0) {
                insertionArray.push(installmentObject)
            }
            bucketAmount = bucketAmountRet

            if (bucketAmount > 0) {
                let [principalObj, installmentObject, bucketAmountRet] = calculatePricipal(principal, payment, bucketAmount, table)
                principal = principalObj
                bucketAmount = bucketAmountRet
                if (Object.keys(installmentObject).length > 0) {
                    insertionArray.push(installmentObject)
                }
            }

            if (bucketAmount <= 0) {
                break
            }
        }
        if(nonFacingUpdation.length > 0){
            installmentsNonFacing = mergeArrays(nonFacingUpdation, installmentsNonFacing)
            installments = sortByInstDateAndNumber([...installmentsFacing, ...installmentsNonFacing])
        }
        installments = mergeArrays(nonFacingUpdation, installments)
        return [installments, insertionArray, bucketAmount]

    } catch (error) {
        if(col === 'col'){
            logger.error("Error while generating payments for colenders>>>>")
        }else{
            logger.error("Error while generating payments for cutomer facing>>>>")
        }
       logger.error(`Error occured while generating payments array:`,error)
    }
}

const mergeArrays = (array1, array2) => {
    const map = new Map()
    array1.forEach(obj => {
        const key = `${obj.id}_${obj.inst_number}_${obj.inst_date}`
        map.set(key, obj)
    });
    array2.forEach(obj => {
        const key = `${obj.id}_${obj.inst_number}_${obj.inst_date}`
        if (map.has(key)) {
            const index = array2.findIndex(o => o.id === obj.id && o.inst_number === obj.inst_number && o.inst_date === obj.inst_date)
            array2[index] = map.get(key);
        }
    });
    return array2
}


const sortByInstDateAndNumber = (arr) => {
    return arr.sort((a, b) => {
      const dateComparison = new Date(a.inst_date) - new Date(b.inst_date);
      if (dateComparison !== 0) {
        return dateComparison;
      }
      return a.inst_number - b.inst_number;
    });
}


const calculatePricipal = ( principal, payment, bucketAmount, table) => {
    let commonIpInsObject = {}
    let availableBalancePrincipal = bucketAmount
    let outstandingPrincipal = principal['amount_outstanding_principal']
    if (outstandingPrincipal > 0) {
        availableBalancePrincipal = availableBalancePrincipal - outstandingPrincipal
        if (availableBalancePrincipal >= 0){
            let recievedPrincipal = principal['inst_principal']
            principal['received_principal'] = recievedPrincipal
            principal['amount_outstanding_principal'] = 0
            principal['last_paying_date'] = payment['received_date']
            principal['updated'] = true
            if(table === 'fip'){
                principal['emi_status_id']  = 2
            }
            commonIpInsObject = createInsertionObject(principal, outstandingPrincipal,payment, 'pri')
            bucketAmount = bucketAmount - outstandingPrincipal
        } else {

            let recievedPrincipal = bucketAmount + principal['received_principal']
            let outstandingAmountPrincipal = principal['amount_outstanding_principal'] - bucketAmount;
            principal['received_principal'] = recievedPrincipal
            principal['amount_outstanding_principal'] = outstandingAmountPrincipal
            principal['last_paying_date'] = payment['received_date']
             principal['updated'] = true
            commonIpInsObject = createInsertionObject(principal, bucketAmount, payment, 'pri')
            bucketAmount = 0
        }
    }
    return [principal, commonIpInsObject, bucketAmount]
}


const calculateInterest =(principal, payment, bucketAmount, table) => {
    let availableBalanceInterest = bucketAmount
    let outstandingInterest = principal['amount_outstanding_interest']
    let commonIpInsObject = {}
    if (outstandingInterest > 0) {
        availableBalanceInterest = availableBalanceInterest - outstandingInterest
        if (availableBalanceInterest >= 0) {
            let recievedInterest = principal['inst_interest']
            principal['received_interest'] =  recievedInterest,
            principal['amount_outstanding_interest'] = 0
            principal['last_paying_date'] = payment['received_date']
            principal['updated'] = true
            if(table === 'pif'){
                principal['emi_status_id'] =  2
            }
            commonIpInsObject = createInsertionObject(principal, outstandingInterest, payment, 'int')
            bucketAmount = bucketAmount - outstandingInterest
        } else {
            let receivedInterest = bucketAmount + principal['received_interest'] 
            let outstandingAmountInterst = principal['amount_outstanding_interest'] - bucketAmount
            principal['received_interest'] = receivedInterest
            principal['amount_outstanding_interest'] = outstandingAmountInterst
            principal['last_paying_date'] =  payment['received_date']
            principal['updated'] = true
            commonIpInsObject = createInsertionObject(principal, bucketAmount, payment, 'int')
            bucketAmount = 0
        }
    }

    return [principal, commonIpInsObject, bucketAmount]
}



const createInsertionObject = (principal,amount, payment, type) => {
    let commonIpInsObject = {
        'customer_id': principal['customer_id'],
        'loan_id': principal['loan_id'],
        'inst_id': principal['id'],
        'inst_number': principal['inst_number'],
        'code_payment_type': type === 'pri' ? 2 : 3,
        'amount_payment': amount,
        'payment_status': 1,
        'payment_pairing_date': new Date(),
        'payment_date': payment['received_date'],
        'payment_id': payment['id']
    }
    return commonIpInsObject
}


const insertPaymentsArray = async (paymentsArray, loanIdString) => {
    try{
        logger.info('Initializing the insertion process in the installment_payment_fip table.')
        const insertQuery = 'Insert into installment_payment_fip set (customer_id, loan_id, inst_id, inst_number, code_payment_type, code_payment_type, amount_payment, payment_status, payment_pairing_date, payment_date, payment_id) VALUES ?'
        const result = await mysql.query(insertQuery, [paymentsArray], 'prod')
        logger.info(`Successfully inserted ${result.affectedRows} in the installment_payments_fip table prod`)
        return result
    }catch(error){
        logger.error(`Error while inserting in installment_payment_fip table for loanIds (${loanIdString})`)
        logger.error(`Complete error description is:`, error)
        throw error
    }
}

const createPaymentInst = (array) => {
    try{
        const compatibleArray = array.map((item) => [item.customer_id, item.loan_id, item.inst_id, item.inst_number, item.code_payment_type,
            item.code_payment_type, item.amount_payment, item.payment_status, item.payment_pairing_date, item.payment_date, item.payment_id
        ])
        return compatibleArray
    }catch(error){
        logger.error(`Error while creating payments installments:`, error)
        throw error
    }
}



const deleteInstallmentsFromProd = (loanIdString) => {
    return new Promise(async (resolve, reject ) => {
        try{
            logger.info('Initiating the installments deletion process from installment_fip table in the production database')
            const deleteDataFromProd =  `DELETE FROM overall_payment WHERE loan_id in (${loanIdString})`
            const deleteOp = await mysql.query(deleteDataFromProd, [], 'prod')
            logger.info(`Successfully completed the installments deletion process from the production database`)
            resolve(deleteOp)
        }catch(error){
            logger.error(`Error while deleting installments from the installment-fip table for (${loanIdString})`)
            logger.error('Cause of the error is:>', error)
            reject(error)
        }
    })
}

const getInstallments = (loanIdString) => {
    return new Promise(async(resolve , reject) => {
        try{
            logger.info(`Getting the installments from the Loan-Tape database for inserting`)
            const loanTapeInstallments = await mysql.query(`Select * from installment_fip where loan_id in (${loanIdString})`, [], 'loan-tape')
            logger.info(`Got the installments from the Loan-Tape database`)
            resolve(loanTapeInstallments)
        }catch(error){
            logger.error(`Error while getting installments from the Loan-tape database for (${loanIdString}).`)
            logger.error(`Error which is Encountered while getting the installments from the Loan-tape database is ${error}`)
            reject(error)
        }
    })
}


const getOverallPayments = (loanIdString) => {
    return new Promise(async(resolve, reject) => {
        try{
            logger.info('Getting all the payments on the following loanids from the production database')
            const overallPayments = await mysql.query(`Select * from overall_payment where loan_id in (${loanIdString}) order by id asc`, [] , 'prod')
            logger.info('Retrived all the payments for the given loanids')
            resolve(overallPayments)
        }catch(error){
            logger.error(`Error while getting the payments from the overall payments db for loan_ids in (${loanIdString})`)
            logger.error(`Description of the error is :`, error)
            reject(error)
        }
    })
}

const groupBy = (array, key) => {
    return array.reduce((result, currentValue) => {
        const keyValue = currentValue[key];
        if (!result[keyValue]) {
            result[keyValue] = [];
        }
        result[keyValue].push(currentValue);
        return result;
    }, {});
}

processJob()


// for(let op in groupedOverallPayments){
//     let sortedInstPaym = sortArrayByKey(groupedPaymentsFip[op], 'inst_number')
//     groupedPaymentsFip[op] = sortedInstPaym
//     let sortedOvPaym = sortArrayByKey(groupedOverallPayments[op], 'id')
//     groupedOverallPayments[op] = sortedOvPaym
//     for(let ipf in sortedInstPaym){
//         let gr1 = groupBy()

//     }
// }

// const sortArrayByKey = (array, key) => {
//     return array.sort((a, b) => {
//         if (a[key] < b[key]) return -1;
//         if (a[key] > b[key]) return 1;
//         return 0;
//     });
// }


// const populateInstId = (prodInst, loanTapePayments) =>{
//     try{
//         for (let inst of prodInst) {
//             for (let paym of loanTapePayments) {
//                 if (inst.customer_id === paym.customer_id && inst.loan_id === paym.loan_id && inst.inst_number === paym.inst_number && (inst.inst_principal === paym.amount_payment || inst.inst_interest === paym.amount_payment)) {
//                     paym['inst_id'] = inst.id
//                     break
//                 }
//             }
//         }
//         return prodInst
//     }catch(error){

//     }
// }