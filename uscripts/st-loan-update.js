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
        const dataStream = conn.query('select * from loan_tape_data where sl = 0 group by loan_id limit 100', []).stream();
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
        try {
            this.buffer = this.buffer || [];
            this.buffer = this.buffer.concat(chunk);
            if (this.buffer.length >= Number(process.env.STREAM_BATCH_SIZE || 100)) {
                dataCount = dataCount + this.buffer.length;
                batchCount = batchCount + 1;
                logger.info(`Batch count:>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>${batchCount}`);
                this.push(this.buffer);
                this.buffer = [];
            }

            callback(); // Proceed to the next chunk
        } catch (error) {
            logger.error("Error in the transform function: ", error);
            callback(error); // Pass the error to the callback
        }
    },
    flush(callback) {
        try {
            if (this.buffer && this.buffer.length > 0) {
                this.push(this.buffer);
            }
            callback(); 
        } catch (error) {
            logger.error("Error in the flush function: ", error);
            callback(error);
        }
    },
});

const createProcessStream = () =>
    new Transform({
        objectMode: true,
        async transform(data, encoding, callback) {
            inProgressCount = inProgressCount + 1;
            try{
                logger.info(`Started with the loan updation process for batch size ${data.length}`)
                const loanIds = data.map((each) => each.loan_id)
                const loanIdStrings = loanIds.join(',')
                console.log("LoanIdString>>>>", loanIdStrings)
                const loanRecords = await mysql.query(`Select * from st_loan where id in (${loanIdStrings})`, [], 'loan-tape')
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
            }catch(error) {
                logger.error(`Error while getting loans:`, error)
                throw error
            }
            callback()
        },
    });




processJob();
