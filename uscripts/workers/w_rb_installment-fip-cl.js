const mysql = require("../../connector/mysql");
const logger = require("../../logger/logger");
const { Transform, pipeline } = require("stream");
const { promisify } = require("util");

const pipelineAsync = promisify(pipeline);
const batchSize = process.env.STREAM_BATCH_SIZE

// Function to calculate the difference in days between two dates
function getDaysDifference(date1, date2) {
  const date1Ms = new Date(date1).getTime();
  const date2Ms = new Date(date2).getTime();
  const differenceMs = Math.abs(date1Ms - date2Ms);
  const daysDifference = Math.floor(differenceMs / (1000 * 60 * 60 * 24));
  return daysDifference;
}

// Batch stream to accumulate records and push them downstream in batches
function createBatchStream(batchSize = 100, callback) {
  let batch = [];

  return new Transform({
    objectMode: true,
    transform(chunk, encoding, done) {
      batch.push(chunk); 
      if (batch.length >= batchSize) {
        this.push(batch);
        batch = []; 
      }

      done();
    },
    flush(done) {
      if (batch.length > 0) {
        this.push(batch);
      }
      if (callback) {
        callback(); 
      }
      done();
    }
  });
}


function createProcessStream() {
  return new Transform({
    objectMode: true,
    async transform(batch, encoding, done) {
      try {
        let promises = batch.map((row) =>
          mysql.query(
            "update installment_fip_cl set days_past_due = ?, emi_status_id = ? where id = ?",
            [getDaysDifference(new Date(), row.inst_date), 4, row.id],
            "prod"
          )
        );
        await Promise.all(promises);
        logger.info(`Successfully updated the batch with ${batch.length} records.`);
        done();
      } catch (error) {
        logger.error("Error while updating installment_fip_cl:", error);
        done(error); // Pass the error to the stream
      }
    }
  });
}

// Main function to execute the job
const processJob = async () => {
  try {
    const conn = await mysql.getConnections('prod');
    const dataStream = conn.query("select * from installment_fip_cl where is_delete = 0 and customer_facing = 1 and inst_status = 1 and emi_status_id in (1,3) and inst_date < date(now())", [])
      .stream();

    const batchStream = createBatchStream(batchSize, () => {
      logger.info("All batches processed successfully.");
    });
    await pipelineAsync(dataStream, batchStream, createProcessStream());

    if (conn) conn.release();
    logger.info("Data streaming and processing completed successfully.");
    process.exit(0);
  } catch (error) {
    logger.error("Error while processing the job:", error);
    process.exit(1);
  }
};

// Execute the job
processJob();
