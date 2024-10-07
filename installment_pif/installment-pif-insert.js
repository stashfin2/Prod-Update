const { Worker } = require('worker_threads');
const mysql = require('./connector/mysql');
const { pipeline } = require('node:stream/promises');
const { Transform } = require('node:stream');
const logger = require('./logger/logger');
const path = require('path');

// Track number of active workers
let activeWorkers = 0;
const maxWorkers = Number(process.env.MAX_WORKERS || 5); // Control how many workers can run concurrently
let retries = {}; // Track retries per batch

const processJob = async () => {
    try {
        const conn = await mysql.getConnections();
        const dataStream = conn.query('SELECT * FROM st_ksf_customer', []).stream();
        await pipeline(dataStream, batchStream); // Process data pipeline
        if (conn) conn.release();
    } catch (error) {
        logger.error('Error while initiating the amortization process:', error);
        handleError(error);
    }
};

const batchStream = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
        this.buffer = (this.buffer || []).concat(chunk);
        if (this.buffer.length >= Number(process.env.STREAM_BATCH_SIZE || 100)) {
            if (activeWorkers < maxWorkers) {
                processBatchInWorker(this.buffer); // Process batch using worker
                this.buffer = [];
            } else {
                setTimeout(() => this.push(this.buffer), 100); // Retry if worker limit is reached
            }
        }
        callback();
    },
    flush(callback) {
        if (this.buffer.length > 0) {
            processBatchInWorker(this.buffer); // Process remaining batch
        }
        callback();
    },
});

const processBatchInWorker = (batchData) => {
    const loanIds = batchData.map((item) => item.loan_id).join(',');

    if (!retries[loanIds]) retries[loanIds] = 0; // Initialize retries for the batch
    activeWorkers++;

    const worker = new Worker(path.resolve(__dirname, 'workerInstallment.js'), {
        workerData: batchData,
    });

    worker.on('message', (msg) => {
        if (msg.success) {
            logger.info('Batch processed successfully.');
            activeWorkers--;
        } else {
            logger.error(`Batch processing failed: ${msg.error}`);
            retries[loanIds] += 1;

            if (retries[loanIds] <= 3) { // Retry up to 3 times
                logger.info(`Retrying batch for loan IDs: ${loanIds}. Retry attempt: ${retries[loanIds]}`);
                processBatchInWorker(batchData); // Retry the batch
            } else {
                logger.error(`Max retry attempts reached for loan IDs: ${loanIds}. Pushing to dead-letter queue.`);
                pushToDeadLetterQueue(batchData); // Push to dead-letter queue after max retries
            }
            activeWorkers--;
        }
    });

    worker.on('error', (error) => {
        logger.error('Worker encountered an error:', error);
        activeWorkers--;
    });

    worker.on('exit', (code) => {
        if (code !== 0) {
            logger.error(`Worker stopped with exit code ${code}`);
        }
        activeWorkers--;
    });
};

// Function to push failed batches to dead-letter queue (DLQ)
const pushToDeadLetterQueue = (batchData) => {
    logger.error(`Pushing failed batch to dead-letter queue.`);
    // Implementation for pushing the failed batch to the DLQ.
};

processJob();
