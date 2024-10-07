const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const path = require("path");
const mysql = require("./connector/mysql");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");
const logger = require("./logger/logger");

const numWorkers = Number(process.env.NUM_WORKERS) || 4; // Number of worker threads from env
const streamBatchSize = Number(process.env.STREAM_BATCH_SIZE) || 100; // Batch size from env
let activeWorkers = 0;
let workers = [];
let taskQueue = [];
let dataCount = 0;
let batchCount = 0;

// Function to handle processing by worker threads
const createWorker = (data) => {
    return new Promise((resolve, reject) => {
        const worker = new Worker(path.resolve(__dirname, 'worker-insert-payment.js'), {
            workerData: data,
        });

        worker.on('message', (result) => resolve(result));
        worker.on('error', (err) => reject(err));
        worker.on('exit', (code) => {
            if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
        });
    });
};

// Worker manager to control parallel processing
const manageWorkers = async () => {
    while (taskQueue.length > 0 || activeWorkers > 0) {
        if (activeWorkers < numWorkers && taskQueue.length > 0) {
            const batch = taskQueue.shift();
            activeWorkers++;
            createWorker(batch)
                .then((result) => {
                    logger.info(`Worker finished processing a batch`);
                    activeWorkers--;
                    if (taskQueue.length > 0) {
                        manageWorkers(); // Check for remaining tasks
                    }
                })
                .catch((error) => {
                    logger.error(`Error in worker processing:`, error);
                    activeWorkers--;
                    if (taskQueue.length > 0) {
                        manageWorkers(); // Check for remaining tasks
                    }
                });
        }
    }
};

// Stream processing
const processJob = async () => {
    try {
        const conn = await mysql.getConnections();
        const dataStream = conn.query('select * from st_ksf_customer', []).stream();
        await pipeline(dataStream, batchStream, createProcessStream());
        await manageWorkers(); // Start managing workers
        if (conn) conn.release();
        process.exit(0);
    } catch (error) {
        logger.error("Error while initiating the payment process");
        
    }
};

// Batch stream transformation
const batchStream = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
        this.buffer = (this.buffer || []).concat(chunk);
        if (this.buffer.length >= streamBatchSize) {
            dataCount += this.buffer.length;
            batchCount++;
            logger.info(`Batch count: ${batchCount}`);
            taskQueue.push(this.buffer); // Push batch to task queue
            this.buffer = [];
        }
        callback();
    },
    flush(callback) {
        if (this.buffer.length > 0) taskQueue.push(this.buffer); // Flush remaining buffer
        callback();
    },
});

// Create process stream
const createProcessStream = () =>
    new Transform({
        objectMode: true,
        async transform(data, encoding, callback) {
            taskQueue.push(data); // Add to task queue
            callback();
        },
    });

processJob();
