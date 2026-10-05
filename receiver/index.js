const amqp = require('amqplib');
const fs = require('fs');
const path = require('path');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const QUEUE_NAME = process.env.QUEUE_NAME || 'file_processing_queue';
const SHARED_STORAGE_DIR = process.env.SHARED_STORAGE_DIR || path.join(__dirname, 'shared-data');
const WORKER_ID = process.env.HOSTNAME || `worker-${process.pid}`;

let isShuttingDown = false;
let activeJobs = 0;
let amqpChannel = null;
let amqpConnection = null;

const PROCESSING_TIME_MS = parseInt(process.env.PROCESSING_TIME_MS || '5000', 10);

async function connectWithRetry() {
  while (!isShuttingDown) {
    try {
      console.log(`[${WORKER_ID}] Attempting connection to RabbitMQ at ${RABBITMQ_URL}...`);
      amqpConnection = await amqp.connect(RABBITMQ_URL);
      console.log(`[${WORKER_ID}] Connected to RabbitMQ successfully.`);

      amqpConnection.on('error', (err) => {
        console.error(`[${WORKER_ID}] AMQP Connection error:`, err.message);
      });

      amqpConnection.on('close', () => {
        if (!isShuttingDown) {
          console.warn(`[${WORKER_ID}] AMQP Connection closed. Reconnecting in 5s...`);
          setTimeout(connectWithRetry, 5000);
        }
      });

      amqpChannel = await amqpConnection.createChannel();
      await amqpChannel.assertQueue(QUEUE_NAME, { durable: true });

      // =========================================================================
      // CRITICAL ARCHITECTURAL FIX: FAIR DISPATCH / COMPETING CONSUMERS
      // Setting prefetch(1) instructs RabbitMQ to NOT give more than 1 message
      // to this worker until it has processed and acknowledged the previous one.
      // This stops idle workers and avoids worker starvation.
      // =========================================================================
      await amqpChannel.prefetch(1);
      console.log(`[${WORKER_ID}] Fair dispatch configured (prefetch = 1). Listening on "${QUEUE_NAME}"...`);

      startConsuming(amqpChannel);
      break;
    } catch (err) {
      console.error(`[${WORKER_ID}] Connect failed: ${err.message}. Retrying in 5s...`);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

function startConsuming(channel) {
  // autoAck MUST be false so RabbitMQ waits for our explicit acknowledgment
  channel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;

    activeJobs++;
    const processingDuration = PROCESSING_TIME_MS;

    try {
      const payload = JSON.parse(msg.content.toString());
      const fileKey = payload.fileKey;
      const targetFilePath = path.join(SHARED_STORAGE_DIR, fileKey);

      console.log(`[${WORKER_ID}] [▶ STARTED] Processing: ${fileKey} | Est. time: ${(processingDuration / 1000).toFixed(2)}s`);

      // 1. Simulate reading and processing the file from the mounted shared storage
      if (fs.existsSync(targetFilePath)) {
        const fileContent = fs.readFileSync(targetFilePath, 'utf-8');
        // Simulated processing work
      } else {
        console.warn(`[${WORKER_ID}] [⚠ File Note] File path ${targetFilePath} not yet on disk or running mock.`);
      }

      // 2. Simulate CPU / I/O work duration (< 10 seconds)
      await new Promise((resolve) => setTimeout(resolve, processingDuration));

      console.log(`[${WORKER_ID}] [✔ FINISHED] Finished ${fileKey} in ${(processingDuration / 1000).toFixed(2)}s. Sending ACK.`);

      // 3. Explicitly acknowledge to pull the next message
      channel.ack(msg);
    } catch (err) {
      console.error(`[${WORKER_ID}] [✖ ERROR] Processing failed:`, err.message);
      // Don't requeue bad corrupted files; send to dead-letter exchange in production
      channel.nack(msg, false, false);
    } finally {
      activeJobs--;
    }
  }, { noAck: false });
}

// Graceful shutdown for Kubernetes pod termination (preStop / SIGTERM)
async function handleShutdown(signal) {
  console.log(`[${WORKER_ID}] Received ${signal}. Starting graceful shutdown...`);
  isShuttingDown = true;

  if (amqpChannel) {
    // Stop accepting new messages
    try {
      await amqpChannel.cancel(QUEUE_NAME);
    } catch (e) {}
  }

  // Wait for in-flight file processing to complete
  const checkInterval = setInterval(async () => {
    if (activeJobs === 0) {
      clearInterval(checkInterval);
      console.log(`[${WORKER_ID}] In-flight jobs drained. Closing RabbitMQ connection.`);
      try {
        if (amqpChannel) await amqpChannel.close();
        if (amqpConnection) await amqpConnection.close();
      } catch (err) {}
      process.exit(0);
    } else {
      console.log(`[${WORKER_ID}] Waiting for ${activeJobs} active job(s) to finish before shutdown...`);
    }
  }, 500);
}

process.on('SIGTERM', () => handleShutdown('SIGTERM'));
process.on('SIGINT', () => handleShutdown('SIGINT'));

connectWithRetry();
