const amqp = require('amqplib');
const fs = require('fs');
const path = require('path');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const QUEUE_NAME = process.env.QUEUE_NAME || 'file_processing_queue';
const SHARED_STORAGE_DIR = process.env.SHARED_STORAGE_DIR || path.join(__dirname, 'shared-data');
const SEND_INTERVAL_MS = parseInt(process.env.SEND_INTERVAL_MS || '2000', 10);

// Ensure shared directory exists
if (!fs.existsSync(SHARED_STORAGE_DIR)) {
  fs.mkdirSync(SHARED_STORAGE_DIR, { recursive: true });
}

let fileCounter = 1;

async function connectWithRetry() {
  while (true) {
    try {
      console.log(`[Sender] Attempting connection to RabbitMQ at ${RABBITMQ_URL}...`);
      const connection = await amqp.connect(RABBITMQ_URL);
      console.log('[Sender] Connected to RabbitMQ successfully.');

      connection.on('error', (err) => {
        console.error('[Sender] Connection error:', err.message);
      });

      connection.on('close', () => {
        console.warn('[Sender] Connection closed. Reconnecting in 5s...');
        setTimeout(connectWithRetry, 5000);
      });

      const channel = await connection.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });
      console.log(`[Sender] Queue "${QUEUE_NAME}" asserted.`);

      startPublishing(channel);
      break;
    } catch (err) {
      console.error(`[Sender] Failed to connect: ${err.message}. Retrying in 5 seconds...`);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

function startPublishing(channel) {
  setInterval(async () => {
    try {
      const fileName = `document_${Date.now()}_#${fileCounter++}.txt`;
      const fullFilePath = path.join(SHARED_STORAGE_DIR, fileName);

      // 1. Simulate Windows application writing the actual file to storage
      const fileContent = `Sample file content for ${fileName}\nGenerated at: ${new Date().toISOString()}`;
      fs.writeFileSync(fullFilePath, fileContent, 'utf-8');

      // 2. Prepare payload - passing relative path / storage key (Cross-platform best practice)
      const payload = {
        fileKey: fileName,
        sizeBytes: Buffer.byteLength(fileContent),
        createdAt: new Date().toISOString()
      };

      // 3. Publish to RabbitMQ with persistent delivery mode
      const messageBuffer = Buffer.from(JSON.stringify(payload));
      channel.sendToQueue(QUEUE_NAME, messageBuffer, { persistent: true });

      console.log(`[Sender] [x] Published: ${fileName} (Queue: ${QUEUE_NAME})`);
    } catch (err) {
      console.error('[Sender] Error publishing message:', err.message);
    }
  }, SEND_INTERVAL_MS);
}

connectWithRetry();
