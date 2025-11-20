const { Pool } = require("pg");
const dotenv = require("dotenv");

dotenv.config();

// --- Configuration ---
const DB_CONNECTION_STRING = process.env.DATABASE_URL;
const NUM_USERS = 1000000; // Simulate 1 million users
const CONCURRENCY = 50000; // Number of simultaneous requests
const DB_POOL_SIZE = 50000; // Should match or slightly exceed concurrency
const SKIP_SETUP = false;

if (!DB_CONNECTION_STRING) {
  console.error("Error: DATABASE_URL is not defined in .env");
  process.exit(1);
}

// --- Database Pool ---
// A pool size of 10 million is unrealistic and will cause OS-level issues.
// It's better to limit the pool and queue requests in the application.
const pool = new Pool({
  connectionString: DB_CONNECTION_STRING,
  max: DB_POOL_SIZE,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

// --- Utilities ---
const generateRandomString = (length) => {
  let result = "";
  const characters =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  const charactersLength = characters.length;
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
};

const generateRandomData = (userId) => {
  const genders = ["Male", "Female", "Other"];
  return {
    name: `User ${generateRandomString(5)}`,
    email: `${generateRandomString(8)}@example.com`,
    user_id: userId,
    gender: genders[Math.floor(Math.random() * genders.length)],
    amount: (Math.random() * 1000).toFixed(2),
    wallet_address: `0x${generateRandomString(40)}`,
  };
};

// --- Setup ---
async function setupTable() {
  if (SKIP_SETUP) {
    console.log("Skipping table setup...");
    return;
  }

  const client = await pool.connect();
  try {
    console.log("Setting up database schema...");
    await client.query("DROP TABLE IF EXISTS stress_test_data");
    await client.query(`
      CREATE TABLE stress_test_data (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(50) NOT NULL,
        name VARCHAR(100),
        email VARCHAR(100),
        gender VARCHAR(20),
        amount DECIMAL(10, 2),
        wallet_address VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    await client.query(
      `CREATE INDEX idx_user_id ON stress_test_data(user_id);`
    );
    console.log("Table 'stress_test_data' recreated with new schema.");
  } catch (err) {
    console.error("Error setting up table:", err);
    throw err;
  } finally {
    client.release();
  }
}

// --- Statistics ---
const stats = {
  completed: 0,
  errors: 0,
  totalDuration: 0,
  latencies: [],
};

// --- Worker Action ---
async function performUserAction(userId) {
  let client;
  const start = Date.now();
  try {
    client = await pool.connect();

    const data = generateRandomData(userId);

    // 1. Write
    await client.query(
      "INSERT INTO stress_test_data (user_id, name, email, gender, amount, wallet_address) VALUES ($1, $2, $3, $4, $5, $6)",
      [
        data.user_id,
        data.name,
        data.email,
        data.gender,
        data.amount,
        data.wallet_address,
      ]
    );

    // 2. Read
    await client.query(
      "SELECT * FROM stress_test_data WHERE user_id = $1 ORDER BY created_at DESC LIMIT 1",
      [userId]
    );

    const duration = Date.now() - start;

    // Update stats safely (JS is single threaded for sync operations)
    stats.completed++;
    stats.totalDuration += duration;
    stats.latencies.push(duration);
  } catch (err) {
    stats.errors++;
    // console.error(`Error for user ${userId}: ${err.message}`);
  } finally {
    if (client) client.release();
  }
}

// --- Main Execution ---
async function runStressTest() {
  console.log("\n===========================================");
  console.log("         DB STRESS TEST: ROBUST EDITION    ");
  console.log("===========================================");
  console.log(`Target DB:          ${new URL(DB_CONNECTION_STRING).hostname}`);
  console.log(`Total Requests:     ${NUM_USERS}`);
  console.log(`Concurrency:        ${CONCURRENCY}`);
  console.log(`Pool Size:          ${DB_POOL_SIZE}`);
  console.log("===========================================\n");

  try {
    await setupTable();
  } catch (e) {
    console.error("Failed to setup DB. Aborting.");
    process.exit(1);
  }

  console.log("Starting stress test...");
  const startTime = Date.now();

  // Worker Pool Pattern
  // We create 'CONCURRENCY' number of workers that keep pulling tasks until done.
  let tasksStarted = 0;

  const workers = [];

  for (let i = 0; i < CONCURRENCY; i++) {
    workers.push(
      (async () => {
        while (true) {
          // Atomically get the next task index
          const taskIndex = tasksStarted++;
          if (taskIndex >= NUM_USERS) break;

          const userId = `user_${taskIndex}_${Date.now()}`;
          await performUserAction(userId);

          // Progress Logging
          if (
            stats.completed > 0 &&
            stats.completed % 1000 === 0 &&
            taskIndex % 1000 === 0
          ) {
            const elapsed = (Date.now() - startTime) / 1000;
            const rate = (stats.completed / elapsed).toFixed(2);
            process.stdout.write(
              `\rProgress: ${stats.completed}/${NUM_USERS} (${rate} ops/sec) | Errors: ${stats.errors}`
            );
          }
        }
      })()
    );
  }

  // Wait for all workers to finish
  await Promise.all(workers);

  const totalTime = (Date.now() - startTime) / 1000;
  const throughput = stats.completed / totalTime;

  // Calculate Percentiles
  stats.latencies.sort((a, b) => a - b);
  const getPercentile = (p) => {
    if (stats.latencies.length === 0) return 0;
    const index = Math.floor((p / 100) * stats.latencies.length);
    return stats.latencies[index];
  };

  console.log("\n\n===========================================");
  console.log("           STRESS TEST RESULTS             ");
  console.log("===========================================");
  console.log(`Total Requests:     ${NUM_USERS}`);
  console.log(`Successful:         ${stats.completed}`);
  console.log(`Failed:             ${stats.errors}`);
  console.log(`Total Time:         ${totalTime.toFixed(2)} s`);
  console.log(`Throughput:         ${throughput.toFixed(2)} ops/sec`);
  console.log("-------------------------------------------");
  console.log(
    `Latency (avg):      ${(
      stats.totalDuration / (stats.completed || 1)
    ).toFixed(2)} ms`
  );
  console.log(`Latency (p50):      ${getPercentile(50)} ms`);
  console.log(`Latency (p95):      ${getPercentile(95)} ms`);
  console.log(`Latency (p99):      ${getPercentile(99)} ms`);
  console.log(
    `Latency (max):      ${stats.latencies[stats.latencies.length - 1] || 0} ms`
  );
  console.log("===========================================");

  await pool.end();
}

runStressTest().catch((err) => console.error(err));
