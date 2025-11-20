const { Pool } = require("pg");
const dotenv = require("dotenv");

dotenv.config();

// --- Configuration ---
// Hardcoded settings as requested, but now defining the ramp-up strategy
const DB_CONNECTION_STRING = process.env.DATABASE_URL;

const STAGE_REQUESTS = 2000; // Number of requests to run per concurrency level
const INITIAL_CONCURRENCY = 10; // Start with 10 concurrent connections
const CONCURRENCY_STEP = 10; // Increase by 10 each step
const MAX_CONCURRENCY = 500; // Stop if we reach this level
const MAX_ERROR_RATE = 0.05; // Stop if > 5% errors
const SKIP_SETUP = false;

if (!DB_CONNECTION_STRING) {
  console.error("Error: DATABASE_URL is not defined in .env");
  process.exit(1);
}

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

// --- Database Setup (One-time) ---
async function setupTable(pool) {
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

// --- Single User Action ---
async function performUserAction(pool, userId) {
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
    return { success: true, duration };
  } catch (err) {
    return { success: false, error: err };
  } finally {
    if (client) client.release();
  }
}

// --- Run a Single Stage ---
async function runStage(concurrency, requestCount) {
  console.log(`\n--- Starting Stage: Concurrency ${concurrency} ---`);

  // Create a specific pool for this stage to test connection limits
  const pool = new Pool({
    connectionString: DB_CONNECTION_STRING,
    max: concurrency, // Set pool max exactly to concurrency to test DB limits
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 5000, // Fast fail if DB is full
  });

  // Handle pool errors to prevent crash
  pool.on("error", (err) => {
    // console.error('Unexpected error on idle client', err);
  });

  const stats = {
    completed: 0,
    errors: 0,
    totalDuration: 0,
    latencies: [],
    connErrors: 0,
  };

  const startTime = Date.now();
  let tasksStarted = 0;
  const workers = [];

  for (let i = 0; i < concurrency; i++) {
    workers.push(
      (async () => {
        while (true) {
          const taskIndex = tasksStarted++;
          if (taskIndex >= requestCount) break;

          const userId = `user_${concurrency}_${taskIndex}_${Date.now()}`;
          const result = await performUserAction(pool, userId);

          if (result.success) {
            stats.completed++;
            stats.totalDuration += result.duration;
            stats.latencies.push(result.duration);
          } else {
            stats.errors++;
            // Check if it's a connection error
            if (
              result.error.code === "53300" ||
              result.error.message.includes("timeout")
            ) {
              stats.connErrors++;
            }
          }
        }
      })()
    );
  }

  await Promise.all(workers);

  await pool.end();

  const totalTime = (Date.now() - startTime) / 1000;
  const throughput = stats.completed / totalTime;
  const avgLatency =
    stats.completed > 0 ? stats.totalDuration / stats.completed : 0;
  const errorRate = stats.errors / requestCount;

  console.log(`Stage Completed:`);
  console.log(`  Throughput: ${throughput.toFixed(2)} ops/sec`);
  console.log(`  Avg Latency: ${avgLatency.toFixed(2)} ms`);
  console.log(`  Errors: ${stats.errors} (${(errorRate * 100).toFixed(1)}%)`);

  if (stats.connErrors > 0) {
    console.log(
      `  WARNING: ${stats.connErrors} connection errors detected (DB limit hit?)`
    );
  }

  return {
    concurrency,
    throughput,
    avgLatency,
    errorRate,
    hasCriticalFailure: stats.connErrors > 0 || errorRate > MAX_ERROR_RATE,
  };
}

// --- Main Progressive Test ---
async function runProgressiveTest() {
  console.log("\n===========================================");
  console.log("      PROGRESSIVE DB STRESS TEST           ");
  console.log("===========================================");
  console.log(`Target DB:          ${new URL(DB_CONNECTION_STRING).hostname}`);
  console.log(
    `Ramp Up:            ${INITIAL_CONCURRENCY} -> ${MAX_CONCURRENCY} (Step: ${CONCURRENCY_STEP})`
  );
  console.log(`Requests per Stage: ${STAGE_REQUESTS}`);
  console.log("===========================================\n");

  // Initial setup using a small pool
  const setupPool = new Pool({
    connectionString: DB_CONNECTION_STRING,
    max: 5,
  });
  try {
    await setupTable(setupPool);
  } catch (e) {
    console.error("Setup failed:", e.message);
    process.exit(1);
  } finally {
    await setupPool.end();
  }

  let currentConcurrency = INITIAL_CONCURRENCY;
  const report = [];

  while (currentConcurrency <= MAX_CONCURRENCY) {
    const result = await runStage(currentConcurrency, STAGE_REQUESTS);
    report.push(result);

    if (result.hasCriticalFailure) {
      console.log("\n!!! CRITICAL FAILURE DETECTED. STOPPING TEST. !!!");
      console.log(
        `Stable concurrency limit seems to be around ${
          currentConcurrency - CONCURRENCY_STEP
        }`
      );
      break;
    }

    currentConcurrency += CONCURRENCY_STEP;
    // Small pause between stages
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  console.log("\n\n===========================================");
  console.log("           FINAL REPORT                    ");
  console.log("===========================================");
  console.log("Concurrency | TPS      | Latency (ms) | Error Rate");
  console.log("-------------------------------------------");
  report.forEach((r) => {
    console.log(
      `${r.concurrency.toString().padEnd(12)}| ${r.throughput
        .toFixed(0)
        .padEnd(9)}| ${r.avgLatency.toFixed(2).padEnd(13)}| ${(
        r.errorRate * 100
      ).toFixed(1)}%`
    );
  });
  console.log("===========================================");
}

runProgressiveTest().catch((err) => console.error(err));
