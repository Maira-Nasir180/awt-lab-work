require('dotenv').config();
const { Pool } = require('pg');

// Connection pooling: a bounded set of reusable connections shared by all requests.
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false,
  max: parseInt(process.env.PG_POOL_MAX || '10', 10),   // max concurrent connections
  min: 2,
  idleTimeoutMillis: 30000,                              // close idle clients after 30s
  connectionTimeoutMillis: 5000,                         // fail fast if pool exhausted
});
pool.on('error', (err) => console.error('[pg-pool] idle client error', err.message));

/**
 * Tenant-scoped DB access. Runs fn inside a transaction that first sets app.tenant_id,
 * which the Postgres RLS policies use. Combined with explicit `WHERE tenant_id = $n`
 * in each query this gives two layers of isolation.
 */
async function withTenant(tenantId, fn) {
  if (!tenantId) throw new Error('withTenant called without tenantId');
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenantId]);
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw e;
  } finally {
    client.release();
  }
}

module.exports = { pool, withTenant };
