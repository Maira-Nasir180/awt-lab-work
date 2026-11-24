const router = require('express').Router();
const { withTenant, pool } = require('../config/db');

// Serverless invoice metrics (cold vs warm) for this tenant
router.get('/', async (req, res, next) => {
  try {
    const rows = await withTenant(req.tenant.id, async (db) =>
      (await db.query(
        `SELECT order_id, cold_start, duration_ms, round_trip_ms, created_at
         FROM invoice_logs WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 50`, [req.tenant.id])).rows);
    const summary = await withTenant(req.tenant.id, async (db) =>
      (await db.query(
        `SELECT cold_start, COUNT(*)::int AS n, ROUND(AVG(duration_ms),2) AS avg_fn_ms, ROUND(AVG(round_trip_ms),2) AS avg_round_trip_ms
         FROM invoice_logs WHERE tenant_id=$1 GROUP BY cold_start`, [req.tenant.id])).rows);
    res.json({ summary, data: rows });
  } catch (e) { next(e); }
});

router.get('/pool/stats', (req, res) => {
  res.json({ total: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount, max: pool.options.max });
});

module.exports = router;
