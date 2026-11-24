// Generates a large dataset so EXPLAIN ANALYZE differences are measurable.
// usage: npm run seed:bulk            (default 200k orders for acme, 50k for globex)
require('dotenv').config();
const { pool, withTenant } = require('../src/config/db');
const PLAN = [['acme', parseInt(process.env.BULK_ACME || '200000', 10)], ['globex', parseInt(process.env.BULK_GLOBEX || '50000', 10)]];

(async () => {
  for (const [slug, n] of PLAN) {
    const { rows: [t] } = await pool.query('SELECT id FROM tenants WHERE slug=$1', [slug]);
    if (!t) throw new Error('Run `npm run seed` first');
    await withTenant(t.id, async (db) => {
      const { rows: [{ c }] } = await db.query("SELECT COUNT(*)::int c FROM orders WHERE tenant_id=$1 AND customer_name LIKE 'Bulk %'", [t.id]);
      if (c >= n) { console.log(`${slug}: already has ${c} bulk orders`); return; }
      await db.query(
        `INSERT INTO orders (tenant_id, customer_name, status, total, created_at)
         SELECT $1, 'Bulk ' || g, 'PROCESSED', round((random()*900)::numeric, 2), now() - (random() * interval '365 days')
         FROM generate_series(1, $2::int) g`, [t.id, n - c]);
      await db.query(
        `INSERT INTO order_items (tenant_id, order_id, product_id, quantity, unit_price)
         SELECT o.tenant_id, o.id, p.id, 1 + floor(random()*5)::int, p.price
         FROM orders o
         CROSS JOIN LATERAL (SELECT id, price FROM products WHERE tenant_id = o.tenant_id ORDER BY random() LIMIT 3) p
         WHERE o.tenant_id = $1 AND o.customer_name LIKE 'Bulk %'
           AND NOT EXISTS (SELECT 1 FROM order_items x WHERE x.order_id = o.id)`, [t.id]);
      console.log(`${slug}: inserted ${n - c} bulk orders with items`);
    });
  }
  await pool.query('ANALYZE orders'); await pool.query('ANALYZE order_items');
  await pool.end();
})().catch((e) => { console.error(e); process.exit(1); });
