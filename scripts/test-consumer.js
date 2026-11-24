// Tests consumer business logic WITHOUT a broker (simulates Kafka messages). usage: npm run test:consumer
require('dotenv').config();
const crypto = require('crypto');
const { pool, withTenant } = require('../src/config/db');
const { handleOrderCreated, callInvoiceFunction } = require('../src/consumer');
let pass = 0, fail = 0;
const ok = (n, c, x = '') => { c ? pass++ : fail++; console.log(`${c ? 'PASS' : 'FAIL'}  ${n} ${c ? '' : x}`); };

(async () => {
  const { rows: [t] } = await pool.query("SELECT id, slug FROM tenants WHERE slug='acme'");
  const prod = await withTenant(t.id, async (db) => (await db.query("SELECT id, stock FROM products WHERE tenant_id=$1 AND sku='ACME-005'", [t.id])).rows[0]);
  const mkOrder = (qty) => withTenant(t.id, async (db) => {
    const { rows: [o] } = await db.query("INSERT INTO orders (tenant_id, customer_name, total) VALUES ($1,'consumer-test',1) RETURNING id", [t.id]);
    await db.query('INSERT INTO order_items (tenant_id, order_id, product_id, quantity, unit_price) VALUES ($1,$2,$3,$4,1)', [t.id, o.id, prod.id, qty]);
    return o.id;
  });
  const status = (id) => withTenant(t.id, async (db) => (await db.query('SELECT status, failure_reason FROM orders WHERE tenant_id=$1 AND id=$2', [t.id, id])).rows[0]);
  const stock = () => withTenant(t.id, async (db) => (await db.query('SELECT stock FROM products WHERE id=$1', [prod.id])).rows[0].stock);
  const ev = (orderId, qty) => ({ eventId: crypto.randomUUID(), type: 'OrderCreated', tenantId: t.id, tenantSlug: t.slug, orderId, customerName: 'consumer-test', total: 1, items: [{ productId: prod.id, quantity: qty, unitPrice: 1 }] });
  const meta = (o) => ({ topic: 'tenant.orders', partition: 0, offset: String(o) });

  const s0 = await stock();
  const o1 = await mkOrder(3); const e1 = ev(o1, 3);
  ok('processes event', (await handleOrderCreated(e1, meta(1))) === 'PROCESSED');
  ok('stock decremented by 3', (await stock()) === s0 - 3);
  ok('order status PROCESSED', (await status(o1)).status === 'PROCESSED');
  ok('duplicate delivery is idempotent', (await handleOrderCreated(e1, meta(1))) === 'DUPLICATE' && (await stock()) === s0 - 3);
  const o2 = await mkOrder(999999);
  ok('insufficient stock => FAILED', (await handleOrderCreated(ev(o2, 999999), meta(2))) === 'FAILED');
  ok('failed order keeps stock & records reason', (await stock()) === s0 - 3 && (await status(o2)).status === 'FAILED');
  const off = await withTenant(t.id, async (db) => (await db.query('SELECT COUNT(*)::int n FROM processed_events WHERE tenant_id=$1', [t.id])).rows[0].n);
  ok('offsets logged in processed_events', off >= 2);

  if (process.env.INVOICE_FUNCTION_URL) {
    for (let i = 0; i < 3; i++) await callInvoiceFunction(e1); // 1st = cold, rest = warm
    const logs = await withTenant(t.id, async (db) => (await db.query('SELECT cold_start FROM invoice_logs WHERE tenant_id=$1 ORDER BY created_at', [t.id])).rows);
    ok('invoice_logs written (cold then warm)', logs.length >= 3 && logs[0].cold_start === true && logs[1].cold_start === false);
  }
  console.log(`\n${pass} passed, ${fail} failed`);
  await pool.end(); process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
