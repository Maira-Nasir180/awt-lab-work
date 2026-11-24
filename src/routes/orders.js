const router = require('express').Router();
const crypto = require('crypto');
const { withTenant } = require('../config/db');
const { publishOrderCreated } = require('../lib/producer');
const { TOPIC } = require('../config/kafka');

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const limitOf = (q) => Math.min(Math.max(parseInt(q.limit || '50', 10) || 50, 1), 200);

/* ------------------------------------------------------------------ *
 * POST /api/v1/orders  -> 202 Accepted + Kafka event
 * ------------------------------------------------------------------ */
router.post('/', async (req, res, next) => {
  try {
    const { customerName, items } = req.body || {};
    if (!customerName || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: 'customerName and a non-empty items[] are required' });
    }
    for (const it of items) {
      if (!it || !UUID_RE.test(String(it.productId)) || !Number.isInteger(it.quantity) || it.quantity < 1) {
        return res.status(400).json({ error: 'each item needs productId (uuid) and integer quantity >= 1' });
      }
    }
    const tenantId = req.tenant.id;

    // 1) persist order as PENDING (stock is NOT touched here - the consumer does that)
    const order = await withTenant(tenantId, async (db) => {
      const ids = [...new Set(items.map((i) => i.productId))];
      const { rows: products } = await db.query(
        'SELECT id, price FROM products WHERE tenant_id = $1 AND id = ANY($2::uuid[])', [tenantId, ids]);
      const priceById = new Map(products.map((p) => [p.id, Number(p.price)]));
      if (priceById.size !== ids.length) {
        const err = new Error('One or more products do not exist for this tenant'); err.status = 422; throw err;
      }
      const total = items.reduce((s, i) => s + priceById.get(i.productId) * i.quantity, 0);
      const { rows: [o] } = await db.query(
        `INSERT INTO orders (tenant_id, customer_name, status, total) VALUES ($1,$2,'PENDING',$3)
         RETURNING id, status, total, created_at`, [tenantId, customerName, total.toFixed(2)]);
      await db.query(
        `INSERT INTO order_items (tenant_id, order_id, product_id, quantity, unit_price)
         SELECT $1, $2, x.pid, x.qty, x.price
         FROM unnest($3::uuid[], $4::int[], $5::numeric[]) AS x(pid, qty, price)`,
        [tenantId, o.id, items.map((i) => i.productId), items.map((i) => i.quantity),
         items.map((i) => priceById.get(i.productId))]);
      return { ...o, items: items.map((i) => ({ ...i, unitPrice: priceById.get(i.productId) })) };
    });

    // 2) publish OrderCreated event (key = tenant_id)
    const event = {
      eventId: crypto.randomUUID(),
      type: 'OrderCreated',
      tenantId,
      tenantSlug: req.tenant.slug,
      orderId: order.id,
      customerName,
      total: Number(order.total),
      items: order.items,
      occurredAt: new Date().toISOString(),
    };
    let meta;
    try {
      meta = await publishOrderCreated(event);
    } catch (kerr) {
      await withTenant(tenantId, (db) => db.query(
        "UPDATE orders SET status='PUBLISH_FAILED', failure_reason=$3 WHERE tenant_id=$1 AND id=$2",
        [tenantId, order.id, String(kerr.message).slice(0, 300)]));
      console.error('[orders] kafka publish failed', kerr.message);
      return res.status(503).json({ error: 'Order saved but event bus unavailable', orderId: order.id, status: 'PUBLISH_FAILED' });
    }

    res.status(202).json({
      message: 'Order accepted for asynchronous processing',
      orderId: order.id,
      status: 'PENDING',
      eventId: event.eventId,
      kafka: { topic: TOPIC, key: tenantId, partition: meta.partition ?? null, offset: meta.baseOffset ?? null },
    });
  } catch (e) { next(e); }
});

/* ------------------------------------------------------------------ *
 * GET /api/v1/orders/slow  -> N+1 problem (1 query + N queries)
 * ------------------------------------------------------------------ */
router.get('/slow', async (req, res, next) => {
  try {
    const t0 = process.hrtime.bigint();
    const tenantId = req.tenant.id;
    let queryCount = 0;
    const data = await withTenant(tenantId, async (db) => {
      const { rows: orders } = await db.query(
        'SELECT id, customer_name, status, total, created_at FROM orders WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT $2',
        [tenantId, limitOf(req.query)]);
      queryCount++;
      for (const o of orders) {               // <-- N additional queries (one per order)
        const { rows } = await db.query(
          `SELECT oi.product_id, p.name, oi.quantity, oi.unit_price
           FROM order_items oi JOIN products p ON p.id = oi.product_id
           WHERE oi.tenant_id=$1 AND oi.order_id=$2`, [tenantId, o.id]);
        queryCount++;
        o.items = rows;
      }
      return orders;
    });
    res.json({ strategy: 'N+1 (slow)', queryCount, durationMs: Number(process.hrtime.bigint() - t0) / 1e6, count: data.length, data });
  } catch (e) { next(e); }
});

/* ------------------------------------------------------------------ *
 * GET /api/v1/orders  -> optimized: ONE query (LEFT JOIN + json_agg)
 * ------------------------------------------------------------------ */
router.get('/', async (req, res, next) => {
  try {
    const t0 = process.hrtime.bigint();
    const tenantId = req.tenant.id;
    const data = await withTenant(tenantId, async (db) =>
      (await db.query(
        `SELECT o.id, o.customer_name, o.status, o.total, o.created_at,
                COALESCE(json_agg(json_build_object('product_id', oi.product_id, 'name', p.name,
                         'quantity', oi.quantity, 'unit_price', oi.unit_price))
                         FILTER (WHERE oi.id IS NOT NULL), '[]') AS items
         FROM (SELECT * FROM orders WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2) o
         LEFT JOIN order_items oi ON oi.order_id = o.id AND oi.tenant_id = o.tenant_id
         LEFT JOIN products p ON p.id = oi.product_id
         GROUP BY o.id, o.customer_name, o.status, o.total, o.created_at
         ORDER BY o.created_at DESC`, [tenantId, limitOf(req.query)])).rows);
    res.json({ strategy: 'JOIN + json_agg (optimized)', queryCount: 1, durationMs: Number(process.hrtime.bigint() - t0) / 1e6, count: data.length, data });
  } catch (e) { next(e); }
});

router.get('/:id', async (req, res, next) => {
  try {
    if (!UUID_RE.test(req.params.id)) return res.status(400).json({ error: 'invalid order id' });
    const order = await withTenant(req.tenant.id, async (db) =>
      (await db.query(
        `SELECT o.id, o.customer_name, o.status, o.total, o.failure_reason, o.created_at,
                COALESCE(json_agg(json_build_object('product_id', oi.product_id, 'name', p.name,
                         'quantity', oi.quantity, 'unit_price', oi.unit_price))
                         FILTER (WHERE oi.id IS NOT NULL), '[]') AS items
         FROM orders o
         LEFT JOIN order_items oi ON oi.order_id = o.id AND oi.tenant_id = o.tenant_id
         LEFT JOIN products p ON p.id = oi.product_id
         WHERE o.tenant_id = $1 AND o.id = $2
         GROUP BY o.id`, [req.tenant.id, req.params.id])).rows[0]);
    if (!order) return res.status(404).json({ error: 'Order not found' }); // same answer for "other tenant's order"
    res.json(order);
  } catch (e) { next(e); }
});

module.exports = router;
