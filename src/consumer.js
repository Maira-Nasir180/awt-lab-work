require('dotenv').config();
const { build, TOPIC } = require('./config/kafka');
const { pool, withTenant } = require('./config/db');

const GROUP_ID = process.env.KAFKA_GROUP_ID || 'inventory-service';
class StockError extends Error {}

async function callInvoiceFunction(event) {
  const url = process.env.INVOICE_FUNCTION_URL;
  if (!url) return;
  const t0 = process.hrtime.bigint();
  try {
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-invoice-secret': process.env.INVOICE_SECRET || '' },
      body: JSON.stringify({
        orderId: event.orderId, tenantSlug: event.tenantSlug, customerName: event.customerName,
        total: event.total, items: event.items,
      }),
    });
    const roundTrip = Number(process.hrtime.bigint() - t0) / 1e6;
    if (!r.ok) throw new Error('invoice function HTTP ' + r.status);
    const info = await r.json();
    await withTenant(event.tenantId, (db) => db.query(
      `INSERT INTO invoice_logs (tenant_id, order_id, cold_start, duration_ms, round_trip_ms) VALUES ($1,$2,$3,$4,$5)`,
      [event.tenantId, event.orderId, info.coldStart, info.durationMs, roundTrip.toFixed(2)]));
    console.log(`[invoice] order=${event.orderId} ${info.coldStart ? 'COLD' : 'WARM'} start fn=${info.durationMs}ms roundTrip=${roundTrip.toFixed(1)}ms`);
  } catch (e) {
    console.error('[invoice] failed (non-fatal):', e.message);
  }
}

async function handleOrderCreated(event, meta) {
  const { tenantId, orderId } = event;
  // Merge duplicate product lines + sort => consistent lock order => no deadlocks
  const need = new Map();
  for (const it of event.items) need.set(it.productId, (need.get(it.productId) || 0) + it.quantity);
  const sorted = [...need.entries()].sort((a, b) => a[0].localeCompare(b[0]));

  try {
    const result = await withTenant(tenantId, async (db) => {
      // idempotency: an event that was already processed is skipped (at-least-once delivery)
      const ins = await db.query(
        `INSERT INTO processed_events (event_id, tenant_id, topic, partition, "offset")
         VALUES ($1,$2,$3,$4,$5) ON CONFLICT (event_id) DO NOTHING`,
        [event.eventId, tenantId, meta.topic, meta.partition, meta.offset]);
      if (ins.rowCount === 0) return 'DUPLICATE';
      for (const [productId, qty] of sorted) {
        const r = await db.query(
          'UPDATE products SET stock = stock - $3 WHERE tenant_id=$1 AND id=$2 AND stock >= $3',
          [tenantId, productId, qty]);
        if (r.rowCount === 0) throw new StockError(`insufficient stock for product ${productId}`);
      }
      await db.query("UPDATE orders SET status='PROCESSED' WHERE tenant_id=$1 AND id=$2", [tenantId, orderId]);
      return 'PROCESSED';
    });
    return result;
  } catch (e) {
    if (!(e instanceof StockError)) throw e; // real errors => KafkaJS retries the message
    await withTenant(tenantId, async (db) => {
      await db.query(
        `INSERT INTO processed_events (event_id, tenant_id, topic, partition, "offset")
         VALUES ($1,$2,$3,$4,$5) ON CONFLICT (event_id) DO NOTHING`,
        [event.eventId, tenantId, meta.topic, meta.partition, meta.offset]);
      await db.query("UPDATE orders SET status='FAILED', failure_reason=$3 WHERE tenant_id=$1 AND id=$2",
        [tenantId, orderId, e.message]);
    });
    return 'FAILED';
  }
}

async function main() {
  const consumer = build('inventory-consumer').consumer({ groupId: GROUP_ID });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  consumer.on(consumer.events.GROUP_JOIN, (e) =>
    console.log('[consumer] joined group', GROUP_ID, 'assignment:', JSON.stringify(e.payload.memberAssignment)));

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const offset = message.offset;
      let event;
      try { event = JSON.parse(message.value.toString()); }
      catch (_) { console.error(`[consumer] bad JSON at ${topic}[${partition}]@${offset} - skipped`); return; }
      if (event.type !== 'OrderCreated') return;
      if (message.key && message.key.toString() !== event.tenantId) {
        console.error(`[consumer] key/tenant mismatch at ${partition}@${offset} - skipped`); return;
      }
      const outcome = await handleOrderCreated(event, { topic, partition, offset });
      console.log(`[offset] topic=${topic} partition=${partition} offset=${offset} key=${message.key} order=${event.orderId} -> ${outcome}`);
      if (outcome === 'PROCESSED') await callInvoiceFunction(event);
      // KafkaJS commits the offset automatically after this function resolves
    },
  });
  console.log(`[consumer] running. topic=${TOPIC} group=${GROUP_ID}`);

  const stop = async () => { await consumer.disconnect(); await pool.end(); process.exit(0); };
  process.on('SIGTERM', stop); process.on('SIGINT', stop);
}
module.exports = { handleOrderCreated, callInvoiceFunction };
if (require.main === module) main().catch((e) => { console.error('[consumer] fatal', e); process.exit(1); });
