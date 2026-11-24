const { build, TOPIC, DISABLED } = require('../config/kafka');

let producer = null;
let connecting = null;

async function getProducer() {
  if (producer) return producer;
  if (!connecting) {
    const p = build('order-api').producer({ allowAutoTopicCreation: true });
    connecting = p.connect()
      .then(() => { producer = p; console.log('[kafka] producer connected'); })
      .catch((e) => { connecting = null; throw e; });
  }
  await connecting;
  return producer;
}

/**
 * Publish OrderCreated. Message KEY = tenant_id => every event of a tenant lands in the
 * same partition => strict ordering per tenant (Kafka orders only inside a partition).
 */
async function publishOrderCreated(event) {
  if (DISABLED) {
    console.log('[kafka] DISABLED - skipping publish for order', event.orderId);
    return { skipped: true };
  }
  const p = await getProducer();
  const res = await p.send({
    topic: TOPIC,
    acks: -1,
    messages: [{
      key: event.tenantId,
      value: JSON.stringify(event),
      headers: { 'event-type': 'OrderCreated' },
    }],
  });
  return res[0]; // { partition, baseOffset, ... }
}

async function disconnect() { if (producer) await producer.disconnect(); }

module.exports = { publishOrderCreated, disconnect };
