// Creates the topic (needed for Upstash/Aiven where auto-create is disabled).
require('dotenv').config();
const { build, TOPIC } = require('../src/config/kafka');
(async () => {
  const admin = build('admin').admin();
  await admin.connect();
  const ok = await admin.createTopics({
    waitForLeaders: true,
    topics: [{ topic: TOPIC, numPartitions: parseInt(process.env.KAFKA_PARTITIONS || '3', 10),
               replicationFactor: parseInt(process.env.KAFKA_REPLICATION || '1', 10) }],
  });
  console.log(ok ? `Topic ${TOPIC} created` : `Topic ${TOPIC} already exists`);
  await admin.disconnect();
})().catch((e) => { console.error(e); process.exit(1); });
