require('dotenv').config();
const { Kafka, logLevel } = require('kafkajs');

const TOPIC = process.env.KAFKA_TOPIC || 'tenant.orders';
const DISABLED = process.env.KAFKA_DISABLED === 'true';

function build(clientId) {
  const cfg = {
    clientId,
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',').map((s) => s.trim()),
    logLevel: logLevel.WARN,
    ssl: process.env.KAFKA_SSL === 'true',
    retry: { retries: 5 },
  };
  if (process.env.KAFKA_SASL_MECHANISM) {
    cfg.sasl = {
      mechanism: process.env.KAFKA_SASL_MECHANISM,
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
    };
  }
  return new Kafka(cfg);
}

module.exports = { build, TOPIC, DISABLED };
