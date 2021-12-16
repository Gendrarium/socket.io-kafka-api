const { Kafka } = require('kafkajs');
const {
  KAFKA_USERNAME: username,
  KAFKA_PASSWORD: password,
  KAFKA_BOOTSTRAP_SERVERS,
  KAFKA_CLIENT_ID,
} = require('./config');

const sasl = username && password ? { username, password, mechanism: 'plain' } : null;
const ssl = !!sasl;

const kafka = new Kafka({
  clientId: KAFKA_CLIENT_ID,
  brokers: [KAFKA_BOOTSTRAP_SERVERS],
  connectionTimeout: 10000,
  authenticationTimeout: 10000,
  ssl,
  sasl,
});

module.exports = kafka;
