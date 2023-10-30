const Kafka = require('node-rdkafka');

const path = require("path");
const sharedfilesPath = path.join(__dirname, "..", "sharedfiles");
const utilPath = path.join(sharedfilesPath, "util.js");
const elasticCRTPath = path.join(sharedfilesPath, "elastic.crt");
const confluentConfigPath = path.join(sharedfilesPath, "confluent.properties");

const { configFromPath } = require(utilPath);

const { Client } = require('@elastic/elasticsearch');
const fs = require('fs');


const elastic_username = "elastic";
const elastic_password = "xvoCIHxdzBBz_1LTbiPC";

function createConfigMap(config) {
  if (config.hasOwnProperty('security.protocol')) {
    return {
      'bootstrap.servers': config['bootstrap.servers'],
      'sasl.username': config['sasl.username'],
      'sasl.password': config['sasl.password'],
      'security.protocol': config['security.protocol'],
      'sasl.mechanisms': config['sasl.mechanisms'],
      'group.id': 'kafka-nodejs-getting-started'
    }
  } else {
    return {
      'bootstrap.servers': config['bootstrap.servers'],
      'group.id': 'kafka-nodejs-getting-started'
    }
  }
}

function createConsumer(config, onData) {
  const consumer = new Kafka.KafkaConsumer(
      createConfigMap(config),
      {'auto.offset.reset': 'earliest'});

  return new Promise((resolve, reject) => {
    consumer
     .on('ready', () => resolve(consumer))
     .on('data', onData);

    consumer.connect();
  });
};

var consumers = []

async function consumer(topic, func) {
  const config = await configFromPath(confluentConfigPath);
  const consumer = await createConsumer(config, func);

  consumers.push([topic, consumer]);

  consumer.subscribe([topic]);
  consumer.consume();

  console.log(`Connecting consumer: ${topic}`);
}

const client = new Client({
  node: 'https://localhost:9200',
  auth: {
    username: elastic_username,
    password: elastic_password
  },
  tls: {
    ca: fs.readFileSync(elasticCRTPath),
    rejectUnauthorized: false
  }
})

process.on('SIGINT', () => {
  console.log("\n")
  for (var i = 0; i < consumers.length; i++) {
    console.log(`Disconnecting consumer: ${consumers[i][0]}`);
    consumers[i][1].disconnect();
  }
  process.exit(0);
});

consumer("orders",({key,value}) => {
  order = JSON.parse(value);
  client.index({
    index: 'orders',
      document: order
    });
  
})
  .catch((err) => {
    console.error(`Something went wrong:\n${err}`);
    process.exit(1);
  });