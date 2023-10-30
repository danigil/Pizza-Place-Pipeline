const Kafka = require('node-rdkafka');

var path = require('path');
const sharedfilesPath = path.join(__dirname, "..", "sharedfiles");
const confluentConfigPath = path.join(sharedfilesPath, "confluent.properties");
const utilPath = path.join(sharedfilesPath, "util.js");

const { configFromPath } = require(utilPath);

const redis = require('redis');
const REDIS_SERVER = "redis://localhost:6379";

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

var open_branches = 0;
var sum_service_time = 0;
var total_orders_today = 0;

var open_orders = {};
var open_orders_amount = 0;

var branch_service_time = {}
var toppings_amounts = {}

var region_distributions = {
  "dan": 0,
  "center": 0,
  "haifa": 0,
  "north": 0,
  "south": 0
}

var order_amount_per_hour = {}
for (const x of Array(25).keys()) {
  order_amount_per_hour[x.toString()]=0
}

var consumers = []

async function consumer(topic, func) {
  
  const config = await configFromPath(confluentConfigPath);

  const consumer = await createConsumer(config, func);

  consumers.push([topic, consumer]);

  consumer.subscribe([topic]);
  consumer.consume();

  console.log(`Connecting consumer: ${topic}`);
}

async function send_vars(){
  nrt_info = {
    "open_branches": open_branches,
    "avg_service_time": sum_service_time / total_orders_today,
    "total_orders_today": total_orders_today,
    "open_orders": open_orders_amount,
    "region_distributions": region_distributions,
    "fastest_branches": get_fastest_branches(),
    "top_toppings":get_top_toppings(),
    "order_amount_per_hour": order_amount_per_hour
  }

  publisher.publish('nrt_info', JSON.stringify(nrt_info)).then(() => {
    console.log("Published to redis nrt info")
  }).catch((err) => {
    console.log("Error with redis publishing")
  });
}

function get_top_toppings(){
  var biggest = Object.entries(toppings_amounts);
  biggest.sort((a, b) => {return b[1] - a[1]});
  const slicedArray = biggest.slice(0, 5);

  return slicedArray;
}

function get_fastest_branches(){
  var arr = Object.entries(branch_service_time);
  arr=arr.map((x) => {
    obj = x[1];

    return [x[0], obj["sum"]/obj["counter"]]
  })

  arr.sort((a, b) => {
    return a[1] - b[1]
  })

  const slicedArray = arr.slice(0, 5);

  return slicedArray;
}

const publisher = redis.createClient(REDIS_SERVER);
publisher.connect()
.then(() => {
  console.log(":) Redis connection success!")
})
.catch((error) => {
  console.log(":( Redis connection error!")
  process.exit(1)
});

consumer("orders",({key,value}) => {
  if(key)
    k = key.toString();
  else
    k = "null";

  order = JSON.parse(value);
  if (order["order_status"] == 'in_progress'){
    open_orders_amount++;
    total_orders_today++;

    region_distributions[order["branch_region"]]++;
    order_number = order["order_number"];
    order_amount_per_hour[(new Date(order["date"])).getHours()]++;

    order["order_toppings"].forEach(topping =>{
      if (!(topping in toppings_amounts)){
        toppings_amounts[topping]=1;
      }
      else{
        toppings_amounts[topping]++;
      }
    });
    
    open_orders[order_number] = order;
  }
  else if (order["order_status"] == 'done'){
    open_orders_amount--;

    order_number = order["order_number"];
    console.assert(order_number in open_orders, "got order finished message for order that didn't get in_progress message for")
    if (order_number in open_orders){
      
      let d1 = new Date(open_orders[order_number].date);
      let d2 = new Date(order.date);
      let sub = d2.getTime()-d1.getTime();
      units = sub/1000;
      sum_service_time += units;

      if(!(order["branch_name"] in branch_service_time)){
        branch_service_time[order["branch_name"]] = {"sum": 0, "counter": 0}
      }
      
      branch_service_time[order["branch_name"]]["sum"] += units;
      branch_service_time[order["branch_name"]]["counter"]++;
      
      delete open_orders[order_number];
    }
    send_vars()
  }
})
  .catch((err) => {
    console.error(`Something went wrong:\n${err}`);
    process.exit(1);
  });

consumer("open-close-events", ({key, value}) => {
  obj = JSON.parse(value);
  console.log(obj);
  console.log(`~~~${obj["branch_status"]}ing | ${obj["branch_name"]}~~~`)
  if(obj["branch_status"] == "open"){
    open_branches++;
  }
  else if(obj["branch_status"] == "close"){
    open_branches--;
  }
  send_vars();
})
  .catch((err) => {
    console.error(`Something went wrong:\n${err}`);
    process.exit(1);
  });

process.on('SIGINT', () => {
  console.log("\n")
  for (var i = 0; i < consumers.length; i++) {
    console.log(`Disconnecting consumer: ${consumers[i][0]}`);
    consumers[i][1].disconnect();
  }

  publisher.disconnect();

  process.exit(0);
});