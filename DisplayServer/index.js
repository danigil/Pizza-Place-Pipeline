const express = require('express');
const app = express();
const http = require('http');
const server = http.createServer(app);
const { Server } = require("socket.io");
const io = new Server(server);
const redis = require('redis');

const { Client } = require('@elastic/elasticsearch');

const { MongoClient, ServerApiVersion } = require('mongodb');
const fs = require('fs');
const path = require("path");

const sharedfilesPath = path.join(__dirname, "..", "sharedfiles");
const branchesPath = path.join(sharedfilesPath, "branches.json");
const elasticCRTPath = path.join(sharedfilesPath, "elastic.crt");

/*
~~~ Credentials ~~~
*/
const bigml_username = "enter bigml username";
const bigml_api_key = "enter bigml api key";
const mongo_uri = "enter mongo uri"
const elastic_username = "enter elastic username"
const elastic_password = "enter elastic password"

const { appendFileSync } = require('fs');
// var bigml = require('bigml');
// connection = new bigml.BigML(bigml_username,
//                              bigml_api_key,
//                              {storage: './my_storage'}
//                              )

const mongo_client = new MongoClient(mongo_uri);

var branches = JSON.parse(fs.readFileSync(branchesPath, 'utf8'));

app.use(express.static('public'))

app.get('/', (req, res) => {
  res.sendFile('views/dashboard.html', {root: __dirname })
});

app.get('/search', (req, res) => {
  res.sendFile('views/search.html', {root: __dirname })
});

// app.get('/analyze', (req, res) => {
//   res.sendFile('views/analyze.html', {root: __dirname })
// });

var elk = new Client({
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

io.on('connection', (socket) => {
    console.log('a user connected');
    socket.on('disconnect', () => {
      console.log('user disconnected');
    });

    socket.on('search', async function (query) {

      obj = query;

      const result = await elk.search({
        index: 'orders',
        "query": {
          "query_string": {
            "query": `branch_name:${obj["branch_name"]} AND date:${obj["date"]}`
          }
        }
      })
      var results = [];

      await result.hits.hits.forEach((hit)=>{

        results.push(Object.fromEntries(Object.entries(hit._source).filter((pair_arr) => {
          return ['order_number', 'branch_name', 'date', 'order_status', 'order_toppings'].includes(pair_arr[0]);
        }).map((pair_arr) => {
          if(pair_arr[0]=='order_toppings'){
            return ['order_toppings', [...new Set(pair_arr[1])]]
          }else{
            return pair_arr;
          }
        })));
      });

      socket.emit("search-result",JSON.stringify(results));
      
    });

    socket.on('analyze', async function (query) {

      directory = "./bigml_storage";
      fs.readdir(directory, (err, files) => {
        if (err) throw err;
      
        for (const file of files) {
          fs.unlink(path.join(directory, file), (err) => {
            if (err) throw err;
          });
        }
      });

      obj = query;

      await mongo_client.connect();
      const database = mongo_client.db("Cluster0");
      const collection = database.collection("bigml");
      const findCursor = await collection.find(filter={
        "date": {
          $gte:new Date(obj["from"]), 
          $lte:new Date(obj["to"]) 
        },
      });
      if ((await collection.countDocuments({"date": {$gte:new Date(obj["from"]), $lte:new Date(obj["to"])}})) === 0) {
        console.log("No documents found!");
      }
      
      var filename_csv = `toppings_${(new Date()).getHours().toString()}_${(new Date()).getMinutes().toString()}.csv`
      var filename = `toppings_${(new Date()).getHours().toString()}_${(new Date()).getMinutes().toString()}`
  
  
      //await findCursor.forEach(console.dir);
      orders_arr = await findCursor.toArray();
      var toppings_dict={
        "anchovies":false,
        "tuna":false,
        "mozzarella cheese":false,
        "bulgarian cheese":false,
        "goat cheese":false,
        "extra cheese":false,
        "parmesan cheese":false,
        "gouda cheese":false,
        "mushrooms":false,
        "olives":false,
        "onions":false,
        "peppers":false,
        "pineapple":false,
        //non-kosher
        "bacon":false,
        "chicken":false,
        "ground beef":false,
        "ham":false,
        "pepperoni":false
      };
      const entries = Object.entries(toppings_dict);
      const topping_keys = Object.keys(toppings_dict);
  
      fs.writeFileSync(filename+".csv", topping_keys.join(",")+"\n");
  
      orders_arr = await orders_arr.filter((obj) => {
          return !(obj.hasOwnProperty("branch_status"));
      }).map((order)=>{
          return [...new Set(order['order_toppings'])];
      }).map((toppings)=>{
          let toppings_copy = JSON.parse(JSON.stringify(toppings_dict)); 
          toppings.forEach(element => {
            if (element in toppings_copy){
              toppings_copy[element] = true;
            }
            
          });
          
          fs.appendFileSync(filename+".csv", Object.values(toppings_copy).join(",")+"\n");
        
          
          return toppings_copy;
      });
  
      var source = new bigml.Source(connection);
      var dataset = new bigml.Dataset(connection);
      var model = new bigml.Association(connection);
      
      source.createAndWait(filename+".csv", {name: filename}, true, function(error1, ret1) {
        if (!error1 && ret1) {
          console.log("created source");
  
          dataset.createAndWait(ret1.resource, {name: "dataset"}, true, function(error2, ret2) {
            if (!error2 && ret2) {
              console.log("created dataset");
              
              model.createAndWait(ret2.resource, (error4, ret4) => {
                if (!error4 && ret4) {
                  console.log("created model");
                }
              })
            }
          })
        }
      });
      await mongo_client.close();   
    });

    

    socket.emit("branches",JSON.stringify(branches));
  });
const client = redis.createClient();

const subscriber = client.duplicate();

subscriber.connect();

subscriber.subscribe('nrt_info', (message) => {
  io.emit('nrt_info', message);
  obj = JSON.stringify(message);
});

server.listen(3002, () => {
  console.log('listening on *:3002');
});