var exec = require('child_process').exec;
var csv = require('fast-csv');
var fs = require('fs');
var elasticsearch = require('es');
var es = elasticsearch();

function import_to_mongo(file) {
  console.log('About to import into Mongo!');
  return new Promise(function(resolve, reject) {
    // executes `pwd`
    var cmd = 'mongoimport --batchSize 1 --verbose -d amadeus -c bookings --type tsv --file ./prep/' +
    file + '2.tsv' +
    '  --headerline';
    exec(cmd, function(error, stdout, stderr) {
      if (error) {
        console.log(error);
        return reject(error);
      }
      console.log(stdout);
      resolve();
    });
  });
}

// function bulk_es_insert(records, index) {
//   return new Promise(function(resolve, reject) {
//     var options = {
//       _index: 'bookings',
//       _type: 'kitteh'
//     };
//
//     es.bulkIndex(options, records, function(err, data) {
//       if (err) {
//         console.log(err);
//         return reject(err);
//       }
//       process.exit();
//       resolve();
//     });
//   });
// }

// function import_to_mongo(file) {
//   console.log('About to import into Mongo!');
//   var counter = 0;
//   var fields;
//   var json;
//   var records = [];
//   var promises = [];
//   return new Promise(function(resolve, reject) {
//     fs.createReadStream('./prep/' + file + '2.tsv')
//     .pipe(csv({delimiter: '\t'}))
//     .on('data', function(data) {
//       // Get indexes for the columns we want
//       if (counter === 0) {
//         fields = data;
//       } else {
//         json = fields.reduce(function(h, e, i) {
//           h[e] = data[i];
//           return h;
//         }, {});
//         records.push(json);
//       }
//
//       if (counter > 0) {
//         console.log(counter)
//         promises.push(
//           bulk_es_insert(records)
//         );
//       }
//       counter++;
//     })
//     .on('end', function() {
//       promises.push(
//         bulk_es_insert(records)
//       );
//       Promise.all(promises).then(values => {
//         console.log("Prep files completed!");
//         resolve();
//       });
//     });
//   });
// }

var createAscendingIndex = function(db, db_fields, callback) {
  console.log('About to create Indexes.')
  return new Promise(function(resolve, reject) {
    var index_hash = db_fields.reduce(function(h, e, i) {
      h[e] = 1;
      return h;
    }, {});

    var MongoClient = require('mongodb').MongoClient;
    var url = 'mongodb://localhost:27017/amadeus';
    // Use connect method to connect to the server

    MongoClient.connect(url, function(err, db) {
      if (err) {
        return reject(err);
      }
      var col = db.collection('bookings');
      // Get the users collection
      // Create the index
      col.createIndex(index_hash, function(err, result) {
        console.log(result);
        resolve();
      });
      db.close();
    });
  });
};

exports.import_to_mongo = function(file, db_fields) {
  return new Promise(function(resolve, reject) {
    import_to_mongo(file)
    .catch(err => { console.log(err);})
    .then(function() {
      createAscendingIndex(file, db_fields)
      .then(function() {
        resolve();
      });
    });
  });
};
