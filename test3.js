
var admin = require('./aggregate_airport_to_admin/convert_airport_to_admin');
var config = require('./config');
var Mobility = require('./models/mobility.js');
var LineByLineReader = require('line-by-line');
var csv_helper = require('./aggregate_airport_to_admin/csv_helper');
var mongoose = require('mongoose');
var u = require('underscore');
var indexes;

var db_fields = config.db_fields;
var csv_columns = config.columns.bookings;

var lr = new LineByLineReader('./data/unicef_WB_2016-07-24_to_2016-07-30.csv');
var counter = 0;

mongoose.connect(config.database, function(err) {
  var bulk = Mobility.collection.initializeUnorderedBulkOp();

  if (err) {
    throw err;
  }

  lr.on('error', function(err) {
    console.log(err);
       // 'err' contains error object
  });

  lr.on('line', function(line) {
    var data = line.split('^');
    // pause emitting of lines...
    if (counter === 0) {
      indexes = csv_helper.find_indexes_for_columns(data, csv_columns);
    } else {
      var origin = data[indexes.origin];
      var destination = data[indexes.destination];
      var date = data[indexes.date].split(/\s+/)[0];
      var pax = parseInt(data[indexes.pax], 10);

      // Get admin names for origin (an airport);
      // origin_a2 is origin admin2
      // var origin_a1 = admin.get_admin(origin, 1);
      var origin_a2 = admin.get_admin(origin, 2);
      // var destination_a1 = admin.get_admin(destination, 1);
      var destination_a2 = admin.get_admin(destination, 2);
      var row;
      if (origin_a2 && destination_a2 && pax && date) {
        row = u.flatten([
          origin_a2,
          destination_a2,
          pax,
          date
        ]);

        var json = db_fields.reduce(function(h, e, i) {
          h[e] = row[i];
          return h;
        }, {});
        // records.push(new Mobility(json));
        if (json.origin_admin && json.dest_admin) {
          bulk.insert(json);
        }
      }
    }

    if (counter % 100000 === 0 & counter > 0) {
      lr.pause();
      console.log(counter);
      bulk.execute(function(err, result) {
        if (err) {
          console.log(err);   // or do something
        }
        // possibly do something with result
        bulk = Mobility.collection.initializeOrderedBulkOp();
        setTimeout(function() {
          // ...and continue emitting lines.
          lr.resume();
        }, 200);
      });
    }
    counter += 1;
  });

  lr.on('end', function () {
       // All lines are read, file is closed now.
  });
});


// var ElasticsearchCSV = require('elasticsearch-csv');
// // create an instance of the importer with options
// var esCSV = new ElasticsearchCSV({
//     es: { index: 'my_index', type: 'my_type', host: 'localhost:9200' },
//     csv: { filePath: './prep/unicef_WB_2016-07-24_to_2016-07-302.tsv', headers: true, delimiter: '\t' }
// });
//
// esCSV.import().then(function (response) {
//         // Elasticsearch response for the bulk insert
//         console.log(response);
//     }, function (err) {
//         // throw error
//         throw err;
//     });
//
//
// var mongoose = require('mongoose'),
//     Schema = mongoose.Schema;
//
// mongoose.connect('mongodb://localhost/ccc');
//
// var sampleSchema  = new Schema({},{ "strict": false });
//
// var Sample = mongoose.model( "Sample", sampleSchema, "sample" );
//
// mongoose.connection.on("open", function(err,conn) {
//
//    var bulk = Sample.collection.initializeOrderedBulkOp();
//    var counter = 0;
//
//    // representing a long loop
//    for ( var x = 0; x < 100000; x++ ) {
//
//        bulk.find(/* some search */).upsert().updateOne(
//            /* update conditions */
//        });
//        counter++;
//
//        if ( counter % 1000 == 0 )
//            bulk.execute(function(err,result) {
//                bulk = Sample.collection.initializeOrderedBulkOp();
//            });
//    }
//
//    if ( counter % 1000 != 0 )
//        bulk.execute(function(err,result) {
//           // maybe do something with result
//        });
//
// });
