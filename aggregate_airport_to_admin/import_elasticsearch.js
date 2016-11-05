
var admin = require('./convert_airport_to_admin');
var elasticsearch = require('es');
var LineByLineReader = require('line-by-line');
var csv_helper = require('./csv_helper');
var u = require('underscore');
var config = require('./config');


function import_to_elastic_search(es, options, file, db_fields, csv_columns) {
  var indexes;
  var counter = 0;
  var records = [];

  var lr = new LineByLineReader(config.localStorageDir + file);
  return new Promise(function(resolve, reject) {
    lr.on('error', function(err) {
      console.log(err);
         // 'err' contains error object
    });

    lr.on('line', function(line) {
      var data = line.split('^');
      // pause emitting of lines...
      if (counter === 0) {
        indexes = csv_helper.find_indexes_for_columns(data, csv_columns);
      } else if (counter !== 0 && data.length > 1) {
        var origin = data[indexes.origin];
        var destination = data[indexes.destination];
        var date = data[indexes.date].split(/\s+/)[0];
        var pax = parseInt(data[indexes.pax], 10);

        // Get admin names for origin (an airport);
        // origin_a2 is origin admin2
        // var origin_a1 = admin.get_admin(origin, 1);
        var origin_a2 = admin.get_admin(origin, 2);
        if (!origin_a2) {
          console.log(origin);
        }
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
            records.push(json);
          }
        }
      }

      if (counter % 100000 === 0 & counter > 0) {
        console.log(counter);
        lr.pause();
        // console.log(counter);
        bulk_es_insert(es, options, records)
        .catch(function(err) { return reject(err);})
        .then(function() {
          setTimeout(function() {
            records = [];
            // What is this?
            es.count(function(err, data) {
              console.log(counter, data.count, '*****');
              lr.resume();
            });
          }, 1000);
        });
      }
      counter += 1;
    });

    lr.on('end', function() {
      if (records.length > 0) {
        bulk_es_insert(es, options, records)
        .catch(function(err) { return reject(err); })
        .then(function() {
          resolve();
        });
      } else {
        console.log('Done importing', file);
        resolve();
      }
    });
  });
}

exports.import_to_elastic_search = function(es_index, kind, file, db_fields, csv_columns) {
  return new Promise(function(resolve, reject) {
    var options = {
      _index: es_index,
      _type: kind
    };
    var es = elasticsearch();

    prepare_index(es, options)
    .catch(function(err) {return reject(err);})
    .then(function() {
      es = elasticsearch(options);
      import_to_elastic_search(es, options, file, db_fields, csv_columns)
      .catch(function(err) {console.log(err);})
      .then(function() {resolve();});
    });
  });
};

function prepare_index(es, options) {
  return new Promise(function(resolve, reject) {
    es.exists(options, function(err, response) {
      if (err) {
        return reject(err);
      }
      if (response.exists) {
        es.indices.deleteIndex(
          {index: 'mobilities'},
          function(err, result) {
            if (err) {
              return reject(err);
            }
            resolve();
          }
        );
      }
      resolve();
    });
  });
}

function bulk_es_insert(es, options, records, index) {
  return new Promise(function(resolve, reject) {
    es.bulkIndex(options, records, function(err, data) {
      if (err) {
        return reject(err);
      }
      resolve();
    });
  });
}
