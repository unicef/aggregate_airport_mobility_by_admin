
// Assigns an admin to a airport.
var admin = require('./convert_airport_to_admin');
var fs = require('fs');
var LineByLineReader = require('line-by-line');
var csv_helper = require('../lib/csv_helper');
var u = require('underscore');
var config = require('../config');
var separator = '\t';
var data;
function create_admin_to_admin_version(collection, file, db_fields, lookup) {
  var indexes;
  var columns = config.columns[collection];
  var counter = 0;
  var records = [];
  var lr = new LineByLineReader(config.localZippedDir + file);
  return new Promise(function(resolve, reject) {
    lr.on('error', function(err) {
      console.log(err);
         // 'err' contains error object
    });
    console.log('About to read lines!');
    lr.on('line', function(line) {
      if (counter === 0) {
        if (line.match(/\^/)) {
          separator = '^';
        }
        data = line.split(separator);
        // It's the first line...
        // Get indexes of needed columns
        // For instance:
        // { origin: 0, destination: 5, pax: 11, date: 15 }
        indexes = csv_helper.find_indexes_for_columns(data, collection);
      } else if (counter !== 0 && data.length > 1) {
        data = line.split(separator);

        var obj = Object.keys(columns).reduce(
          function(h, key) {
            if (key.match(/pax/)) {
              parseInt(data[indexes[key]], 10);
            }
            h[key] = data[indexes[key]];
            return h;
          },
        {});

        var origin_a2 = admin.get_admin(lookup, obj.origin, 2);
        if (!origin_a2) {
          console.log(obj.origin);
        }

        // var destination_a1 = admin.get_admin(destination, 1);
        var destination_a2 = admin.get_admin(lookup, obj.destination, 2);
        // var row;
        if (origin_a2 && destination_a2 && obj.pax) {
          obj.origin_id = origin_a2.admin_id;
          obj.origin_iso = origin_a2.iso;
          obj.dest_iso = destination_a2.iso;
          obj.dest_id = destination_a2.admin_id;

          if (obj.origin_id && obj.dest_id) {
            records.push(db_fields.map(function(e) {
              return obj[e];
            }).join(','));
          }
        }
      }

      if (counter % 50000 === 0 & counter > 0) {
        lr.pause();
        append_records_to_file(file, records)
        .then(function() {
          records = [];
          lr.resume();
        });
      }
      counter += 1;
    });

    lr.on('end', function() {
      if (records.length > 0) {
        append_records_to_file(file, records)
        .then(function() { resolve() });
      } else {
        console.log('Done importing', file);
        resolve();
      }
    });
  });
}

function append_records_to_file(file, records) {
console.log(file, records.slice(0,2));
  return new Promise(function(resolve, reject) {
    fs.appendFile(config.localTransformedDir + file, records.join('\n'), function(err) {
      if (err) {
        console.log(err);
      }
      resolve();
    });
  });
}
exports.create_admin_to_admin_version = function(collection, file, db_fields, lookup) {
  return new Promise(function(resolve, reject) {
    var fs = require('fs');
    fs.writeFile(config.localTransformedDir  + file, db_fields.join(',') + '\n', function(err) {
      if (err) {
        return console.log(err);
      }
      create_admin_to_admin_version(collection, file, db_fields, lookup)
      .then(function() {
        resolve();
      });
    });
  });
};
