
// Assigns an admin to a airport.
var admin = require('./convert_airport_to_admin');
var fs = require('fs');
var LineByLineReader = require('line-by-line');
var csv_helper = require('../lib/csv_helper');
var u = require('underscore');
var config = require('../config');
var separator = '\t';
var data;
function create_admin_to_admin_version(kind, file, db_fields, lookup) {
  var indexes;
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
        indexes = csv_helper.find_indexes_for_columns(data);
      } else if (counter !== 0 && data.length > 1) {
        data = line.split(separator);
        // Origin airport
        var origin = data[indexes.origin];
        // Destination airport
        var destination = data[indexes.destination];
        // Date of journey
        // var date = data[indexes.date].split(/\s+/)[0];
        var year = data[indexes.year];
        var week = data[indexes.week];
        // Number of passengers
        var pax = parseInt(data[indexes.pax], 10);

        // Get admin names for origin (an airport);
        // origin_a2 is origin admin2
        // var origin_a1 = admin.get_admin(origin, 1);
        var origin_a2 = admin.get_admin(lookup, origin, 2);
        if (!origin_a2) {
          console.log(origin);
        }
        // var destination_a1 = admin.get_admin(destination, 1);
        var destination_a2 = admin.get_admin(lookup, destination, 2);
        // var row;
        if (origin_a2 && destination_a2 && pax) {
          // row = u.flatten([
          //   origin_a2,
          //   destination_a2,
          //   pax
          // ]);

//          var json = db_fields.reduce(function(h, e, i) {
//            h[e] = row[i];
//            return h;
//          }, {});
          var json = {
            origin_id: origin_a2.admin_id,
            origin_iso: origin_a2.iso,
            dest_iso: destination_a2.iso,
            dest_id: destination_a2.admin_id,
            year: year,
            week: week,
            pax: pax
          }
          // records.push(new Mobility(json));
          //if (json.origin_admin && json.dest_admin) {
          if (json.origin_id && json.dest_id) {
            //records.push(json);
            records.push([json.origin_iso, json.origin_id, json.dest_iso, json.dest_id, json.year, json.week, pax].join(','));
          }
        }
      }

      if (counter % 50000 === 0 & counter > 0) {
        lr.pause();
        append_records_to_file(file, records)
        .then(lr.resume);
      }
      counter += 1;
    });

    lr.on('end', function() {
      if (records.length > 0) {
        append_records_to_file(file, records)
        .then(resolve);
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
exports.create_admin_to_admin_version = function(kind, file, db_fields, lookup) {
  return new Promise(function(resolve, reject) {
    var fs = require('fs');
    fs.writeFile(config.localTransformedDir  + file, db_fields + '\n', function(err) {
      if (err) {
        return console.log(err);
      }
      create_admin_to_admin_version(kind, file, db_fields, lookup)
      .then(resolve);
    });
  });
};
