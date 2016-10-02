var admin = require('./convert_airport_to_admin');
var u = require('underscore');
var csv = require('fast-csv');
var fs = require('fs');
var count = 0;

var date_index;
var origin_index;
var destination_index;
var pax_index;

exports.create_csvs_for_import = function(file, headers) {
  return new Promise(function(resolve, reject) {
    fs.writeFile('./prep/' + file + '1.tsv', headers.join('\t'), function(err) {
      if (err) {
        return reject(err);
      }
      fs.writeFile('./prep/' + file + '2.tsv', headers.join('\t'), function(err) {
        if (err) {
          return reject(err);
        }
        console.log('Prep csvs created.');
        resolve();
      });
    });
  });
};

function append_to_file(file, lines, admin_level, count) {
  return new Promise(function(resolve, reject) {
    fs.appendFile('./prep/' + file + admin_level + '.tsv', lines, err => {

      if (err) {
        console.log(err);
        return reject(err);
      }
      resolve();
    });
  });
}

exports.prepare_csv_for_mongo_import = function(file, kind) {
  console.log('Begin reading original csv:', file);
  var string_admin_1 = '';
  var string_admin_2 = '';
  var promises = [];
  return new Promise(function(resolve, reject) {
    fs.createReadStream('./data/' + file + '.csv').pipe(csv({delimiter: '^'})).on('data', function(data) {
      if (count === 0) {
        date_index = data.findIndex(function(e) {
          return e.includes('date');
        });
        origin_index = data.findIndex(function(e) {
          return e.includes('dep_port');
        });
        destination_index = data.findIndex(function(e) {
          return e.includes('arr_port');
        });
        pax_index = data.findIndex(function(e) {
          return e.includes('pax');
        });
      }

      var date = data[date_index].split(/\s+/)[0];
      var origin = data[origin_index];
      var destination = data[destination_index];
      var pax = parseInt(data[pax_index], 10);
      var line;
      // Get admin names for origin (an airport);
      // origin_a2 is origin admin2
      var origin_a1 = admin.get_admin(origin, 1);
      var origin_a2 = admin.get_admin(origin, 2);
      var destination_a1 = admin.get_admin(destination, 1);
      var destination_a2 = admin.get_admin(destination, 2);

      if (origin_a2 && destination_a2 && pax && date) {
        line = u.flatten([
          origin_a2,
          destination_a2,
          pax,
          date
        ]).join("\t") + "\n";
        string_admin_2 += line;
      }
      if (count % 100000 === 0) {

        promises.push(
          append_to_file(file, string_admin_2, 2, count)
          .catch(err => {
            console.log(err);
            return reject(err);
          })
        );
        string_admin_2 = '';
        console.log('c:', count);
      }
      count++;
    })
    .on('end', function() {
      console.log('Done reading file.');
      Promise.all(promises).then(values => {
        console.log("Prep files completed!");
        resolve();
      });
    });
  });
};
