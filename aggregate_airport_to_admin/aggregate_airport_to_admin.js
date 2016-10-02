var admin = require('./convert_airport_to_admin');
var csv = require('fast-csv');
var fs = require('fs');
// var file_save = require('./save_to_file');


/**
 * Iterate through csv, each line is an airport to airport mobility
 * Match airport to admin..
 * then maintain count of admin to admin per date in [[admin]]-[[admin]]-[[date]].txt
 * @param{String} file - Name of csv file to parse
 * @param{String} kind - Kind of mobility, i.e. booking, search, schedule
 * @return{Promise} Fulfilled with result of aggregation
 */
exports.aggregate_mobility_by_admin = function(file, kind, alasql) {
  return new Promise(function(resolve, reject) {
    // Simple counter for debug
    var count = 0;
    // While iterating lines, push promise to create or update file origin|destination|date.txt
    // var promises = [];
    // A mobility record has four main attributes: origin, destination, date, number (pax);
    // Related column names vary in name and column index.
    // Declare indices for each.
    var date_index;
    var origin_index;
    var destination_index;
    var pax_index;

    // Form path to mobility file about to be parsed
    var path = './data/' + kind + '/' + file;
    var path = './data/' + file;
    var insert_statement = "INSERT INTO mobilities VALUES ";
    console.log(path, '!!!!')
    fs.createReadStream(path).pipe(csv({delimiter: '^'})).on('data', function(data) {
      // Capture the index for each attribute in the first line of the file.
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
      count++;
      // Assign date (2016-07-26) and clip hour (00:00:00)
      var date = data[date_index].split(/\s+/)[0];
      var origin = data[origin_index];
      var destination = data[destination_index];
      var pax = parseInt(data[pax_index], 10);

      // Get admin names for origin (an airport);
      // origin_a2 is origin admin2
      var origin_a2 = admin.get_admin(origin, 2);
      var origin_a1 = admin.get_admin(origin, 1);
      var destination_a2 = admin.get_admin(destination, 2);
      var destination_a1 = admin.get_admin(destination, 1);

      // promises.push(file_save.save_to_file(origin_a1, destination_a1, date, pax, kind));
      // promises.push(file_save.save_to_file(origin_a2, destination_a2, date, pax, kind));
      if( count >= 0 ){
        if(origin_a2 && destination_a2) {
          // console.log(count, '222222', origin_a2, destination_a2);
          insert_statement += "('" + origin_a2 + "','" + destination_a2 + "','" + date + "'," + pax + ", 2),";
          // alasql("INSERT INTO mobilities VALUES ('" + origin_a2 + "','" + destination_a2 + "','" + date + "'," + pax + ", 2)");

          // Debug
          if (count % 50000 === 0) {
            insert_statement = insert_statement.replace(/,$/, '');
            console.log('START')
            alasql(insert_statement);
            console.log('DONE!')
            insert_statement = "INSERT INTO mobilities VALUES ";
            console.log(count);
          }


        }
          // console.log(count, '1111', origin_a1, destination_a1);
        // if(origin_a1 && destination_a1) {
        //   alasql("INSERT INTO mobilities VALUES ('" + origin_a1 + "','" + destination_a1 + "','" + date + "'," + pax + ", 1)");
        // }

      }
    })
    .on('end', function() {
      console.log('Done reading file.');
      resolve(alasql);
      // Promise.all(promises).then(function(values) {
      //   console.log('Promises complete!');
      //   resolve();
      // });
    });
  });
};
