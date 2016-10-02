var fs = require('fs');

/**
 * Saves or updates admin to admin mobility count per date
 * in file [[admin]]-[[admin]]-[[date]].txt
 * @param{String} admin_from - Name origin admin
 * @param{String} admin_to - Name destination admin
 * @param{String} date - Date of mobility
 * @param{String} number - Number of people moved
 * @param{String} kind - Kind of mobility being parsed
 * @return{Promise} Fulfilled with result of file create or update
 */
exports.save_to_file = function(admin_from, admin_to, date, number, kind) {
  return new Promise(function(resolve, reject) {
    var file = kind + '/' + [admin_from, admin_to, date].join('^') + '.txt';
    // Check if file exists
    fs.exists(file, function(exists) {
      if (exists) {
        // If so, update file with new figure after adding current with number in file.
        add_to_number(file, number).then(function() {
          resolve();
        });
      } else {
        // File doesn't exist, create it and add number
        save_number_to_file(file, number).then(function() {
          resolve();
        });
      }
    });
  });
};

/**
 * Saves admin to admin mobility count per date
 * in file.
 * @param{String} file - [[admin]]-[[admin]]-[[date]].txt
 * @param{String} number - Number of people moved
 * @return{Promise} Fulfilled with result of file created
 */
function save_number_to_file(file, number) {
  return new Promise(function(resolve, reject) {
    fs.writeFile(file, number, function(err) {
      if (err) {
        console.log(file, err);
        return reject(err);
      }
      resolve();
    });
  });
}

/**
 * Saves admin to admin mobility count per date
 * in file.
 * @param{String} file - [[admin]]-[[admin]]-[[date]].txt
 * @param{String} number - Number of people moved
 * @return{Promise} Fulfilled with result of file create or updated
 */
function add_to_number(file, number) {
  return new Promise(function(resolve, reject) {
    fs.readFile(file, 'utf8', function(err, data) {
      if (err) {
        return reject(err);
      }
      var new_figure = parseInt(data, 10) + number;
      save_number_to_file(file, new_figure).then(function() {
        resolve();
      });
    });
  });
}
