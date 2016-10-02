var exec = require('child_process').exec;

function import_to_mongo(file) {
  return new Promise(function(resolve, reject) {
    // executes `pwd`
    var cmd = 'mongoimport -d amadeus -c bookings --type tsv --file ./prep/' +
    file + '2.tsv' +
    '  --headerline';
    exec(cmd, function(error, stdout, stderr) {
      if (error) {
        console.log(error);
        return reject(error);
      }
      console.log('stdout: ' + stdout);
      resolve();
    });
  });
}

exports.import_to_mongo = function(file) {
  return new Promise(function(resolve, reject) {
    import_to_mongo(file).then(function() {
      resolve();
    });
  });
};
