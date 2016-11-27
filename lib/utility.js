var config = require('../config');
var fs = require('fs');
var zlib = require('zlib');
var local_store = config.localStorageDir;
var processed_store = config.localProcessedDir;
var exec = require('exec');

exports.unzip = function(dir, file) {
  console.log('Deep unzip', file);
  return new Promise(function(resolve, reject) {
    if (file.match(/csv$/)) {
      resolve(file);
    } else {
      var unzipped_file = file.replace(/.gz$/, '');
      console.log('About to exec');
      exec('gunzip ' + config.localStorageDir + file , function(err, out, code) {
       if (err) {
          console.log('error:', err);
          return reject(err);
        }
	console.log('Done with unzip!');
        resolve(unzipped_file);
      });
    }
  });
};
exports.destroy_files = function(file) {
  return new Promise(function(resolve, reject) {
    console.log('about to destroy files');
    destroy_file(local_store, file)
    .then(function() {
      destroy_file(local_store, file + '.gz')
      .then(function() {
        destroy_file(processed_store, file)
        .then(function() {
          console.log('Finished destroying files!')
          resolve();
        });
      });
    });
  });
};

function destroy_file(dir, file) {
  return new Promise(function(resolve, reject) {
    resolve();
    fs.stat(dir + file, function(err, stats) {
      console.log(stats); // here we got all information of file in stats variable
      if (err) {
        console.log(err);
      }
      fs.unlink(dir + file, function(err) {
        if (err) return console.log(err);
        console.log('file deleted successfully');
        resolve();
      });
    });
  });
}
