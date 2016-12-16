var async = require('async');
var config = require('../config');
var fs = require('fs');
var local_store = config.localZippedDir;
var processed_dir= config.localProcessedDir;
var local_dir = config.localStorageDir;
var exec = require('exec');
var bluebird = require('bluebird');
exports.combine_spark_output = function(file, db_fields) {
  console.log("COMBINING");
  return new Promise(function(resolve, reject) {
    console.log('Opening for write:', local_dir + file);
    fs.writeFile(local_dir + file, db_fields + '\n', (err) => {
      if (err) {
        console.log(err);
        return reject(err);
      }
      var path = processed_dir + file;
      fs.readdir(path, (err, files) => {
        bluebird.map(files.filter(function(e) { return e.match(/csv$/)}), function(f) {
          return append_to_file(file, f);
        }, {concurrency: 1}).catch(function(err) {
          return reject(err);
        }).then(function() {
          console.log('Done appending spark output');
          resolve();
        });
      });
    });
  });
};

function append_to_file(file, f) {
  return new Promise(function(resolve, reject) {
    fs.readFile(processed_dir + file + '/' + f, 'utf8', function(err, data) {
      if (err) throw err;
      if (f.match(/csv$/)) {
        fs.appendFile(local_dir + file, data, function(err) {
          if (err) {
            return reject(err);
          }
          resolve();
        });
      }
    });
  });
}

exports.unzip = function(dir, file) {
  console.log('Deep unzip', file);
  return new Promise(function(resolve, reject) {
    if (file.match(/csv$/)) {
      resolve(file);
    } else {
      var unzipped_file = file.replace(/.gz$/, '');
      console.log('About to exec');
      exec('gunzip ' + local_store + file, function(err, out, code) {
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

function execute_command(command) {
  return new Promise(function(resolve, reject) {
    try {
      exec(command, function(err, stdout, stderr) {
        if (err) {
          console.log(err);
        }
        resolve();
      });
    } catch (e) {
      console.log(e);
      resolve();
    }
  });
}

// Remove temp storage directory if it exists
// Then create temp dirs for unzipped, transformed, and processed
exports.prepare_temp = function() {
  return new Promise(function(resolve, reject) {
    var path = config.localStorageDir;
    var commands = [
      'rm -rf ' + path,
      'mkdir ' + config.localStorageDir,
      'mkdir ' + config.localZippedDir,
      'mkdir ' + config.localTransformedDir,
      'mkdir ' + config.localProcessedDir
    ];

    bluebird.mapSeries(commands, function(c) {
      return execute_command(c);
    }, {concurrency: 1}).then(function() {
      resolve();
    });
  });
};

exports.destroy_files = function(file) {
  return new Promise(function(resolve, reject) {
    console.log('about to destroy files');
    destroy_file(config.localZippedDir, file)
    .then(function() {
      destroy_file(localTransformedDir, file)
      .then(function() {
        destroy_file(processed_dir, file)
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
      console.log('About to unlink', dir, file);
      fs.unlink(dir + file, function(err) {
        if (err) return console.log(err);
        console.log('file deleted successfully');
        resolve();
      });
    });
  });
}
