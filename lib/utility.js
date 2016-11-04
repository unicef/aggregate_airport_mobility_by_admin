var config = require('../config');
var fs = require('fs');
var zlib = require('zlib');

exports.unzip = function(dir, file) {
  return new Promise(function(resolve, reject) {
    if (file.match(/csv$/)) {
      resolve(file);
    } else {
      var unzipped_file = file.replace(/.gz$/, '');
      var gzippedFile = fs.readFileSync(config.localStorageDir + file);
      var data = zlib.unzipSync(gzippedFile);
      fs.writeFileSync(config.localStorageDir + unzipped_file, data);
      resolve(unzipped_file);
    }
  });
};
