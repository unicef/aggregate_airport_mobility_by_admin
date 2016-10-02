var fs = require('fs');
var Transform = require('stream').Transform;
var util = require('util');

exports.normalize_delimiter = function(kind, file) {
  var file = './data/' + file;
  get_line(file, function(err, line){

  });
};

function get_line(filename, callback) {
  var stream = fs.createReadStream(filename, {
    flags: 'r',
    encoding: 'utf-8',
    fd: null,
    mode: 0666,
    bufferSize: 64 * 1024
  });

  var fileData = '';
  stream.on('data', function(data){
    fileData += data;

    // The next lines should be improved
    var lines = fileData.split("\n");

    if(lines.length >= +line_no){
      stream.destroy();
      callback(null, lines[+line_no]);
    }
  });

  stream.on('error', function(){
    callback('Error', null);
  });

  stream.on('end', function(){
    callback('File end reached without finding line', null);
  });
}
