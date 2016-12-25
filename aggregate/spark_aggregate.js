var sys = require('sys')
var exec = require('child_process').exec;
var child;
// executes `pwd`
exports.aggregate = function(file, collection) {
  console.log('SPARK CLI!', file);
  return new Promise(function(resolve, reject) {
      child = exec(
        '../../spark-2.0.2-bin-hadoop2.7/bin/spark-shell -i ./spark/aggregate.scala --conf spark.driver.extraJavaOptions="-D' + file + ',' + collection + '"', function (error, stdout, stderr) {
      sys.print('stdout: ' + stdout);
      sys.print('stderr: ' + stderr);
      if (error !== null) {
        console.log('exec error: ' + error);
      }
      console.log('DONE!');
      resolve();
    });
  });
}
