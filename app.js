var ArgumentParser = require('argparse').ArgumentParser;
var aggregate = require('./aggregate_airport_to_admin/aggregate');
var es = require('./aggregate_airport_to_admin/import_elasticsearch')
var config = require('./config');
var csv_columns = config.columns.bookings;
var db_fields = config.db_fields;

// var aggregate = require('./aggregate_airport_to_admin/aggregate_airport_to_admin');

/**
 * Main function for when this module is called directly as a script.
 * Receives csv of airport mobility
 * Iterates through file, creating and updating files in designated directory,
 * with total number of bookings.
 */
function main() {
  var parser = new ArgumentParser({
    version: '0.0.1',
    addHelp: true,
    description: 'Aggregate a csv of airport by admin 1 and 2'
  });

  parser.addArgument(
    ['-f', '--file'],
    {help: 'Name of csv to import'}
  );

  parser.addArgument(
    ['-k', '--kind'],
    {help: 'Kind of mobility being parsed'}
  );

  var args = parser.parseArgs();
  var file = args.file;
  var kind = args.kind;

  es.import_to_elastic_search(file, db_fields, csv_columns).then(function() {
    console.log('DONE with everything!');
  }).catch(err => { console.log(err);})
  .then(function() {
  }).then(function() {
    aggregate.aggregate_admin_to_admin_date().then(function(){
      console.log("Done aggregating!!!")
      process.exit();
    })
  })
}
main();
