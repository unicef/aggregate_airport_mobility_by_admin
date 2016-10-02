var ArgumentParser = require('argparse').ArgumentParser;
var prepare_csv = require('./aggregate_airport_to_admin/prepare_csv_for_mongo_import');
var mongo = require('./aggregate_airport_to_admin/import_to_mongo')


var headers = [
  'origin_iso',
  'origin_admin',
  'origin_id',
  'dest_iso',
  'dest_admin',
  'dest_id',
  'pax',
  'date'
];
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

  // aggregate.aggregate_mobility_by_admin(file, kind)
  prepare_csv.create_csvs_for_import(file, headers)
  .catch(err => { console.log(err);})
  .then(function() {
    prepare_csv.prepare_csv_for_mongo_import(file)
    .catch(err => { console.log(err);})
    .then(function() {
      mongo.import_to_mongo(file, headers);
    });
  });
}
main();
