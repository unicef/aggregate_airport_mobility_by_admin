var ArgumentParser = require('argparse').ArgumentParser;
var bluebird = require('bluebird');
var queue = require('./lib/queue');
var config = require('./config');
var csv_columns = config.columns;
var db_fields = config.db_fields;
var azure = require('./aggregate_airport_to_admin/azure_storage');
// var aggregate = require('./aggregate_airport_to_admin/aggregate_airport_to_admin');

function aggregate_new_blobs(collection) {
  return new Promise(function(resolve, reject) {
    // Get list of blobs in pre aggregation collection
    // that do not exist in aggregated collection
    azure.get_blob_names(collection)
    .catch(function(err) {
      return reject(err);
    })
    .then(function(blobs) {
      blobs = blobs.filter(function(e) {
        return e.match(/.gz$/);
      });

      blobs.forEach(function(blob) {
        queue.queue.push(
          {
            collection: collection,
            blob: blob,
            columns: csv_columns,
            db_fields: db_fields
          }, function(err) {
          console.log(err);
        });
      });
    });
  });
}

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

  // var args = parser.parseArgs();
  // var file = args.file;
  // var kind = args.kind;

  azure.get_collection_names()
  .catch(function(err) { console.log(err);})
  .then(function(collections) {
    collections = [collections[4]];
    // Create a storage container for each collection if it doesn't already exist.
    azure.create_storage_containers(collections)
    .then(function() {
      // Iterate through collections, and aggregate new blobs to collection
      bluebird.map(collections, function(collection, index) {
        return aggregate_new_blobs(collection);
      }, {concurrency: 1})
      .catch(function(err) { console.log(err);})
      .then(function() {
        console.log('Done!');
      });
    });
  });
}

main();
