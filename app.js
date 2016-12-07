// After sftp2blob fetches new mobility zip files from azure and uploads to azure blob storage
// this program does aggregations

var ArgumentParser = require('argparse').ArgumentParser;
var bluebird = require('bluebird');
var queue = require('./lib/queue');
var config = require('./config');
var csv_columns = config.columns;
var db_fields = config.db_fields;
var azure = require('./lib/azure_storage');
var async = require('async');
var airports = require('./lib/airport_to_admin_lookup');
// var aggregate = require('./aggregate_airport_to_admin/aggregate_airport_to_admin');

function aggregate_new_blobs(collection, lookup) {
  return new Promise(function(resolve, reject) {
    if (collection.match(/search/)) {
      resolve();
    };
    // Get list of blobs in pre aggregation collection
    // that do not exist in aggregated collection
    azure.get_blob_names(collection)
    .catch(reject)
    // At the moment, only process zipped files.
    .then(function(blobs) {
      blobs = blobs.filter(function(e) {
        return e.match(/.gz$/);
      });
      if (blobs.length === 0) {
        resolve();
      }

      blobs.forEach(function(blob) {
        queue.queue.push(
          {
            lookup: lookup,
            collection: collection,
            blob: blob,
            columns: csv_columns,
            db_fields: db_fields
          }, function(err) {
          console.log(err);
        });
      });
      queue.queue.drain = function() {
        console.log('all items have been processed');
        resolve();
       };
    });
  });
}

/**
 * Main function for when this module is called directly as a script.
 * Receives csv of airport mobility
 * Iterates through file, creating and updating files in designated directory,
 * with total number of bookings.
 */
function main(lookup) {
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

  async.waterfall([
    function(callback) {
      // Retrieves list of blobs in a collection: 'booking', 'search', 'schedule'..etc.
      azure.get_collection_names()
      .catch(console.log)
      // Passes collections
      .then(callback.bind(null, null));
    },
    function(collections, callback) {
      // Create a storage container for each collection on azure
      azure.create_storage_containers(collections)
      .then(function(results) {
        // Iterate through collections, and aggregate new blobs to collection
        bluebird.map(collections, function(collection, index) {
          console.log('Start collection', collection);
          return aggregate_new_blobs(collection, lookup);
        }, {concurrency: 1})
        .catch(console.log)
        .then(callback);
      });
    }
  ], function(err, result) {
    if (err) {
      console.log(err);
    }
    process.exit();
  });
}

airports.airport_lookup().catch(function(err) {
  console.log(err);
}).then(main);
