var async = require('async');
var aggregate = require('../aggregate_airport_to_admin/aggregate');
var es = require('../aggregate_airport_to_admin/import_elasticsearch');
var azure = require('../aggregate_airport_to_admin/azure_storage');
var util = require('../lib/utility');
var config = require('../config');
var es_index = config.es_index;

var queue = async.queue(function(obj, callback) {
  var collection = obj.collection;
  var file = obj.blob;
  var db_fields = obj.db_fields;
  var columns = obj.columns;
  var lookup = obj.lookup;

  async.waterfall([
    function(callback) {
      // Download zipped mobility file from blob storage
      // traffic traffic_monthly_201605.csv.gz, for instance
      azure.dl_blob(collection, file)
      .catch(function(err) {
        console.log(err);
      })
      // Name of unzipped_file is returned
      .then(function(unzipped_file) {
        callback(null, unzipped_file);
      });
    },
    function(unzipped_file, callback) {
      // Import newly unzeipped file to elasticsearch
      es.import_to_elastic_search(
        es_index,
        collection,
        unzipped_file,
        db_fields,
        columns[collection],
        lookup
      ).catch(err => {
        console.log(err);
      })
      .then(function() {
        callback(null, unzipped_file);
      });
    },

    function(unzipped_file, callback) {
      aggregate.aggregate_admin_to_admin_date(es_index, collection, unzipped_file)
      .catch(function(err) {
        console.log(err);
      })
      .then(function() {
        callback(null, unzipped_file);
      });
    },

    function(unzipped_file, callback) {
      azure.upload_blob(collection, unzipped_file)
      .catch(function(err) {
        console.log(err);
      })
      .then(function() {
        callback(null, unzipped_file);
      });
    },

    function(unzipped_file, callback) {
      azure.upload_blob(collection, unzipped_file)
      .catch(function(err) {
        console.log(err);
      })
      .then(function() {
        callback(null, unzipped_file);
      });
    },

    function(unzipped_file, callback) {
      util.destroy_files(unzipped_file)
      .catch(function(err) {
        console.log(err);
      })
      .then(function() {
        callback(null, unzipped_file);
      });
    }

  ], function(unzipped_file) {
    console.log("Done aggregating!!!", unzipped_file);
    callback();
  });
}, 1);
  // azure.dl_blob(collection, file)
  // .then(function(unzipped_file) {
  //   // Import mobility records into elasticsearch
  //   es.import_to_elastic_search(
  //     es_index,
  //     collection,
  //     unzipped_file,
  //     db_fields,
  //     columns[collection])
  //   .catch(err => { console.log(err);})
  //   .then(function() {
  //     console.log('ABOUT to AGGREGATE!');
  //     // Aggregate records
  //     aggregate.aggregate_admin_to_admin_date(es_index, collection, unzipped_file)
  //     .catch(function(err) { console.log(err); })
  //     .then(function() {
  //       azure.upload_blob(collection, unzipped_file)
  //       .catch(function(err) { console.log(err);})
  //       .then(function() {
  //         util.destroy_files(unzipped_file)
  //         .catch(function(err) { console.log(err);})
  //         .then(function() {
  //           console.log("Done aggregating!!!", unzipped_file);
  //           callback();
  //         });
  //       });
  //     });
  //   });
  // });
// }, 1);

exports.queue = queue;
