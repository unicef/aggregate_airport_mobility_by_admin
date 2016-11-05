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
  azure.dl_blob(collection, file)
  .then(function(unzipped_file) {
    // Import mobility record into elasticsearch
    es.import_to_elastic_search(
      es_index,
      collection,
      unzipped_file,
      db_fields,
      columns[collection])
    .catch(err => { console.log(err);})
    .then(function() {
      console.log('ABOUT to AGGREGATE!');
      // Aggregate records
      aggregate.aggregate_admin_to_admin_date(es_index, collection, unzipped_file)
      .catch(function(err) { console.log(err); })
      .then(function() {
        azure.upload_blob(collection, unzipped_file)
        .catch(function(err) { console.log(err);})
        .then(function() {
          util.destroy_files(unzipped_file)
          .catch(function(err) { console.log(err);})
          .then(function() {
            console.log("Done aggregating!!!", unzipped_file);
            callback();
          });
        });
      });
    });
  });
}, 1);

exports.queue = queue;
