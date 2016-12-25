var async = require('async');
var config = require('../config');
var aggregate = require('../aggregate/spark_aggregate');
var admin_version = require('../aggregate/create_admin_to_admin_version');
var azure = require('../lib/azure_storage');
var util = require('../lib/utility');

var queue = async.queue(function(obj, callback) {
  var collection = obj.collection;
  var file = obj.blob;
  var db_fields = config.db_fields[collection];
  var columns = obj.columns;
  var lookup = obj.lookup;

  async.waterfall([
    function(callback) {
      // Create temp directory with folders for zipped, transformed and processed.
      util.prepare_temp()
      .catch(function(err) {
        console.log(err);
      })
      .then(function() {
        callback(null);
      });
    },

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
      // Create a an admin to admin version
      admin_version.create_admin_to_admin_version(
        collection,
        unzipped_file,
        db_fields,
        lookup
      ).catch(err => {
        console.log(err);
      })
      .then(function() {
        console.log('Done transforming!!!', unzipped_file);
        callback(null, unzipped_file);
      });
    },

    function(unzipped_file, callback) {
      console.log('ABOUT TO AGGREGATE SPARK!');
      aggregate.aggregate(unzipped_file, collection)
      .catch(function(err) {
        console.log(err);
      })
      .then(function() {
        console.log("Done aggregating!!!!", unzipped_file);
        callback(null, unzipped_file);
      });
    },

    function(unzipped_file, callback) {
      console.log('About to COMBINE!!!');
      util.combine_spark_output(unzipped_file, db_fields)
      .catch(function(err) {
        console.log(err);
      })
      .then(function() {
        callback(null, unzipped_file);
      });
    },

    function(unzipped_file, callback) {
      console.log('About to upload!!', unzipped_file);
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

exports.queue = queue;
