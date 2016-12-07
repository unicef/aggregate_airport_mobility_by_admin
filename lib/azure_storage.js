var azure = require('azure-storage');
var config = require('../config');
var storage_account_from = config.azure.from.account;
var azure_key_from = config.azure.from.key;
var blobSvcFrom = azure.createBlobService(storage_account_from, azure_key_from);
var storage_account_to = config.azure.to.account;
var azure_key_to = config.azure.to.key;
var blobSvcTo = azure.createBlobService(storage_account_to, azure_key_to);
var fs = require('fs');
var utility = require('../lib/utility');
var local_dir = config.localStorageDir;

/**
 * Retrieves list of blobs in a collection:
 * 'booking', 'search', 'schedule'..etc.
 * @return{Promise} Fulfilled with array of blob names
 */

exports.get_collection_names = function(col) {
  return new Promise(function(resolve, reject) {
    blobSvcFrom.listContainersSegmented(null, function(err, result, response) {
      if (!err) {
        resolve(result.entries.map(entry => entry.name));
      } else {
        return reject(err);
      }
    });
  });
};

exports.get_blob_names = function(col) {
  return new Promise(function(resolve, reject) {
    // Fetch names in pre-aggregated container
    blobSvcFrom.listBlobsSegmented(col, null, function(err, result, response) {
      if (err) {
        return reject(err);
      } else {
        // Fetch names in aggregated container
        var names_from = result.entries.map(entry => entry.name);
        blobSvcTo.listBlobsSegmented(col, null, function(err, result, response) {
          if (err) {
            return reject(err);
          } else {
            var names_to = result.entries.map(entry => entry.name);
            var new_blobs = names_from.filter(function(e) {
              return names_to.indexOf(e.replace(/.gz$/, '')) === -1;
            });
            resolve(new_blobs);
          }
        });
      }
    });
  });
};

exports.dl_blob = function(collection, blob) {
  console.log('about to download', collection, blob);
  return new Promise(function(resolve, reject) {
    blobSvcFrom.getBlobToStream(
      collection,
      blob,
      fs.createWriteStream(config.localZippedDir + blob), function(error, result, response) {
        if (error) {
          return reject(error);
        }
        utility.unzip(collection, blob)
        .then(function(unzipped_file) {
          resolve(unzipped_file);
        });
      });
  });
};

exports.upload_blob = function(collection, file) {
  console.log('about to upload ***', collection, file);
  return new Promise(function(resolve, reject) {
    blobSvcTo.createBlockBlobFromLocalFile(
      collection,
      file,
      local_dir + file, function(error, result, response) {
        if (error) {
          return reject(error);
        }
        // utility.unzip(collection, blob)
        // .then(function(unzipped_file) {
        console.log(file, 'uploaded!!')
        resolve(file);
        // });
      });
  });
};

/**
 * Creates a storage container for each collection if doesn't already exist.
 * @param{Array} list - List of collection names.
 * @return{Promise} Fulfilled with a value suitable for use as a condition
 */
exports.create_storage_containers = function(list) {
  var promises = [];

  return new Promise(function(resolve, reject) {
    list.forEach(function(collection) {
      promises.push(
        create_storage_container(collection)
      );
    });

    Promise.all(promises).then(function(container_created_results) {
      resolve(container_created_results);
    });
  });
};

function create_storage_container(collection) {
  return new Promise(function(resolve, reject) {
    blobSvcTo.createContainerIfNotExists(collection, {
    }, function(error, result, response) {
      if (error) {
        return reject(error);
      } else {
        return resolve(result); // if result = false, container already existed.
      }
    });
  });
}
