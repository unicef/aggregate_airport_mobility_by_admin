var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});
var fs = require('fs');

function distinct_origin_ids(es_index, kind) {
  return new Promise(function(resolve, reject) {
    client.search({
      index: es_index,
      type: kind,
      body: {
        aggs: {
          origin_ids: {
            terms: {
              field: 'origin_id',
              size: 100000
            }
          }
        }
      }
    }).then(function(resp) {
      resolve(resp.aggregations.origin_ids);
    }, function(err) {
      console.trace(err.message);
      // return reject(err);
    });
  });
}
function aggregate_by_destination_and_date(es_index, kind, file_name, origin_id, counter) {
  console.log('About to agg', file_name, origin_id, counter);
  return new Promise(function(resolve, reject) {
    // Aggregate by origin -> destination -> date, and sum the pax
    client.search({
      index: es_index,
      type: kind,
      body: {
        query: {
          match: {
            origin_id: origin_id
          }
        },
        aggs: {
          dest_ids: {
            terms: {
              field: 'dest_id',
              size: 100000
            },
            //aggs: {
            //  date: {
            //    terms: {
            //      field: 'date',
            //      size: 100000
            //    },
                aggs: {
                  pax: {sum: {field: 'pax'}}
                }
            //  }
            //}
          }
        }
      }
    })
    .catch(function(err) {return reject(err);})
    .then(function(resp) {
      var buckets = resp.aggregations.dest_ids.buckets;
      buckets.forEach(function(e) {
        //e.date.buckets.forEach(function(d) {
          // console.log(counter, origin_id, e.key, d.key_as_string, d.pax.value);
          var line = [origin_id, e.key, e.key_as_string, e.pax.value].join('\t') + '\n';
          fs.appendFile('./processed/' + file_name, line, function(err) {
            if (err) {
              return reject(err);
            }
          });
        //});
      });
      resolve(resp.aggregations);
    }, function(err) {
      console.log(err);
      return reject(err);
    });
  });
}

exports.aggregate_admin_to_admin_date = function(es_index, kind, file_name) {
  return new Promise(function(resolve, reject) {
    // Get a unique list of ids of origin admins
    distinct_origin_ids(es_index, kind).then(function(origin_admins) {
      require('bluebird').map(origin_admins.buckets, function(origin_admin, i) {
        return aggregate_by_destination_and_date(es_index, kind, file_name, origin_admin.key, i);
      }, {concurrency: 1})
      .catch(function(err) {
        console.log(err);
        return reject(err);
      })
      .then(function() {
        console.log('Done aggregating', file_name);
        resolve();
      });
    });
  });
};
