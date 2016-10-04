var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});

function distinct_origin_ids() {
  return new Promise(function(resolve, reject) {
    console.log("LLLLL")
    client.search({
      index: 'mobilities',
      type: 'booking',
      body: {
        "aggs": {
          "origin_ids": {
            "terms": {
              "field": "origin_id",
              "size": 100000
            }
          }
        }
      }
    }).then(function (resp) {
      console.log("FFFFF")
      resolve(resp.aggregations.origin_ids);
    }, function (err) {
        console.trace(err.message);
    });
  });
}

function aggregate_by_destination_and_date(origin_id) {
  return new Promise(function(resolve, reject) {
    MongoClient.connect(url, function(err, db) {
      if (err) {
        console.log(err);
      }
      var col = db.collection('bookings');
      col.aggregate(
        [
          {$match: {origin_id: origin_id}},
          {$group:{_id:{"dest_id":"$dest_id", "date":"$date"}, pax:{"$sum":1}}}
        ], function(err, results) {
        if (err) {
          console.log(err);
        }
        console.log(results);
        resolve(results);
    }
      );
    });
  });
}

exports.aggregate_admin_to_admin_date = function() {
  return new Promise(function(resolve, reject) {
    distinct_origin_ids().then(function(origin_admins) {
      console.log(origin_admins);
      aggregate_by_destination_and_date(origin_admins[0]).then(function(agg) {
        console.log(agg);
      });
    });
  });
};
