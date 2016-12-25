var _ = require('underscore');
var config = require('../config');
// Origin and destination countries/ISO's are currently retrieved from airport's admin.
exports.find_indexes_for_columns = function(headers, collection) {
  var columns = config.columns[collection];
  return Object.keys(columns).reduce(function(h, column) {
    h[column] = headers.findIndex(
      function(e) {
        return e.includes(
          _.intersection(headers, columns[column])[0]
        );
      }
    );
    return h;
  }, {});

  // var columns_index = {
  //   origin:  headers.findIndex(function(e) {return e.includes(_.intersection(headers, cols.origin)[0]);}),
  //   dest:  headers.findIndex(function(e) {return e.includes(_.intersection(headers, cols.destination)[0]);}),
  //   pax:  headers.findIndex(function(e) {return e.includes(_.intersection(headers, cols.pax)[0]);}),
  //   year:  headers.findIndex(function(e) {return e.includes(_.intersection(headers, cols.year)[0]);}),
  //   week:  headers.findIndex(function(e) {return e.includes(_.intersection(headers, cols.week)[0]);})
  // };
  // var columns_index = {
  //   origin:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.origins)[0]);}),
  //   destination:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.destinations)[0]);}),
  //   origin_country:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.origin_country)[0]);}),
  //   destination_country:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.destination_country)[0]);}),
  //   pax:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.pax)[0]);}),
  //   year:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.year)[0]);}),
  //   week:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.week)[0]);})
  // };

  return columns_index;
}
