var _ = require('underscore')
var config = require('../config');
var cols = config.columns;
// Origin and destination countries/ISO's are currently retrieved from airport's admin.
exports.find_indexes_for_columns = function(data, columns) {
  var columns_index = {
    origin:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.origins)[0]);}),
    destination:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.destinations)[0]);}),
    origin_country:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.origin_country)[0]);}),
    destination_country:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.destination_country)[0]);}),
    pax:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.pax)[0]);}),
  };

  return columns_index;
}
