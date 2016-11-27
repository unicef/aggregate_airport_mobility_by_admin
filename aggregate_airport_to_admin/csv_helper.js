var _ = require('underscore')

exports.find_indexes_for_columns = function(data, columns) {
  var cols = {
    origins: ['departure_airport', 'dep_port', 'depapt','origin_airport'],
    destinations: ['arrival_airport', 'arr_port', 'arrapt', 'destination_airport'],
    pax: ['traffic_estimation', 'paxpermonth', 'pax', 'aircraft_seats', 'seats']
  }
  var columns_index = {
    origin:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.origins)[0]);}),
    destination:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.destinations)[0]);}),
    pax:  data.findIndex(function(e) {return e.includes(_.intersection(data, cols.pax)[0]);}),
  };
console.log(columns_index);
  return columns_index;
}

