
exports.find_indexes_for_columns = function(data, columns) {
  var columns_index = {
    origin: data.findIndex(function(e) {return e.includes(columns[0]);}),
    destination: data.findIndex(function(e) {return e.includes(columns[1]);}),
    pax: data.findIndex(function(e) {return e.includes(columns[2]);}),
    date: data.findIndex(function(e) {return e.includes(columns[3]);})
  };
  return columns_index;
}
