var airports = require('./airport_to_admin_lookup');
var lookup = airports.airport_lookup();

exports.get_admin = function(airport, admin_level) {
  var admin = admin_level === 1 ? get_admin_1(airport) : get_admin_2(airport);
  if (admin) {
    if (lookup[airport]) {
      var iso = lookup[airport].iso;
      // These are admin codes.
      var id_0 = lookup[airport].id_0;
      var id_1 = lookup[airport].id_1;
      var id_2 = lookup[airport].id_2;

      // Create admin id with country ISO, lowercase admin name, and admin codes.
      // Example: usa-travis-244_44-2754
      var admin_no_space = admin.replace(/\s+/g, '_').toLowerCase();
      admin_modified = admin_no_space.replace(/'/g, '-').toLowerCase();
      var admin_id = iso.toLowerCase() +
      '-' + admin_modified +
      '-' +
      id_0 +
      '_' +
      id_1;

      if (id_2) {
        admin_id = admin_id + '_' + id_2;
      }
      return [iso, admin, admin_id]
    }
  }
  return null;
};

function get_admin_1(airport){
  if (lookup[airport] && (lookup[airport].admin_1 || lookup[airport].admin_2)) {
    if (lookup[airport].admin_1) {
      return lookup[airport].admin_1.NAME_1;
    } else {
      return lookup[airport].admin_2.NAME_2;
    }
  }
}

function get_admin_2(airport){
  if (lookup[airport] && (lookup[airport].admin_1 || lookup[airport].admin_2)) {
    if (lookup[airport].admin_2) {
      return lookup[airport].admin_2.NAME_2;
    } else {
      return lookup[airport].admin_1.NAME_1;
    }
  }
}
