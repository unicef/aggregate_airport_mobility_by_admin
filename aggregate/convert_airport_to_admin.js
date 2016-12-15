
exports.get_admin = function(lookup, airport, admin_level) {
  // var admin = admin_level === 1 ? get_admin_1(airport) : get_admin_2(airport);
  var admin = null;
  if (lookup[airport]) {
    admin = lookup[airport];
  }
  if (admin) {
    if (lookup[airport]) {
      var iso = admin.ISO;
      // These are admin codes.
      var id_0 = admin.ID_0;
      var id_1 = admin.ID_1;
      var id_2 = admin.ID_2;
      var pub_src = admin.pub_src;

      var admin_id = iso.toLowerCase();
      [id_0, id_1, id_2].forEach(function(e) {
        if (e) {
          admin_id = admin_id + '_' + e;
        }
      });
      //admin_id = admin_id + '_' + pub_src.replace(/-/g, '_');
      admin_id = admin_id + '_' + pub_src;
      return {iso: iso, admin_id: admin_id};
    }
  }
  return null;
};

function get_admin_1(airport) {
  if (lookup[airport] && (lookup[airport].admin_1 || lookup[airport].admin_2)) {
    if (lookup[airport].admin_1) {
      return lookup[airport].admin_1.NAME_1;
    } else {
      return lookup[airport].admin_2.NAME_2;
    }
  }
}

function get_admin_2(airport) {
  if (lookup[airport] && (lookup[airport].admin_1 || lookup[airport].admin_2)) {
    if (lookup[airport].admin_2) {
      return lookup[airport].admin_2.NAME_2;
    } else {
      return lookup[airport].admin_1.NAME_1;
    }
  }
}
