

exports.get_admin = function(lookup, airport, admin_level) {
    // var admin = admin_level === 1 ? get_admin_1(airport) : get_admin_2(airport);
    var admin = lookup[airport];
    if (admin) {
      if (lookup[airport]) {
        var iso = lookup[airport].ISO;
        // These are admin codes.
        var id_0 = lookup[airport].ID_0;
        var id_1 = lookup[airport].ID_1;
        var id_2 = lookup[airport].ID_2;
	var pub_src = lookup[airport].pub_src;
        // Create admin id with country ISO, lowercase admin name, and admin codes.
        // Example: usa-travis-244_44-2754
        // Temp hack 
        var admin_name = admin.NAME_2 || admin.NAME_1 || admin.NAME_0 || admin.NAME_ISO || admin.NOMBRE_MPI;

        var admin_name_no_space = admin_name.replace(/(\s+|-|\.)/g, '_').toLowerCase();
        var admin_name_modified = admin_name_no_space.replace(/'/g, '_').toLowerCase();
        var admin_id = iso.toLowerCase();
        [id_0, id_1, id_2].forEach(function(e) {
          if (e) {
            admin_id = admin_id + '_' + e;
          }
        });
	admin_id = admin_id + '_' + pub_src.replace(/-/g, '_');
        //admin_id = admin_id + '_' + admin_name_modified.replace(/_+/g, '_');;
        //return [iso, admin, admin_id];
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
