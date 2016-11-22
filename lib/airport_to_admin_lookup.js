var fs = require('fs');
// var dir = '../updated_airports/';
// var files = fs.readdirSync(dir);
// var jsonfile = require('jsonfile');

var elasticsearch = require('es');
var options_airports = {
  _index: 'airports',
  _type: 'airport',
  refresh: true
};
var es_airport = elasticsearch(options_airports);

function replace_slash_with_hyphen(admin_info) {

  if (admin_info && admin_info.NAME_2) {
    admin_info.NAME_2 = admin_info.NAME_2.replace(/\//g, '-');
  }
  return admin_info;
}

exports.airport_lookup = function() {
  return new Promise(function(resolve, reject) {
    es_airport.search({
      query: {
        match_all: {}
      }

    }, function(err, data) {
      if (err) {
        return reject(err);
      }
      var results = data.hits.hits.reduce(function(h, a) {
      // var airport = jsonfile.readFileSync(dir + f);
        var airport = a._source;
        // var iso;
        // var id_0;
        // var id_1;
        // var id_2;

        // Presuming that only one other option possibily available per country apart form gadm
        var admin = a._source.properties.admin_info.filter(function(obj) {
          return obj.pub_src != 'gadm2-8'
        })[0];

        // Get most granular admin if more than one exists for gadm.
        if(!admin) {
          admin = a._source.properties.admin_info.sort(function(a, b) {
            return a.admin_level - b.admin_level
          })[0];
        }

        // // Airports don't necessarily have both admin 1 and 2 info
        // if (airport.properties.admin_2_info || airport.properties.admin_1_info) {
        //   if (airport.properties.admin_2_info) {
        //     iso = airport.properties.admin_2_info.ISO;
        //     // admin 0 code
        //     id_0 = airport.properties.admin_2_info.ID_0;
        //     // admin 1 code
        //     id_1 = airport.properties.admin_2_info.ID_1;
        //     // admin 2 code
        //     id_2 = airport.properties.admin_2_info.ID_2;
        //   } else {
        //     id_0 = airport.properties.admin_1_info.ID_0;
        //     id_1 = airport.properties.admin_1_info.ID_1;
        //     iso = airport.properties.admin_1_info.ISO;
        //   }
        // }

        // h[airport.properties.iata_code] = {
        //   admin_1: airport.properties.admin_1_info,
        //   admin_2: replace_slash_with_hyphen(airport.properties.admin_2_info),
        //   id_0: id_0,
        //   id_1: id_1,
        //   id_2: id_2,
        //   // Risky to take ISO from airport record and not from either admin info
        //   // Is okay now because that was the original link between airport and admin
        //   iso: iso
        // };
        h[airport.properties.iata_code] = admin;
        return h;
      }, {});
      resolve(results);
    });
  });
};
