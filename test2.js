var async = require('async');
var LineInputStream = require("line-input-stream"),
fs = require("fs"),
mongoose = require("mongoose"),
Schema = mongoose.Schema;
var fields;
var config = require('./config');
var fields = config.db_fields;
var stream = LineInputStream(fs.createReadStream("./prep/unicef_WB_2016-07-24_to_2016-07-302.tsv",{ flags: "r" }));
var entrySchema = new Schema({
  origin_iso: {type: String, index: {unique: false}},
  origin_admin: {type: String, index: {unique: false}},
  origin_id: {type: String, index: {unique: false}},
  dest_iso: {type: String, index: {unique: false}},
  dest_admin: {type: String, index: {unique: false}},
  dest_id: {type: String, index: {unique: false}},
  pax: {type: Number, index: {unique: false}},
  date: {type: Date, index: {unique: false}}
},{ strict: false });

var Entry = mongoose.model( "Zed", entrySchema );
stream.setDelimiter("\n");
mongoose.connect('mongodb://localhost/bbbb');
mongoose.connection.on("open",function(err,conn) {
  var bulk = Entry.collection.initializeUnorderedBulkOp();
    var counter = 0;
  stream.on("line",function(line) {
      async.series(
          [
              function(callback) {
                  var row = line.split("\t");     // split the lines on delimiter
                  var obj = {};
                  // other manipulation

                  var row = line.split("\t");     // split the lines on delimiter
                  json = fields.reduce(function(h, e, i) {
                    h[e] = row[i];
                    return h;
                  }, {});

                  bulk.insert(json);  // Bulk is okay if you don't need schema
                                     // defaults. Or can just set them.

                  counter++;

                  if ( counter % 1000 == 0 ) {
                    console.log(counter, '!!!')
                      bulk.execute(function(err,result) {
                          if (err) throw err;   // or do something
                          // possibly do something with result
                          bulk = Entry.collection.initializeOrderedBulkOp();
                          callback();
                      });
                  } else {
                    console.log("!!!")

                      setTimeout(function(){console.log('hi');callback()}, 500);
                  }
             }
         ],
         function (err) {
             // each iteration is done
         }
     );
  });
});
