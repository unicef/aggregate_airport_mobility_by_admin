var LineInputStream = require("line-input-stream"),
  fs = require("fs"),
  mongoose = require("mongoose"),
  Schema = mongoose.Schema;
var fields;
var config = require('./config');
var fields = config.db_fields;
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

var stream = LineInputStream(fs.createReadStream("./prep/unicef_WB_2016-07-24_to_2016-07-302.tsv",{ flags: "r" }));

stream.setDelimiter("\n");
mongoose.connect('mongodb://localhost/bbbb');
mongoose.connection.on("open",function(err,conn) {

    // lower level method, needs connection
    var bulk = Entry.collection.initializeUnorderedBulkOp();
    var counter = 0;

    stream.on("error",function(err) {
        console.log(err); // or otherwise deal with it
    });

    stream.on("line",function(line) {


        var row = line.split("\t");     // split the lines on delimiter
        json = fields.reduce(function(h, e, i) {
          h[e] = row[i];
          return h;
        }, {});

        var obj = json;
        // other manipulation
        if(row.length===8){
          bulk.insert(json);  // Bulk is okay if you don't need schema
        }

                           // defaults. Or can just set them.

        counter++;

        if ( counter % 100 === 0 ) {
          console.log("hi")
            stream.pause(); //lets stop reading from file until we finish writing this batch to db

            bulk.execute(function(err,result) {
                if (err) throw err;   // or do something
                // possibly do something with result
                bulk = Entry.collection.initializeOrderedBulkOp();

                setTimeout(function(){stream.resume();}, 2000); //continue to read from file
            });
        }
    });

    stream.on("end",function() {
        if ( counter % 1000 != 0 ) {
            bulk.execute(function(err,result) {
                if (err) throw err;   // or something
                // maybe look at result
            });
        }
    });

});
