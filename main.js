var pbf = require("./pbf.js");
var mongodb = require('mongodb');

var server = new mongodb.Db('test', new mongodb.Server("127.0.0.1", 27017, {}))
server.open(function(err, client) {
  var collection = new mongodb.Collection(client,"test_insert");
  insertStuffIntoCollection(collection);
  collection.insert({a:"record"});
});

function insertStuffIntoCollection(collection){
  var path="/storage/maps/boston.osm.pbf";
  var fileblockfile = new pbf.FileBlockFile(path);

  var i=0;
  fileblockfile.read(function(fb){
    i++;
    if(i>2)
      return;
    console.log(i);
    if(fb.header.type==="OSMData"){
      fb.readPayload(function(fb){
        console.log( fb.header );
        if(fb.payload.primitivegroup.dense){
          fb.payload.primitivegroup.dense.nodesSync(function(node){
            collection.insert(node);
            console.log(node);
          });
        } else {
          console.log( fb.payload.primitivegroup.ways.length );
        }
        
        if(i==1){
          return;
        }
      });
    }
  });
}
