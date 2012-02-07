var pbf = require("./pbf.js");

var path="/storage/maps/boston.osm.pbf";
var fileblockfile = new pbf.FileBlockFile(path);

var i=0;
fileblockfile.read(function(fb){
  if(fb.header.type==="OSMData"){
    fb.readPayload(function(fb){
      console.log( fb.header );
      if(fb.payload.primitivegroup.dense){
        fb.payload.primitivegroup.dense.nodesSync(function(node){
        });
      } else {
        console.log( fb.payload.primitivegroup.ways.length );
      }
    });
  }
});
