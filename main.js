var pbf = require("./pbf.js");
var mongodb = require('mongodb');

var path="/storage/maps/boston.osm.pbf";
var fileblockfile = new pbf.FileBlockFile(path);
var pbffile = new pbf.PBFFile(fileblockfile);

var server = new mongodb.Db('test', new mongodb.Server("127.0.0.1", 27017, {}))
server.open(function(err, client) {
  var collection = new mongodb.Collection(client,"osm_nodes");

  var i=0;
  var nodes = [];
  pbffile.nodes( function(node) {
    i+=1;
    if(i%10000==0)
      console.log(i);
    nodes.push(node);
    /*collection.insert( node, {safe:true}, function(err,objs){
      console.log(objs);
      i+=1;
      if(i%100==0){
        console.log(i);
      }
      if(err){
        throw err;
      }
    } );*/
  },function(){
    console.log(nodes.length);
    console.log( "nodes finished" );

    collection.insert(nodes,{safe:true},function(err,obj){
      console.log( err );
      console.log("nodes entered");
    });
  } );
});

/*
console.log( "fileblockfile", fileblockfile );

fileblockfile.fileblock(801,function(fileblock){
  console.log(fileblock.header);
  fileblock.readPayload(function(payload){
    payload.nodes(function(node){
      console.log(node);
    },function(){
      console.log("nodes done");
    });
    console.log("after nodes called");

    payload.ways(function(way){
      console.log(way);
    },function(){
      console.log("ways done");
    }); 
  });
});
*/

/*
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
*/
