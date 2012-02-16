var pbf = require("./pbf.js");
var mongodb = require('mongodb');

var path="/storage/maps/boston.osm.pbf";
var fileblockfile = new pbf.FileBlockFile(path);
var pbffile = new pbf.PBFFile(fileblockfile);

// open connection to mongo server
var server = new mongodb.Db('test', new mongodb.Server("127.0.0.1", 27017, {}))
server.open(function(err, client) {
  var collection = new mongodb.Collection(client,"osm_nodes");

  // collect a list of nodes from the pbffile
  var i=0;
  var nodes = [];
  pbffile.nodes( function(node) {
    i+=1;
    if(i%10000==0)
      console.log(i);
    nodes.push({'id':node.id,'keyval':node.keyval,'loc':[node.lon,node.lat]});

  // when done, insert them into mongodb
  },function(){
    console.log(nodes.length);
    console.log( "nodes finished" );

    // insert 100K at a time via tail recursion
    i=0;
    var slicesize=100000;
    var foo=function(){
      if( i>nodes.length ){
        console.log("done");
        return;
      }

      collection.insert(nodes.slice(i,i+slicesize),{safe:true},function(err,obj){
        console.log( err );
        console.log("nodes entered "+i+"-"+(i+slicesize));
        i += slicesize;
        foo();
      });
    }
    foo();

  } );
});

