var pbf = require("./pbf.js");
var mongodb = require('mongodb');

var path="/storage/maps/seattle.osm.pbf";
var fileblockfile = new pbf.FileBlockFile(path);
var pbffile = new pbf.PBFFile(fileblockfile);

function clean_keysvals(keysvals){
  // key names may not contian "." characters
  for(var key in keysvals){
    if( key.indexOf(".") != -1 ){
      var val=keysvals[key];
      delete keysvals[key];
      keysvals[key.replace(/\./g,"-")]=val;
    }
  }
}

// open connection to mongo server
var server = new mongodb.Db('test', new mongodb.Server("127.0.0.1", 27017, {}))
server.open(function(err, client) {
  var collection = new mongodb.Collection(client,"city_osm_nodes");

  // collect a list of nodes from the pbffile
  var i=0;
  var nodes = [];
  pbffile.nodes( function(node) {
    if(node.id<0){
      console.log(node);
      process.exit();
    }

    i+=1;
    if(i%10000==0)
      console.log(i);

    clean_keysvals( node.keyval );
    nodes.push({'id':node.id,'keyval':node.keyval,'loc':[node.lon,node.lat]});

  // when done, insert them into mongodb
  },function(){
    console.log(nodes.length);
    console.log( "nodes finished" );

    // insert 100K at a time via tail recursion
    i=0;
    var slicesize=50000;
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

