var pbf = require("./pbf.js");
var mongodb = require('mongodb');

var path="/storage/maps/austin.osm.pbf";
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
  var collection = new mongodb.Collection(client,"cop_osm_ways");

  // collect a list of ways from the pbffile
  var i=0;
  var ways = [];
  pbffile.ways( function(way) {
    i+=1;
    if(i%10000==0)
      console.log(i);

    clean_keysvals( way.keysvals );
    ways.push(way);

  // when done, insert them into mongodb
  },function(){
    console.log(ways.length);
    console.log( "ways finished" );

    // insert 100K at a time via tail recursion
    i=0;
    var slicesize=50000;
    var foo=function(){
      if( i>ways.length ){
        console.log("done");
        return;
      }

      collection.insert(ways.slice(i,i+slicesize),{safe:true},function(err,obj){
        console.log( err );
        console.log("ways entered "+i+"-"+(i+slicesize));
        i += slicesize;
        foo();
      });
    }
    foo();

  } );
});
