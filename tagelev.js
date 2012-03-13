gf = require('./gridfloat.js'); 
var mongodb = require('mongodb');

      function undiffcompress( ary ) {
        var ret = [];

        var last = null;
        for(var i in ary){
          // scan forward for first non-null item
          if( last===null ){
            last=ary[i];
            ret.push( last );
            continue;
          }
          // only get this far if 'last' is non-null
          // if this item is null, push onto the stack and move forward
          if( ary[i] === null ){
            ret.push( null );
            continue;
          }
          
          last += ary[i];
          ret.push( last );
        }

         return ret;
      }

      function uncompressloc( loc ) {
        var lons = undiffcompress( loc[0] );
        var lats = undiffcompress( loc[1] );

        for(var i in lons){
          lons[i] = lons[i]!==null ? lons[i]/1000000 : null; 
        }
        for(var i in lats){
          lats[i] = lats[i]!==null ? lats[i]/1000000 : null;
        }

        var ret = [];
        for(var i in lons){
          ret.push( [lons[i],lats[i]] );
        }
        return ret;
      }

function risefall( ary ){
  // take ary of [x,y] and return sum rise and sum fall
  // if any y===null, returns [null,null]

  var rise=0;
  var fall=0;
  for(var i=0; i<ary.length-1; i++){
    if(ary[i][1]===null || ary[i+1][1]===null){
      return [null,null];
    }
    var diff = ary[i+1][1]-ary[i][1];
    if(diff>0){
      rise += diff;
    } else {
      fall -= diff; //subtract a negative number == add a positive number
    } 
  }

  return [rise,fall];
}

var tilewidth=0.02;

var ff = new gf.GridFloat( "data/25747857/25747857" )
console.log( ff );
console.log( ff.cell(0,0) );
console.log( ff.elevation(-122.45345, 37.738332) );

var server = new mongodb.Db('test', new mongodb.Server("127.0.0.1", 27017, {}));

server.open(function(err, client) {
  var compcoll = new mongodb.Collection(client,"simple_tiles");

  // go through every tile that touches the DEM's rectange
  var xstart = (Math.round(Math.floor(ff.left/tilewidth)*tilewidth*1000)/1000);
  var ystart = (Math.round(Math.floor(ff.bottom/tilewidth)*tilewidth*1000)/1000);
  var x = xstart;
  var y = ystart;
  while(x<ff.right){
    while(y<ff.top){
      var tilekey = x.toFixed(2)+":"+y.toFixed(2);
      console.log( tilekey );
      var cursor = compcoll.find({_id:tilekey}).limit(1);
      
      cursor.nextObject( function(err,doc){
        if(doc){
          console.log( "doc for "+doc._id+" with "+doc.value.ways.length+" ways" );
          for( var way in doc.value.ways ){
            var loc = uncompressloc( doc.value.ways[way].loc );
            //console.log( "way "+way+", "+loc.length+" points" );
            //if( doc._id==="-122.52:37.76" && way==138 ) {
            //  debugger;
            //}
            var profile = ff.profile( loc );
            if( profile===null ){
              continue;
            }
            var wayrisefall = risefall( profile );
            doc.value.ways[way].rise = wayrisefall[0];
            doc.value.ways[way].fall = wayrisefall[1];
          }
          console.log( "updating "+doc._id );
          compcoll.update({_id:doc._id},doc);
          console.log( doc._id+" done" );
        } else {
          console.log( "null tile" );
        }
      });
      y += tilewidth;
    }
    y = ystart;
    x += tilewidth;
  }
  console.log("done");
});
