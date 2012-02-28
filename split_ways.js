var ways = db.osm_ways.find({'keysvals.highway':{$exists:true}});

ways.forEach(function(way){
  var seg = {id:way.id, keysvals:way.keysvals}
  var cut = 0;
  for(var i=1; i<way.refs.length-1; i++){
    var split = db.nodes_count.findOne({'value.nodeid':way.refs[i]}).value.count > 1;
    if( split ) {
      print( "split "+cut+" to "+(i+1) );
      print( way.refs.slice(cut,i+1) );
      cut = i;
    }
  }
  print( "end split "+cut+" to "+(way.refs.length) );
  print( way.refs.slice( cut, way.refs.length ) );
});
