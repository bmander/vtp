/*
 * count number of times nodes are referenced
 */

/*

function mapf(){
  if( this.keysvals.highway !== undefined ){
    for(var i=0; i<this.refs.length; i++){
      emit(this.refs[i],{id:this.refs[i],loc:null,count:1,ways:[[this.id,i,this.keysvals]]});
    }
  }
}

function reducef(key, values){
  //print(key);
  
  var n=0;
  var ways=[];
  values.forEach(function(v){
    //print(v.ways);
    n += 1;
    if(v && v.ways && v.ways.length>0)
      ways = ways.concat( v.ways );
  });
  //print( ways );
  return {id:key,loc:null, count:n, ways:ways};
}

db.runCommand( { mapreduce: "city_osm_ways",
                 map:mapf,
                 reduce:reducef,
                 out:"city_nodes_count"
               });
*/

/*
function mapf(){
  emit("loc"+this.id,{id:this.id,loc:this.loc,count:0,ways:[]});
}

function reducef(key, values){
  return values[0];
}


db.runCommand( { mapreduce: "city_osm_nodes",
                 map: mapf,
                 reduce: reducef,
                 out: {merge:"city_nodes_count"} } )
*/


//merge loc and counts into one doc
function mapf(){
  emit(this.value.id,this.value);
}

function reducef(key,values){
  ret = {id:key,loc:null,count:0,ways:[]};
  for(var i=0; i<values.length;i++){
    if(values[i].loc !== null){
      ret.loc = values[i].loc;
    }
    ret.count += values[i].count;
    ret.ways = ret.ways.concat( values[i].ways );
  }
  return ret;
}

db.runCommand( {mapreduce: "city_nodes_count",
                map: mapf,
                reduce: reducef,
                out: "city_nodes_joined"} );


/*

//convert node documents into way documents
function mapf(){
  if(this.value.count==0){
    return;
  }

  for(var i=0;i<this.value.ways.length;i++){
    var nodes = [];
    nodes[this.value.ways[i][1]] = {id:this.value.id,loc:this.value.loc,count:this.value.count};
    //if(this.value.id==61356947){
    //  printjson(nodes);
    //}
    emit( this.value.ways[i][0], {nodes:nodes,keysvals:this.value.ways[i][2]} );
  }
}

function reducef(key,values){
  nodes = [];
  // for each value
  for(var i=0; i<values.length; i++){
    // there exists a component of the way
    var comp = values[i].nodes;
    // for each item in the component
    for(var j=0; j<comp.length; j++){
      // if it's null, ignore
      if(comp[j]===undefined || comp[j]===null){
        continue;
      }
      // if there's a component item, splice it into the reduced nodes list
      nodes[j]=comp[j];
    }
  }
  return {nodes:nodes,keysvals:values[0].keysvals}
}

db.runCommand( {mapreduce:"city_nodes_joined",
                map:mapf,
                reduce:reducef,
                out:"city_presliced_ways"} );
*/

/*
//split up presplit ways
function mapf(){
  if(!this.value.nodes[0]){
    print( this );
    return;
  }

  var seg = {'nodes':[this.value.nodes[0].id],'loc':[this.value.nodes[0].loc],'keysvals':this.value.keysvals,'id':this._id};
  var cut = 0;
  var cuts=0;
  for(var i=1; i<this.value.nodes.length-1; i++){
    if(!this.value.nodes[i]){
      print( this );
      return;
    }
    seg.nodes.push( this.value.nodes[i].id );
    seg.loc.push( this.value.nodes[i].loc ); 
    if(this.value.nodes[i].count>1){
      emit(this._id+"."+cuts,seg);
      seg = {'nodes':[this.value.nodes[i].id],'loc':[this.value.nodes[i].loc],'keysvals':this.value.keysvals,'id':this._id};
      cuts += 1;
      cut = i;
    }
  }

  if(!this.value.nodes[i]){
    print( this );
    return;
  }
  seg.nodes.push(this.value.nodes[i].id);
  seg.loc.push(this.value.nodes[i].loc);
  emit(this._id+"."+cuts,seg);
}

// no need to reduce
function reducef(key,values){
  return values[0];
}

db.runCommand( {mapreduce:"city_presliced_ways",
                map:mapf,
                reduce:reducef,
                out:"city_sliced_ways"} );
*/

/*
//chunk split ways into tiles
function mapf(){
  var gettilespec = function(loc,res){
    var x = Math.round(Math.floor(loc[0]/res)*res*1000)/1000;
    var y = Math.round(Math.floor(loc[1]/res)*res*1000)/1000;

    return x.toFixed(2)+":"+y.toFixed(2);
  }

  if( !this.value.loc ){
    //printjson( this );
    return;
  }

  var tiles={};
  //emit a tile for every tile that this way is in
  for(var i=0; i<this.value.loc.length; i++){
    if(!this.value.loc[i]){
      //printjson(this.value.loc);
      continue;
    }

    var tilespec = gettilespec(this.value.loc[i],0.02);
    if(tiles[tilespec]!=true){
      emit(tilespec, {ways:[{id:this._id,
                             wayid:this.value.id,
                             nodes:this.value.nodes,
                             loc:this.value.loc,
                             keysvals:this.value.keysvals}]});
      tiles[tilespec]=true;
    }
  }
}

function reducef(key,values){
  ret = []
  for(var i=0; i<values.length; i++){
    ret = ret.concat( values[i].ways );
  }
  return {ways:ret};
}

db.runCommand({mapreduce:"city_sliced_ways",
               map:mapf,
               reduce:reducef,
               out:"city_tiled_ways"})
*/

/*

//chunk split ways into tiles
function mapf(){
  var gettilespec = function(loc,res){
    var x = Math.round(Math.floor(loc[0]/res)*res*1000)/1000;
    var y = Math.round(Math.floor(loc[1]/res)*res*1000)/1000;

    return x.toFixed(2)+":"+y.toFixed(2);
  }

  var intify = function( ary, precision ){
    ret = [];
    for(var i in ary){
      if(ary[i]){
        ret.push( [Math.round(ary[i][0]*Math.pow(10,precision)),Math.round(ary[i][1]*Math.pow(10,precision))] );
      } else {
        ret[i]=ary[i];
      }
    }
    return ret;
  }

  var zip = function(ary){
    var lats=[];
    var lons=[];
    for(var i in ary){
      if(ary[i]){
        lons.push( ary[i][0] );
        lats.push( ary[i][1] );
      } else {
        lons.push(null);
        lats.push(null);
      }
    }
    return [lons,lats];
  }

  var diffcompress = function(ary){
    if(ary.length<2)
      return ary;
  
    var last=ary[0]; 
    var ret = [last];

    for(var i=1; i<ary.length; i++){
      if( last===null ){
        ret.push( ary[i] );
        last = ary[i];
        continue;
      }
      if(ary[i]===null){
        ret.push( null );
        continue;
      }
      
      ret.push( ary[i]-last );
      last = ary[i];
    }

    return ret;
  }

  var diffcompress_locs = function(ary){
    return [diffcompress(ary[0]),diffcompress(ary[1])];
  }

  if( !this.value.loc ){
    //printjson( this );
    return;
  }

  var tiles={};
  //emit a tile for every tile that this way is in
  for(var i=0; i<this.value.loc.length; i++){
    if(!this.value.loc[i]){
      //printjson(this.value.loc);
      continue;
    }

    var tilespec = gettilespec(this.value.loc[i],0.02);
    if(tiles[tilespec]!=true){
      var wayinfo={};
      wayinfo[this.value.id]=this.value.keysvals;
      emit(tilespec, {ways:[{id:this._id,
                             wayid:this.value.id,
                             fromv:this.value.nodes[0],
                             tov:this.value.nodes[this.value.nodes.length-1],
                             loc:diffcompress_locs(zip(intify(this.value.loc,6)))}],
                      wayinfo:wayinfo});
      tiles[tilespec]=true;
    }
  }
}

function reducef(key,values){
  ret = []
  wayinfo={};
  for(var i=0; i<values.length; i++){
    ret = ret.concat( values[i].ways );
    for(var wayid in values[i].wayinfo){
      wayinfo[wayid]=values[i].wayinfo[wayid];
    }
  }
  return {ways:ret,wayinfo:wayinfo};
}

db.runCommand({mapreduce:"city_sliced_ways",
               map:mapf,
               reduce:reducef,
               out:{"merge":"simple_tiles"}})
*/

