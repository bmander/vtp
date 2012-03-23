/*
 * convert GTFS in mongodb -> something close to a transit graph
 */

/*
// collect stop_times together, for to merge them with loc
function mapf(){
  emit( this.stop_id, {'loc':null, 'stop_times':[this]} );
}

function reducef(keys,vals){
  stop_times = [];
  for(var i=0; i<vals.length; i++){
    stop_times = stop_times.concat( vals[i].stop_times );
  }
  return {'loc':null, 'stop_times':stop_times}; 
}

db.runCommand( {mapreduce:"sfmta_stop_times",
                map:mapf,
                reduce:reducef,
                out:"sfmta_stop_times_collected"} );
*/

/*

// merge stop location into bart_stop_times_collected
function mapf(){
  emit( this.stop_id, {'loc':[this.stop_lon,this.stop_lat],stop_times:null} );
}

function reducef(key,vals){
  var loc=null;
  var stop_times=null;
  for(var i=0; i<vals.length; i++){
    if( vals[i].loc !== null ){
      loc=vals[i].loc;
    }
    if( vals[i].stop_times !== null ){
      stop_times=vals[i].stop_times;
    }
  }
  return {'loc':loc,'stop_times':stop_times};
}

db.runCommand( {mapreduce:"sfmta_stops",
                map: mapf,
                reduce: reducef,
                out:{'reduce':'sfmta_stop_times_collected'}} );
*/

/*
// convert bart_stop_times_collected back into a list of stop_times
function mapf(){
  for( var i=0; i<this.value.stop_times.length; i++ ){
    var stop_time = this.value.stop_times[i];
    var id = stop_time._id;
    delete stop_time['_id'];
    stop_time.loc=this.value.loc;
    emit( id, stop_time);
  }
}

function reducef(key,values){
  return values[0];
}

db.runCommand( {mapreduce:"sfmta_stop_times_collected",
                map: mapf,
                reduce:reducef,
                out:"sfmta_stop_times_loc"} );
*/


// set up a table into which we can fold trips and their stoptimes

/*
function mapf(){
  emit( this.trip_id, {"route_id":this.route_id,"service_id":this.service_id,"trip_id":this.trip_id, "trip_headsign" :this.trip_headsign, "direction_id" :this.direction_id, "block_id" :this.block_id, "shape_id" :this.shape_id, stop_times:[]} );
}

function reducef(key,values){
  return values[0];
}

db.runCommand( {mapreduce:"sfmta_trips",
                map:mapf,
                reduce:reducef,
                out:"sfmta_trips_stoptimes"} );

*/

/*

// compile stop_times into trip object
function mapf(){
  ret = {route_id:null,service_id:null,trip_id:null,trip_headsign:null,direction_id:null,block_id:null,shape_id:null,stop_times:[this.value]};
  //printjson( ret );
  emit( this.value.trip_id, ret);
}

function reducef(key,values){
  //printjson(key);
  //if( key==="01DC1"){
  //  printjson( values );
 // }
  function insert_in_order( ary, item, key ){
    var i;
    for(i=0; i<ary.length; i++){
      if( key(item)<key(ary[i]) ) {
        ary.splice(i,0,item);
        break;
      }
    }
    if(i==ary.length){
      ary.push(item);
    }
  }

  function merge_lists( ary, items, key ){
    for( var i in items ){
      insert_in_order( ary, items[i], key );
    }
  }

  var retdict = {};
  var ret = [];
  for( var i in values ){
    for(var key in values[i]){
      if( key !== "stop_times" && values[i][key] !== null ){
        retdict[key] = values[i][key];
      }
    }
    merge_lists( ret, values[i].stop_times, function(x){x.stop_sequence;} );
  }
  retdict["stop_times"]=ret;
  return retdict;
}

db.runCommand( {mapreduce:"sfmta_stop_times_loc",
                map:mapf,
                reduce:reducef,
                out:{"reduce":"sfmta_trips_stoptimes"}} );

*/

// find every stop_time associated with a particular pattern and stop

/*
function mapf(){

  function secs_since_midnight(time){
    return 3600*parseInt(time.substring(0,2),10)+
           60*parseInt(time.substring(3,5),10)+
           parseInt(time.substring(6,8),10);
  }

  function get_pattern_key( stop_times ){
    var stop_ids = stop_times.map( function(x){
      return x.stop_id;
    });
    //var crossings = [];
    //for(var i=0; i<stop_times.length-1; i++){
    //  crossings.push( secs_since_midnight(stop_times[i+1].arrival_time)-secs_since_midnight(stop_times[i].departure_time) );
    //}
    //var stands = [];
    //for(var i in stop_times){
    //  stands.push( secs_since_midnight(stop_times[i].departure_time)-secs_since_midnight(stop_times[i].arrival_time) );
    //}
    //return stop_ids.join("\n")+"\n"+crossings.join("\n")+"\n"+stands.join("\n");
    return stop_ids.join("|");
  }

  var pattern_key = get_pattern_key( this.value.stop_times );

  // tag each stop_time with depart and arrive secs_since_midnight
  for( var i=0; i<this.value.stop_times.length-1; i++ ) {
    var stop_time = this.value.stop_times[i];
    var next_stop_time = this.value.stop_times[i+1];
    stop_time.arrival_time_secs = secs_since_midnight( stop_time.arrival_time );
    stop_time.departure_time_secs = secs_since_midnight( stop_time.departure_time );
    stop_time.crossing_time = secs_since_midnight( next_stop_time.arrival_time ) - secs_since_midnight( stop_time.departure_time );
  }

  
  for( var i=0; i<this.value.stop_times.length; i++) {
    var stop_time = this.value.stop_times[i];
    var next_stop_id=null;
    if(i<this.value.stop_times.length-1){
      next_stop_id = this.value.stop_times[i+1].stop_id;
      next_stop_loc = this.value.stop_times[i+1].loc;
    }
    var stub_stop_times={};
    stub_stop_times[this.value.service_id]=[stop_time];
    emit( pattern_key+"-"+stop_time.stop_id, {stop_id:stop_time.stop_id,
      stop_loc:stop_time.loc,
      next_stop_id:next_stop_id,
      next_stop_loc:next_stop_loc,
      pattern_key:pattern_key,
      stop_times:stub_stop_times} );
  }
}

function reducef(key,values){
  function insert_in_order( ary, item, key ){
    var i;
    for(i=0; i<ary.length; i++){
      //print( key(item)+" "+key(ary[i]) );
      if( key(item)<key(ary[i]) ) {
        ary.splice(i,0,item);
        return;
      }
    }
    if(i==ary.length){
      ary.push(item);
    }
  }

  function merge_lists( ary, items, key ){

    for( var i in items ){
      insert_in_order( ary, items[i], key );
    }

  }

  schedules = {};
  for( var i in values ){
    for( service_id in values[i].stop_times ) {
      if( schedules[service_id] === undefined ){
        schedules[service_id]=[];
      }

      merge_lists( schedules[service_id], values[i].stop_times[service_id], function(x){ return x.departure_time_secs} );
    }
  }

  return {stop_id:values[i].stop_id,
    stop_loc:values[i].stop_loc,
    next_stop_id:values[i].next_stop_id,
    next_stop_loc:values[i].next_stop_loc,
    pattern_key:values[i].pattern_key,
    stop_times:schedules};
}

db.runCommand( {mapreduce:"sfmta_trips_stoptimes",
                map:mapf,
                reduce:reducef,
                out:"sfmta_stop_time_bundles"} );
*/

// collect and boil down edge information

/*
function mapf(){
  var scheds={};
  for( var service_id in this.value.stop_times ) {
    var simple_departures = this.value.stop_times[service_id].map( function(stoptime){
      return {trip_id:stoptime.trip_id, crossing_time:stoptime.crossing_time, depart:stoptime.departure_time_secs};
    });
    scheds[service_id] = [{pattern_key:this.value.pattern_key,
                           departures:simple_departures}];
  }

  emit( this.value.stop_id+"\n"+this.value.next_stop_id, {stop_id:this.value.stop_id,
                                                          stop_loc:this.value.stop_loc,
                                                          next_stop_id:this.value.next_stop_id,
                                                          next_stop_loc:this.value.next_stop_loc,
                                                          schedules:scheds} );
  
}

function reducef(key, values){

  var combined_scheds={};
  for( var i=0; i<values.length; i++) {
    for(var service_id in values[i].schedules){
      if(combined_scheds[service_id]===undefined){
        combined_scheds[service_id]=values[i].schedules[service_id];
      } else {
        combined_scheds[service_id] = combined_scheds[service_id].concat( values[i].schedules[service_id] );
      }
    }
  }

  return {stop_id:values[0].stop_id,
          stop_loc:values[0].stop_loc,
          next_stop_id:values[0].next_stop_id,
          next_stop_loc:values[0].next_stop_loc,
          schedules:combined_scheds};
}

db.runCommand( {mapreduce:"sfmta_stop_time_bundles",
                map:mapf,
                reduce:reducef,
                out:"sfmta_edges"} );

*/

/*
function mapf(){
  var gettilespec = function(loc,res){
    var x = Math.round(Math.floor(loc[0]/res)*res*1000)/1000;
    var y = Math.round(Math.floor(loc[1]/res)*res*1000)/1000;

    return x.toFixed(2)+":"+y.toFixed(2);
  }

  emit( gettilespec( this.value.stop_loc, 0.02), {'links':null,'edges':[this.value]} )
}

function reducef(key,values){
  var edges = [];
  for( var i=0; i<values.length; i++ ){
    edges = edges.concat( values[i].edges );
  }
  return {'links':null,'edges':edges};
}

db.runCommand( {mapreduce:"sfmta_edges",
                map:mapf,
                reduce:reducef,
                out:"sfmta_tiles"} );
*/


function mapf(){
  function values(obj){
    ret = [];
    for(key in obj){
      ret.push( obj[key] );
    }
    return ret;
  }

  var links = {};
  for(var i=0; i<this.value.edges.length; i++){
    var edge = this.value.edges[i];
    if( links[edge.stop_id] === undefined ){
      link_node = db.city_nodes_joined.find({"value.count":{$gt:1}, "value.loc":{$near:edge.stop_loc}}).limit(1).toArray()[0];
      printjson( edge.stop_id );
      printjson( link_node.value.loc );
      links[edge.stop_id]={stop_id:edge.stop_id,
                           stop_loc:edge.stop_loc,
                           link_id:link_node.value.id,
                           link_loc:link_node.value.loc};
    }
  }

  links = values(links);
  if(links.length < 300)
    emit( this._id, {'links':values(links), 'edges':null} )
}

function reducef( key, vals ){
  var ret = {};
  for(var i=0; i<vals.length; i++){
    if(vals[i].links){
      ret.links=vals[i].links;
    }
    if(vals[i].edges){
      ret.edges=vals[i].edges;
    }
  }
  return ret;
}

db.runCommand( {mapreduce:"sfmta_tiles",
                map: mapf,
                reduce: reducef,
                out: {'reduce':'sfmta_tiles'}} );

