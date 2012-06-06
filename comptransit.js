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

db.runCommand( {mapreduce:"kc_stop_times",
                map:mapf,
                reduce:reducef,
                out:"kc_stop_times_collected"} );
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

db.runCommand( {mapreduce:"kc_stops",
                map: mapf,
                reduce: reducef,
                out:{'reduce':'kc_stop_times_collected'}} );
*/

/*
// convert bart_stop_times_collected back into a list of stop_times
function mapf(){
  if( this.value.stop_times === null ){
    return; //the result of a stop with no stop_times; it happens.
  }

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

db.runCommand( {mapreduce:"kc_stop_times_collected",
                map: mapf,
                reduce:reducef,
                out:"kc_stop_times_loc"} );
*/


// set up a table into which we can fold trips and their stoptimes

/*
function mapf(){
  emit( this.trip_id, {"route_id":this.route_id,"service_id":this.service_id,"trip_id":this.trip_id, "trip_headsign" :this.trip_headsign, "direction_id" :this.direction_id, "block_id" :this.block_id, "shape_id" :this.shape_id, stop_times:[]} );
}

function reducef(key,values){
  return values[0];
}

db.runCommand( {mapreduce:"kc_trips",
                map:mapf,
                reduce:reducef,
                out:"kc_trips_stoptimes"} );
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

db.runCommand( {mapreduce:"kc_stop_times_loc",
                map:mapf,
                reduce:reducef,
                out:{"reduce":"kc_trips_stoptimes"}} );
*/

// find every stop_time associated with a particular pattern and stop

/*

function mapf(){

var Sha1 = {};  // Sha1 namespace


 //Generates SHA-1 hash of string

 //@param {String} msg                String to be hashed
 //@param {Boolean} [utf8encode=true] Encode msg as UTF-8 before generating hash
 //@returns {String}                  Hash of msg as hex character string

Sha1.hash = function(msg, utf8encode) {
  utf8encode =  (typeof utf8encode == 'undefined') ? true : utf8encode;
  
  // convert string to UTF-8, as SHA only deals with byte-streams
  if (utf8encode) msg = Utf8.encode(msg);
  
  // constants [§4.2.1]
  var K = [0x5a827999, 0x6ed9eba1, 0x8f1bbcdc, 0xca62c1d6];
  
  // PREPROCESSING 
  
  msg += String.fromCharCode(0x80);  // add trailing '1' bit (+ 0's padding) to string [§5.1.1]
  
  // convert string msg into 512-bit/16-integer blocks arrays of ints [§5.2.1]
  var l = msg.length/4 + 2;  // length (in 32-bit integers) of msg + .1. + appended length
  var N = Math.ceil(l/16);   // number of 16-integer-blocks required to hold 'l' ints
  var M = new Array(N);
  
  for (var i=0; i<N; i++) {
    M[i] = new Array(16);
    for (var j=0; j<16; j++) {  // encode 4 chars per integer, big-endian encoding
      M[i][j] = (msg.charCodeAt(i*64+j*4)<<24) | (msg.charCodeAt(i*64+j*4+1)<<16) | 
        (msg.charCodeAt(i*64+j*4+2)<<8) | (msg.charCodeAt(i*64+j*4+3));
    } // note running off the end of msg is ok 'cos bitwise ops on NaN return 0
  }
  // add length (in bits) into final pair of 32-bit integers (big-endian) [§5.1.1]
  // note: most significant word would be (len-1)*8 >>> 32, but since JS converts
  // bitwise-op args to 32 bits, we need to simulate this by arithmetic operators
  M[N-1][14] = ((msg.length-1)*8) / Math.pow(2, 32); M[N-1][14] = Math.floor(M[N-1][14])
  M[N-1][15] = ((msg.length-1)*8) & 0xffffffff;
  
  // set initial hash value [§5.3.1]
  var H0 = 0x67452301;
  var H1 = 0xefcdab89;
  var H2 = 0x98badcfe;
  var H3 = 0x10325476;
  var H4 = 0xc3d2e1f0;
  
  // HASH COMPUTATION [§6.1.2]
  
  var W = new Array(80); var a, b, c, d, e;
  for (var i=0; i<N; i++) {
  
    // 1 - prepare message schedule 'W'
    for (var t=0;  t<16; t++) W[t] = M[i][t];
    for (var t=16; t<80; t++) W[t] = Sha1.ROTL(W[t-3] ^ W[t-8] ^ W[t-14] ^ W[t-16], 1);
    
    // 2 - initialise five working variables a, b, c, d, e with previous hash value
    a = H0; b = H1; c = H2; d = H3; e = H4;
    
    // 3 - main loop
    for (var t=0; t<80; t++) {
      var s = Math.floor(t/20); // seq for blocks of 'f' functions and 'K' constants
      var T = (Sha1.ROTL(a,5) + Sha1.f(s,b,c,d) + e + K[s] + W[t]) & 0xffffffff;
      e = d;
      d = c;
      c = Sha1.ROTL(b, 30);
      b = a;
      a = T;
    }
    
    // 4 - compute the new intermediate hash value
    H0 = (H0+a) & 0xffffffff;  // note 'addition modulo 2^32'
    H1 = (H1+b) & 0xffffffff; 
    H2 = (H2+c) & 0xffffffff; 
    H3 = (H3+d) & 0xffffffff; 
    H4 = (H4+e) & 0xffffffff;
  }

  return Sha1.toHexStr(H0) + Sha1.toHexStr(H1) + 
    Sha1.toHexStr(H2) + Sha1.toHexStr(H3) + Sha1.toHexStr(H4);
}

//
// function 'f' [§4.1.1]
//
Sha1.f = function(s, x, y, z)  {
  switch (s) {
  case 0: return (x & y) ^ (~x & z);           // Ch()
  case 1: return x ^ y ^ z;                    // Parity()
  case 2: return (x & y) ^ (x & z) ^ (y & z);  // Maj()
  case 3: return x ^ y ^ z;                    // Parity()
  }
}

//
// rotate left (circular left shift) value x by n positions [§3.2.5]
//
Sha1.ROTL = function(x, n) {
  return (x<<n) | (x>>>(32-n));
}

//
// hexadecimal representation of a number 
//   (note toString(16) is implementation-dependant, and  
//   in IE returns signed numbers when used on full words)
//
Sha1.toHexStr = function(n) {
  var s="", v;
  for (var i=7; i>=0; i--) { v = (n>>>(i*4)) & 0xf; s += v.toString(16); }
  return s;
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  //
//  Utf8 class: encode / decode between multi-byte Unicode characters and UTF-8 multiple          //
//              single-byte character encoding (c) Chris Veness 2002-2010                         //
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  //

var Utf8 = {};  // Utf8 namespace

//
// Encode multi-byte Unicode string into utf-8 multiple single-byte characters 
// (BMP / basic multilingual plane only)
//
// Chars in range U+0080 - U+07FF are encoded in 2 chars, U+0800 - U+FFFF in 3 chars
//
// @param {String} strUni Unicode string to be encoded as UTF-8
// @returns {String} encoded string
//
Utf8.encode = function(strUni) {
  // use regular expressions & String.replace callback function for better efficiency 
  // than procedural approaches
  var strUtf = strUni.replace(
      /[\u0080-\u07ff]/g,  // U+0080 - U+07FF => 2 bytes 110yyyyy, 10zzzzzz
      function(c) { 
        var cc = c.charCodeAt(0);
        return String.fromCharCode(0xc0 | cc>>6, 0x80 | cc&0x3f); }
    );
  strUtf = strUtf.replace(
      /[\u0800-\uffff]/g,  // U+0800 - U+FFFF => 3 bytes 1110xxxx, 10yyyyyy, 10zzzzzz
      function(c) { 
        var cc = c.charCodeAt(0); 
        return String.fromCharCode(0xe0 | cc>>12, 0x80 | cc>>6&0x3F, 0x80 | cc&0x3f); }
    );
  return strUtf;
}

//
// Decode utf-8 encoded string back into multi-byte Unicode characters
//
// @param {String} strUtf UTF-8 string to be decoded back to Unicode
// @returns {String} decoded string
//
Utf8.decode = function(strUtf) {
  // note: decode 3-byte chars first as decoded 2-byte strings could appear to be 3-byte char!
  var strUni = strUtf.replace(
      /[\u00e0-\u00ef][\u0080-\u00bf][\u0080-\u00bf]/g,  // 3-byte chars
      function(c) {  // (note parentheses for precence)
        var cc = ((c.charCodeAt(0)&0x0f)<<12) | ((c.charCodeAt(1)&0x3f)<<6) | ( c.charCodeAt(2)&0x3f); 
        return String.fromCharCode(cc); }
    );
  strUni = strUni.replace(
      /[\u00c0-\u00df][\u0080-\u00bf]/g,                 // 2-byte chars
      function(c) {  // (note parentheses for precence)
        var cc = (c.charCodeAt(0)&0x1f)<<6 | c.charCodeAt(1)&0x3f;
        return String.fromCharCode(cc); }
    );
  return strUni;
}


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

    var pattern = stop_ids.join("|");
    
    return Sha1.hash( pattern );
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

db.runCommand( {mapreduce:"kc_trips_stoptimes",
                map:mapf,
                reduce:reducef,
                out:"kc_stop_time_bundles"} );
*/

// collect and boil down edge information

/*

function mapf(){
  var scheds={};
  for( var service_id in this.value.stop_times ) {

    var simple_departures=[];
    var last_crossing_time=null;
    var last_departure_time=null;
    for(var i=0; i<this.value.stop_times[service_id].length; i++){
      var stoptime = this.value.stop_times[service_id][i];

      var departure_time;
      if( i==0 ){
        departure_time=stoptime.departure_time_secs;
      } else {
        departure_time=stoptime.departure_time_secs-this.value.stop_times[service_id][i-1].departure_time_secs;
      }

      if( stoptime.crossing_time == last_crossing_time ){
        simple_departures.push( [stoptime.trip_id, departure_time] );
      } else {
        simple_departures.push( [stoptime.trip_id, departure_time, stoptime.crossing_time] );
      }
      last_crossing_time=stoptime.crossing_time; 
    }

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

db.runCommand( {mapreduce:"kc_stop_time_bundles",
                map:mapf,
                reduce:reducef,
                out:"kc_edges"} );

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

db.runCommand( {mapreduce:"kc_edges",
                map:mapf,
                reduce:reducef,
                out:"kc_tiles"} );
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

db.runCommand( {mapreduce:"kc_tiles",
                map: mapf,
                reduce: reducef,
                out: {'reduce':'kc_tiles'}} );

