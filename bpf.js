var fs = require('fs');
var zlib = require('zlib');
var protobuf = require('./protobuf.js');

function BlobHeader(message){
  this.type = message.val(1).toString();
  this.indexdata = message.val(2);
  this.datasize = message.val(3);
}

function Fileblock(fd, fileoffset){
  this.fd=fd;
  this.fileoffset=fileoffset;
  this.headersize=null;
  this.payloadsize=null;
  this.len=null;
  this.header=null;
  this.payload=null;

  var metathis=this;

  this.readHeader = function(callback){
    // read header length
    var buf = new Buffer(4);
    fs.read(fd,buf,0,4,this.fileoffset,function(err,bytesRead,buffer){

      // read the header
      metathis.headersize = buf.readUInt32BE(0);
      var headerbuf = new Buffer(metathis.headersize);
      fs.read(fd,headerbuf,0,metathis.headersize,metathis.fileoffset+4,function(err,bytesRead,buffer){

        var headerMessage = new protobuf.Message( headerbuf );
        metathis.header= new BlobHeader(headerMessage);
        metathis.payloadsize = metathis.header.datasize;
        metathis.size=4+metathis.headersize+metathis.payloadsize;
        callback(metathis);
      });
    });
  }

  this.convertPayloadMessage = function(payload){
    var messageType={"OSMHeader":HeaderBlock,
                     "OSMData":PrimitiveBlock};

    return new messageType[this.header.type](payload);
  }

  this.readPayload = function(callback){
    // read the blob payload
    var blobbuf = new Buffer(metathis.payloadsize);
    fs.read(fd,blobbuf,0,metathis.payloadsize,metathis.fileoffset+metathis.headersize+4,function(err,bytesRead,buffer){

      var packedBlobMessage = new protobuf.Message( blobbuf );

      if( packedBlobMessage.hasField(1) ) {
        metathis.payload=metathis.convertPayloadMessage(new protobuf.Message(packedBlobMessage.val(1)));
        callback( metathis );
      } else if( packedBlobMessage.hasField(3) ) {
        zlib.unzip(packedBlobMessage.val(3),function(err,buffer){
          var unpackedBlobMessage = new protobuf.Message( buffer );
          metathis.payload = metathis.convertPayloadMessage(unpackedBlobMessage);
          callback( metathis );
        });
      }
    });
  }

  this.read = function(callback){
    this.readHeader(function(fb){
      metathis.readPayload(callback);
    });
  }
}

function DenseData(buf){
  this.buf=buf;
  this.i=0;
  this.more = function(){
    return this.i<this.buf.length;
  }
  this.next = function(){
    var valdef = protobuf.readVarint(this.buf,this.i);
    this.i += valdef[1];
    return valdef[0];
  }
}

function DenseKeysVals(buf){
  this.densedata = new DenseData(buf);
  this.more = function(){
    return this.densedata.more();
  }
  this.next = function(){
    var ret = []

    while(true){
      var k=this.densedata.next();
      if(k==0)
        return ret;
      var v=this.densedata.next();
      ret.push([k,v]);
    }  
  }
}

function readRepeated(buf){
  ret = []

  var i=0;
  while(i<buf.length){
    var valdef = protobuf.readVarint(buf,i);
    var val=valdef[0];
    ret.push( val );
    i += valdef[1];
  }

  return ret;
}

function StringTable(message){
  this.data = message.vals(1)
  this.getString = function(i){
    return this.data[i].toString();
  }
}

function DenseInfo(message) {
  this.version = readRepeated( message.val(1) );
  this.timestamp = readRepeated( message.val(2) );
  this.changeset = readRepeated( message.val(3) );
  this.uid = readRepeated( message.val(4) );
  this.user_sid = readRepeated( message.val(5) );
}

function DenseNodes(message){
  this.message = message;
  this.nodesSync = function(onnode){
    if(!this.message.hasField(1))
      return; 

    var ids = new DenseData( this.message.val(1) );
    var id = protobuf.decode_signed(ids.next());

    var lats = new DenseData( this.message.val(8) );
    var lat = protobuf.decode_signed(lats.next())/10000000;

    var lons = new DenseData( this.message.val(9) );
    var lon = protobuf.decode_signed(lons.next())/10000000;

    if(this.message.hasField(10)){
      var keysvals = new DenseKeysVals( this.message.val(10) );
      var keyval = keysvals.next();
    }else{
      var keyvals=null;
    }
   
    onnode([id,lat,lon,keyval]);
 
    while( ids.more() ) {
      id = protobuf.decode_signed(ids.next())+id;
      lat = protobuf.decode_signed(lats.next())/10000000+lat;
      lon = protobuf.decode_signed(lons.next())/10000000+lon;
      keyval = keyvals ? keysvals.next() : null;
      onnode( [id,lat,lon,keyval] );
    }
  }
}

function Way(message){
  this.message=message;
  
  this.id = message.val(1);
  this.keysvals = function(){
    var keys = new DenseData( message.val(2) );
    var vals = new DenseData( message.val(3) );

    ret = [];
    while(keys.more()){
      ret.push( [keys.next(), vals.next()] );
    }
    return ret;
  }
  this.refs = function(){
    ret = [];
    var denserefs = new DenseData( message.val(8) );
    if(denserefs.more()){
      var ref = denserefs.next();
      ret.push(ref);
    }

    while(denserefs.more()){
      var ref = protobuf.decode_signed( denserefs.next() )+ref;
      ret.push( ref );
    }
    return ret;
  }
  
}

function PrimitiveGroup(message){
  this.dense=null;
  if( message.hasField(2) )
    this.dense = new DenseNodes( new protobuf.Message( message.val(2) ) );

  this.ways=[];
  if( message.hasField(3) ){
    var waymessages = message.vals(3);
    for(var i=0; i<waymessages.length; i++) {
      this.ways.push( new Way( new protobuf.Message( waymessages[i] ) ) );
    }
  }
}

function PrimitiveBlock(message){
  this.stringtable = new StringTable( new protobuf.Message( message.val(1) ) );
  this.primitivegroup = new PrimitiveGroup( new protobuf.Message( message.vals(2)[0] ) );
}

function HeaderBlock(message){
}

function FileBlockFile(path){
  this.read = function(callback){
    fs.open( path, "r", function(err,fd) {
      var stats = fs.statSync( path );

      var offset=0;
      var onblobread = function(fb){
        if(fb){
          offset += fb.size;
          callback(fb);
        }

        if(offset==stats.size)
          return;

        var fileblock = new Fileblock(fd,offset);
        fileblock.readHeader( onblobread );
      }
      onblobread(null,0);
    });
  }
}

var path="/storage/maps/boston.osm.pbf";
var fileblockfile = new FileBlockFile(path);

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
