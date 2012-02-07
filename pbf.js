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

function DenseKeysVals(buf){
  this.densedata = new protobuf.DenseData(buf);
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

function StringTable(message){
  this.data = message.vals(1)
  this.getString = function(i){
    return this.data[i].toString();
  }
}

function DenseInfo(message) {
}

function DenseNodes(message){
  this.message = message;
  this.nodesSync = function(onnode){
    if(!this.message.hasField(1))
      return; 

    var ids = new protobuf.DenseData( this.message.val(1) );
    var id = ids.next(true);

    var lats = new protobuf.DenseData( this.message.val(8) );
    var lat = lats.next(true)/10000000;

    var lons = new protobuf.DenseData( this.message.val(9) );
    var lon = lons.next(true)/10000000;

    if(this.message.hasField(10)){
      var keysvals = new DenseKeysVals( this.message.val(10) );
      var keyval = keysvals.next();
    }else{
      var keysvals=null;
    }
   
    onnode({id:id,lat:lat,lon:lon,keyval:keyval});
 
    while( ids.more() ) {
      id = ids.next(true)+id;
      lat = lats.next(true)/10000000+lat;
      lon = lons.next(true)/10000000+lon;
      keyval = keysvals ? keysvals.next() : null;
      onnode({id:id,lat:lat,lon:lon,keyval:keyval});
    }
  }
}

function Way(message){
  this.message=message;
  
  this.id = message.val(1);
  this.keysvals = function(){
    var keys = new protobuf.DenseData( message.val(2) );
    var vals = new protobuf.DenseData( message.val(3) );

    ret = [];
    while(keys.more()){
      ret.push( [keys.next(), vals.next()] );
    }
    return ret;
  }
  this.refs = function(){
    ret = [];
    var denserefs = new protobuf.DenseData( message.val(8) );
    if(denserefs.more()){
      var ref = denserefs.next();
      ret.push(ref);
    }

    while(denserefs.more()){
      var ref = denserefs.next(true)+ref;
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

exports.FileBlockFile = FileBlockFile;
