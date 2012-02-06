var fs = require('fs');
var zlib = require('zlib');

var wiretype={'LENGTH':2,
              'VARINT':0};

function hexary(ary){
  var ret=[];
  for(var i in ary){
    ret[i]=ary[i].toString(16);
  }
  return ret;
}

function peek(ary){
  return ary[ary.length-1];
}

function more_bytes(bb){
  return (bb&0x80)==0x80;
}
function strip_msb(bb){
  return 0x7f&bb;
}
function get_wire_type(val){
  return 0x07&val;
}
function get_field_number(val){
  return val>>3;
}


function readVarint( ary, offset, callback ){
  var i=offset;
  var bytes = [strip_msb(ary[i])];
  while( more_bytes(ary[i]) && i<ary.length-1 ){
    i += 1;
    bytes.push( strip_msb(ary[i]) );
  }

  var val = 0;
  for(i=0; i<bytes.length; i++){
    val += bytes[i]<<(7*i);
  }
  return [val,i];
}

function readField(buf,offset){
  var nread=0;

  var fielddef = readVarint(buf,offset);
  var wire_type = get_wire_type(fielddef[0]);
  var field_number = get_field_number(fielddef[0]);
  nread += fielddef[1];

  var val = null;
  if(wire_type==wiretype.LENGTH){
    var strlendef = readVarint(buf,offset+nread);
    var strlen = strlendef[0];
    nread += strlendef[1]; 
    val = buf.slice(offset+nread,offset+nread+strlen);
    //val = buf.toString( 'utf8', offset+nread, strlen );
    nread += strlen;
  } else if(wire_type==wiretype.VARINT) {
    valdef = readVarint( buf, offset+nread );
    val = valdef[0];
    nread += valdef[1];
  }

  return [field_number, val, nread];
}

function readMessage(buf){
  var ret = {};
  var offset=0;
  while(offset<buf.length){
    var field = readField( buf, offset );
    var ftag=field[0].toString();
    var fval=field[1];
    var flen=field[2];

    if(ret[ftag] === undefined){
      ret[ftag] = []
    }

    ret[ftag].push(fval);
    offset += flen;
    
  }
  return ret;
}

function readFileblock(fd, fileoffset, callback){
  var ret = {};

  var buf = new Buffer(4);
  var totalRead=0;
  fs.read(fd,buf,0,4,fileoffset,function(err,bytesRead,buffer){
    totalRead += bytesRead;
    fileoffset += bytesRead;

    var headerLength = buf.readUInt32BE(0);
    var headerbuf = new Buffer(headerLength);
    fs.read(fd,headerbuf,0,headerLength,fileoffset,function(err,bytesRead,buffer){
      totalRead += bytesRead;
      fileoffset += bytesRead;

      var headerMessage = readMessage( headerbuf );
      ret['header']=headerMessage;

      var bloblen = headerMessage['3'][0];
      var blobbuf = new Buffer(bloblen);
      fs.read(fd,blobbuf,0,bloblen,fileoffset,function(err,bytesRead,buffer){
        totalRead += bytesRead;
        fileoffset += bytesRead;

        var packedBlobMessage = readMessage( blobbuf );
        if( packedBlobMessage['1'] !== undefined ) {
          ret['blob']=readMessage(packedBlobMessage['1']);
          callback( ret, totalRead );
        } else if( packedBlobMessage['3'] !== undefined ) {
          zlib.unzip(packedBlobMessage['3'][0],function(err,buffer){
            var unpackedBlobMessage = readMessage( buffer );
            ret['blob'] = unpackedBlobMessage;
            callback( ret, totalRead );
          });
        }
      });
    });
  });
}

function readRepeated(buf){
  ret = []

  var i=0;
  while(i<buf.length){
    var valdef = readVarint(buf,i);
    var val=valdef[0];
    ret.push( val );
    i += valdef[1];
  }

  return ret;
}

function StringTable(message){
  this.data = message['1']
  this.getString = function(i){
    return this.data[i].toString();
  }
}

function DenseInfo(message) {
  this.version = readRepeated( message['1'][0] );
  this.timestamp = readRepeated( message['2'][0] );
  this.changeset = readRepeated( message['3'][0] );
  this.uid = readRepeated( message['4'][0] );
  this.user_sid = readRepeated( message['5'][0] );
}

function DenseNodes(message){
  this.ids = readRepeated( message['1'][0] );
  this.denseinfo = new DenseInfo( readMessage( message['5'][0] ) );
  this.lat = readRepeated( message['8'][0] );
  this.lon = readRepeated( message['9'][0] );
  this.keys_vals = readRepeated( message['10'][0] );

  console.log(this.keys_vals[3]);
}

function PrimitiveGroup(message){
  this.dense = new DenseNodes( readMessage( message['2'][0] ) )
}

function OSMData(fb){
  if( fb['header']['1'].toString() !== "OSMData" ) {
    throw "Not an OSMData fileblock";
  }

  this.stringtable = new StringTable( readMessage( fb['blob']['1'][0] ) );
  this.primitivegroup = new PrimitiveGroup( readMessage( fb['blob']['2'][0] ) );
}

fs.open( "/storage/maps/boston.osm.pbf", "r", function(err,fd) {
  readFileblock(fd, 0, function(fb,bytesRead){
    readFileblock(fd, bytesRead, function(fb,bytesRead){
      var osmdata = new OSMData(fb);
      
    });
  });
});

