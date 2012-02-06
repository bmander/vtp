var fs = require('fs');
var zlib = require('zlib');

var wiretype={'LENGTH':2,
              'VARINT':0};

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
    nread += strlen;
  } else if(wire_type==wiretype.VARINT) {
    valdef = readVarint( buf, offset+nread );
    val = valdef[0];
    nread += valdef[1];
  }

  return [field_number, val, nread];
}

function Message(buf){
  this.fields = {}

  var offset=0;
  while(offset<buf.length){
    var field = readField( buf, offset );
    var ftag=field[0].toString();
    var fval=field[1];
    var flen=field[2];

    if(this.fields[ftag] === undefined){
      this.fields[ftag] = []
    }

    this.fields[ftag].push(fval);
    offset += flen;
    
  }

  this.val = function(tag){
    if(!this.hasField(tag))
      return null;
    return this.fields[tag.toString()][0];
  }
  this.vals = function(tag){
    if(!this.hasField(tag))
      return []
    return this.fields[tag.toString()];
  }
  this.hasField = function(tag){
    return this.fields[tag.toString()]!==undefined
  }
}

function BlobHeader(message){
  this.type = message.val(1).toString();
  this.indexdata = message.val(2);
  this.datasize = message.val(3);
}

function Fileblock(fd, fileoffset, callback){
  this.fd=fd;
  this.fileoffset=fileoffset;
  this.header=null;
  this.payload=null;

  this.read = function(callback){
    // read header length
    var buf = new Buffer(4);
    var totalRead=0;
    fs.read(fd,buf,0,4,fileoffset,function(err,bytesRead,buffer){
      totalRead += bytesRead;
      fileoffset += bytesRead;

      // read the header
      var headerLength = buf.readUInt32BE(0);
      var headerbuf = new Buffer(headerLength);

      fs.read(fd,headerbuf,0,headerLength,fileoffset,function(err,bytesRead,buffer){
        totalRead += bytesRead;
        fileoffset += bytesRead;

        var headerMessage = new Message( headerbuf );
        this.header= new BlobHeader(headerMessage);

        // read the blob payload
        var bloblen = headerMessage.val(3);
        var blobbuf = new Buffer(bloblen);
        fs.read(fd,blobbuf,0,bloblen,fileoffset,function(err,bytesRead,buffer){
          totalRead += bytesRead;
          fileoffset += bytesRead;

          var packedBlobMessage = new Message( blobbuf );

          if( packedBlobMessage.hasField(1) ) {
            this.payload=new Message(packedBlobMessage.val(1));
            callback( this, totalRead );
          } else if( packedBlobMessage.hasField(3) ) {
            zlib.unzip(packedBlobMessage.val(3),function(err,buffer){
              var unpackedBlobMessage = new Message( buffer );
              this.payload = unpackedBlobMessage;
              callback( this, totalRead );
            });
          }
        });
      });
    });
  }
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
  this.ids = readRepeated( message.val(1) );
  this.denseinfo = new DenseInfo( new Message( message.val(5) ) );
  this.lat = readRepeated( message.val(8) );
  this.lon = readRepeated( message.val(9) );
  this.keys_vals = readRepeated( message.val(10) );
}

function PrimitiveGroup(message){
  this.dense = new DenseNodes( new Message( message.val(2) ) )
}

function OSMData(fb){
  if( fb['header'].val(1).toString() !== "OSMData" ) {
    throw "Not an OSMData fileblock";
  }

  this.stringtable = new StringTable( new Message( fb['blob'].val(1) ) );
  this.primitivegroup = new PrimitiveGroup( new Message( fb['blob'].val(2) ) );
}

function FileBlockFile(path){
  this.read = function(callback){
    fs.open( path, "r", function(err,fd) {
      var stats = fs.statSync( path );

      var offset=0;
      var onblobread = function(fb,bytesRead){
        if(fb){
          callback(fb);
        }

        offset += bytesRead;
        if(offset==stats.size)
          return;

        var fileblock = new Fileblock(fd,offset,onblobread);
        fileblock.read( onblobread );
      }
      onblobread(null,0);
    });
  }
}

var path="/storage/maps/boston.osm.pbf";
var fileblockfile = new FileBlockFile(path);
fileblockfile.read(function(fb){
  console.log(fb.header);
})
