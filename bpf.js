var fs = require('fs');
var zlib = require('zlib');

var wiretype={'STRING':2,
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
  if(wire_type==wiretype.STRING){
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

fs.open( "/storage/maps/boston.osm.pbf", "r", function(err,fd) {
  var buf = new Buffer(4);
  var fileoffset=0;
  fs.read(fd,buf,0,4,fileoffset,function(err,bytesRead,buffer){
    fileoffset += 4;

    var headerLength = buf.readUInt32BE(0);
    var headerbuf = new Buffer(headerLength);
    fs.read(fd,headerbuf,0,headerLength,fileoffset,function(err,bytesRead,buffer){
      fileoffset += headerLength;

      var message = readMessage( headerbuf );
      console.log( message );

      var bloblen = message['3'][0];
      var blobbuf = new Buffer(bloblen);
      fs.read(fd,blobbuf,0,bloblen,fileoffset,function(err,bytesRead,buffer){
        console.log( blobbuf );
        var blobmessage = readMessage( blobbuf );
        console.log( blobmessage );
        console.log( blobmessage['3'][0].length );
        zlib.unzip(blobmessage['3'][0],function(err,buffer){
          console.log( buffer );
          console.log( buffer.length );
          var osmheader = readMessage( buffer );
          console.log( osmheader );
          console.log( osmheader['4'][1].toString('utf8') );
        });
      });
    });
  });
});

