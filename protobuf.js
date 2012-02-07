
var WIRETYPE={'LENGTH':2,
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
function decode_signed(val){
  //zigzag encoding
  if(val%2==0)
    return val/2;
  return -((val+1)/2);
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
  if(wire_type==WIRETYPE.LENGTH){
    var strlendef = readVarint(buf,offset+nread);
    var strlen = strlendef[0];
    nread += strlendef[1]; 
    val = buf.slice(offset+nread,offset+nread+strlen);
    nread += strlen;
  } else if(wire_type==WIRETYPE.VARINT) {
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


exports.more_bytes=more_bytes;
exports.strip_msb=strip_msb;
exports.get_wire_type=get_wire_type;
exports.get_field_number=get_field_number;
exports.decode_signed=decode_signed;
exports.readVarint=readVarint;
exports.readField=readField;
exports.Message=Message;
