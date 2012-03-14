var fs = require('fs');

var rad=function(x){
  return Math.PI*(x/180.0);
}

function distVincenty(lat1, lon1, lat2, lon2) {
  var a = 6378137, b = 6356752.314245,  f = 1/298.257223563;  // WGS-84 ellipsoid params
  var L = rad(lon2-lon1);
  var U1 = Math.atan((1-f) * Math.tan(rad(lat1)));
  var U2 = Math.atan((1-f) * Math.tan(rad(lat2)));
  var sinU1 = Math.sin(U1), cosU1 = Math.cos(U1);
  var sinU2 = Math.sin(U2), cosU2 = Math.cos(U2);
  
  var lambda = L, lambdaP, iterLimit = 100;
  do {
    var sinLambda = Math.sin(lambda), cosLambda = Math.cos(lambda);
    var sinSigma = Math.sqrt((cosU2*sinLambda) * (cosU2*sinLambda) + 
      (cosU1*sinU2-sinU1*cosU2*cosLambda) * (cosU1*sinU2-sinU1*cosU2*cosLambda));
    if (sinSigma==0) return 0;  // co-incident points
    var cosSigma = sinU1*sinU2 + cosU1*cosU2*cosLambda;
    var sigma = Math.atan2(sinSigma, cosSigma);
    var sinAlpha = cosU1 * cosU2 * sinLambda / sinSigma;
    var cosSqAlpha = 1 - sinAlpha*sinAlpha;
    var cos2SigmaM = cosSigma - 2*sinU1*sinU2/cosSqAlpha;
    if (isNaN(cos2SigmaM)) cos2SigmaM = 0;  // equatorial line: cosSqAlpha=0 (§6)
    var C = f/16*cosSqAlpha*(4+f*(4-3*cosSqAlpha));
    lambdaP = lambda;
    lambda = L + (1-C) * f * sinAlpha *
      (sigma + C*sinSigma*(cos2SigmaM+C*cosSigma*(-1+2*cos2SigmaM*cos2SigmaM)));
  } while (Math.abs(lambda-lambdaP) > 1e-12 && --iterLimit>0);

  if (iterLimit==0) return NaN  // formula failed to converge

  var uSq = cosSqAlpha * (a*a - b*b) / (b*b);
  var A = 1 + uSq/16384*(4096+uSq*(-768+uSq*(320-175*uSq)));
  var B = uSq/1024 * (256+uSq*(-128+uSq*(74-47*uSq)));
  var deltaSigma = B*sinSigma*(cos2SigmaM+B/4*(cosSigma*(-1+2*cos2SigmaM*cos2SigmaM)-
    B/6*cos2SigmaM*(-3+4*sinSigma*sinSigma)*(-3+4*cos2SigmaM*cos2SigmaM)));
  var s = b*A*(sigma-deltaSigma);
  
  s = s.toFixed(3); // round to 1mm precision
  return s;
  
  // note: to return initial/final bearings in addition to distance, use something like:
  var fwdAz = Math.atan2(cosU2*sinLambda,  cosU1*sinU2-sinU1*cosU2*cosLambda);
  var revAz = Math.atan2(cosU1*sinLambda, -sinU1*cosU2+cosU1*sinU2*cosLambda);
  return { distance: s, initialBearing: fwdAz.toDeg(), finalBearing: revAz.toDeg() };
}

function split_line_segment(lng1, lat1, lng2, lat2, max_section_length){
    // Split line segment defined by (x1, y1, x2, y2) into a set of points 
    // (x,y,displacement) spaced less than max_section_length apart

    if(lng1===null || lat1===null || lng2===null || lat2===null){
      return null;
    }
   
    var ret = [];
 
    if(lng1==lng2 && lat1==lat2){
        ret.push( [lng1, lat1, 0] )
        ret.push( [lng2, lat2, 0] )
        return ret;
    }
    
    var street_len = distVincenty(lat1, lng1, lat2, lng2);
    var n_sections = Math.floor(street_len/max_section_length)+1;
    
    var geolen = Math.pow(Math.pow((lat2-lat1),2) + Math.pow((lng2-lng1),2),0.5);
    var section_len = geolen/n_sections;
    var street_vector = [lng2-lng1, lat2-lat1];
    var unit_vector = [street_vector[0]/geolen, street_vector[1]/geolen];
    
    for(var i=0; i<n_sections+1; i++){
        var vec = [unit_vector[0]*section_len*i, unit_vector[1]*section_len*i];
        vec = [lng1+vec[0], lat1+vec[1], (street_len/n_sections)*i]
        ret.push( vec );
    }

    return ret;
}

function split_line_string(points, max_section_length){
    
    //Split each line segment in the linestring into segment smaller than max_section_length
    var split_segs = [];
    for(var i=0; i<points.length-1; i++){
      var pt1 = points[i];
      var pt2 = points[i+1];
      var split_seg = split_line_segment(pt1[0], pt1[1], pt2[0], pt2[1], max_section_length);
      if( split_seg===null ){
        return null; // if any input point is null, give up
      }
      split_segs.push( split_seg );
    }
    
    //String together the sub linestrings into a single linestring
    var ret = []
    var segstart_s = 0
    for(var i=0; i<split_segs.length; i++){
      var split_seg = split_segs[i];

      for(var j=0; j<split_seg.length-1; j++){
        ret.push( [split_seg[j][0], split_seg[j][1], split_seg[j][2]+segstart_s] );
      }
        
      if(i==split_segs.length-1){
        ret.push( [split_seg[split_seg.length-1][0], split_seg[split_seg.length-1][1], split_seg[split_seg.length-1][2]+segstart_s] )
      }
        
      segstart_s += split_seg[split_seg.length-1][2];
    }
            
    return ret;
}

function trim(string) {
    return string.replace(/^\s*|\s*$/g, '')
}

function GridFloat(basename){
  this.basename = basename;
  this.fp = fs.openSync(this.basename+".flt", "r");

  this._read_header = function(){
    var hdr = fs.readFileSync( this.basename+".hdr", "utf-8" );
    var hdrlines = hdr.split(/\r?\n/);
    var hdrdict = {};

    for(var i in hdrlines){
      var key = trim( hdrlines[i].substring(0,14) );
      var val = trim( hdrlines[i].substring(14) );
      hdrdict[key]=val;
    }

    this.ncols = parseInt(hdrdict.ncols);
    this.nrows = parseInt(hdrdict.nrows);
    this.xllcorner = parseFloat(hdrdict.xllcorner);
    this.yllcorner = parseFloat(hdrdict.yllcorner);
    this.cellsize = parseFloat(hdrdict.cellsize);
    this.NODATA_value = parseInt(hdrdict.NODATA_value);
    this.byteorder = hdrdict.byteorder;
    this.lsbfirst = this.byteorder==="LSBFIRST";

    this.left = this.xllcorner;
    this.right = this.xllcorner + (this.ncols-1)*this.cellsize;
    this.bottom = this.yllcorner;
    this.top = this.yllcorner + (this.nrows-1)*this.cellsize;
  }

  this.cell = function(x,y){
    var position = (y*this.ncols+x)*4;
    var buf = new Buffer(4);
    fs.readSync( this.fp, buf, 0, 4, position );
    if(this.lsbfirst){
      return buf.readFloatLE(0);
    } else {
      return buf.readFloatBE(0);
    }
  }

  this.allcells = function(){
    var buf = new Buffer(this.ncols*this.nrows*4);
    fs.readSync(this.fp, buf, 0, this.ncols*this.nrows*4, 0);
    
    var ret = [];
    for(var i=0; i<this.ncols*this.nrows; i++){
      ret.push( this.lsbfirst ? buf.readFloatLE(i*4) : buf.readFloatBE(i*4) );
    }
    return ret;
  }

  this.extremes = function(){
    var all = this.allcells();
    var max = -10000000;
    var maxpoint;
    var min = 10000000;
    var minpoint;
    for( var i in all ){
      var y = this.top-this.cellsize*(Math.floor(i/this.ncols));
      var x = this.left+this.cellsize*(i%this.ncols);

      if(all[i] < min){
        min=all[i];
        minpoint=[x,y];
      }
      if(all[i] > max){
        max=all[i];
        maxpoint=[x,y];
      }
    }
    return [[min,minpoint],[max,maxpoint]];
  }

  this.elevation = function( lng, lat, interpolate ) {
    if(interpolate===undefined){
      interpolate=true;
    }

    if(lng < this.left || lng >= this.right || lat <= this.bottom || lat > this.top) {
      return null;
    }
        
    var x = (lng-this.left)/this.cellsize;
    var y = (this.top-lat)/this.cellsize;
        
    var ulx = Math.floor(x);
    var uly = Math.floor(y);
        
    var ul = this.cell( ulx, uly );
    if(!interpolate){
      return ul;
    }
    var ur = this.cell( ulx+1, uly );
    var ll = this.cell( ulx, uly+1 );
    var lr = this.cell( ulx+1, uly+1 );
        
    var cellleft = x%1;
    var celltop = y%1;
    var um = (ur-ul)*cellleft+ul; //uppermiddle
    var lm = (lr-ll)*cellleft+ll; //lowermiddle
        
    return (lm-um)*celltop+um;
  }

  this.profile = function(points, resolution){
    if(resolution===undefined){
      resolution=10;
    }

    var splitted = split_line_string( points, resolution );
    if( splitted===null ){
      return null; // if the line splitter failed, give up
    } 

    var ret = [];
    for(var i in splitted){
      var pt = splitted[i];
      ret.push( [pt[2],this.elevation(pt[0],pt[1])] );
    }
    
    return ret;
  }

  this._read_header( this.basename+".hdr" );

}

//var gf = new GridFloat( "data/65991774/65991774" )
//console.log( gf );
//console.log( gf.cell(0,0) );
//console.log( gf.elevation(-122.45345, 37.738332) );

exports.GridFloat=GridFloat;
