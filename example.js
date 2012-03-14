
var express = require('express');
var fs = require('fs');
var mongodb = require('mongodb');

var server = new mongodb.Db('test', new mongodb.Server("127.0.0.1", 27017, {}));
var app = express.createServer();

app.get('/foobar', function(req,res){
  res.contentType("text");
  res.send( "Hi Elsbeth!" );
});

app.get('/static/js/jquery.js', function(req,res){
  res.contentType("javascript");
  fs.readFile( "templates/jquery.js", function(err,data){
    res.send( data );
  });
});

server.open(function(err, client) {
  var collection = new mongodb.Collection(client,"tiled_ways");
  var compcoll = new mongodb.Collection(client,"simple_tiles");
  var profilecoll = new mongodb.Collection(client,"profiles");

  app.get('/tile/*', function(req,res) {
    res.contentType("json");
    var cursor = collection.find({_id:req.params[0]}).limit(1);
    cursor.nextObject( function(err,doc){
        res.send( doc );
    });
  });

  app.get('/comptile/*', function(req,res) {
    res.contentType("json");
    var cursor = compcoll.find({_id:req.params[0]}).limit(1);
    cursor.nextObject( function(err,doc) {
      res.send( doc );
    });
  });
  
  app.get('/profile/*', function(req,res) {
    res.contentType("json");
    var cursor = profilecoll.find({id:req.params[0]}).limit(1);
    cursor.nextObject( function(err,doc) {
      res.send( doc );
    });
  });
});

app.get('/', function(req, res){
  res.contentType("html");
  fs.readFile("templates/game.html", function(err,data){
    res.send( data );
  });
});

app.listen(80);
