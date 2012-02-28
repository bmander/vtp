/*var http = require('http');
http.createServer(function (req, res) {
  res.writeHead(200, {'Content-Type': 'text/plain'});
  res.end('Hello World\n');
}).listen(80, "ec2-174-129-102-215.compute-1.amazonaws.com");
console.log('Server running at http://127.0.0.1:80/');*/

var express = require('express');
var fs = require('fs');
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

app.get('/', function(req, res){
  res.contentType("html");
  fs.readFile("templates/game.html", function(err,data){
    res.send( data );
  });
});

app.listen(80);
