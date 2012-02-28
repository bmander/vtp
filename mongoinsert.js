var mongodb = require('mongodb');

var server = new mongodb.Db('test', new mongodb.Server("127.0.0.1", 27017, {}))
server.open(function(err, client) {
  var collection = new mongodb.Collection(client,"count");

  var docs = [];
  for(var i=0; i<1000000; i++){
    docs.push({'a':i});
  }

  collection.insert(docs,{safe:true},function(err,obj){
    console.log("done");
  });
});
