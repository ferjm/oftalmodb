var express   = require('express');
var http      = require('http');

module.exports = (function() {
  var app = express();

  // Configure.
  require('./config')(app);

  // DB.
  db = require('./db');

  // API.
  require('./api')(app, db);

  function run() {
    http.createServer(app).listen(app.get('port'), function() {
      console.log('Oftalmo server listening on port ' + app.get('port'));
    });
  }

  return {
    run: run,
    app: app
  };
})();
