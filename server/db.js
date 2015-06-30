var mongoskin = require('mongoskin');

module.exports = (function() {
  var DB_NAME = 'oftalmo';
  // XXX use process.env.DB_HOST
  var DB_HOST = 'localhost';

  var db = mongoskin.db('mongodb://@localhost:27017/oftalmo', {safe:true});

  return db;
})();
