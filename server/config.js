var bodyParser     = require('body-parser');
var errorHandler   = require('error-handler');
var express        = require('express');
var expressWinston = require('express-winston');
var winston        = require('winston');

var config = function(app) {
  app.set('port', process.env.PORT || 3000);
  app.use(bodyParser());
  app.use(errorHandler);
  app.use(expressWinston.logger({
    transports: [
      new winston.transports.Console({
        json: false,
        colorize: true
      })
    ],
    meta: true,
    msg: "HTTP {{req.method}} {{req.url}}"
  }));
};

module.exports = config;
