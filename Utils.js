/**
 * Copyright (c) 2016. Phagun Baya <http://phagunbaya.com>
 *
 */

var https    = require("https");

function Utils() {
  return {
    POST : function(host, path, payload, done, timeout){
      var options = {
        host: host,
        port: 443,
        path: path,
        method: "POST",
        headers:  {"Content-Type" : "application/json"}
      };

      var request = https.request(options, function(response){
        var result = "";
        var responseCode = response.statusCode;

        response.on('data', function(data){
          result += data;
        });

        response.on('end', function(){
          if(responseCode >= 400)
            return done(result, null);
          else
            return done(false, result);
        });

        response.on('error', function(error){
          return done(error, null);
        });
      });

      request.on('error', function(error){
        return done("Error sending request", null);
      });

      if(timeout) {
        request.setTimeout(timeout, function(){
          request.abort();
        });
      }

      request.write(payload);
      request.end();
    },
    PUT : function(host, path, payload, done, timeout){
      var options = {
        host: host,
        port: 443,
        path: path,
        method: "PUT",
        headers: {"Content-Type" : "application/json"}
      };

      var request = https.request(options, function(response){
        var result = "";
        var responseCode = response.statusCode;

        response.on('data', function(data){
          result += data;
        });

        response.on('end', function(){
          if(responseCode >= 400)
            return done(result, null);
          else
            return done(false, result);
        });

      });

      request.on('error', function(error){
        return done("Error handling request", null);
      });

      if(timeout) {
        request.setTimeout(timeout, function(){
          request.abort();
        });
      }

      request.write(payload);
      request.end();
    },
    GET : function(host, path, done, timeout){
      var options = {
        host: host,
        port: 443,
        path: path,
        headers:  {"Content-Type" : "application/json"}
      };

      var request = https.get(options, function(response) {
        var result = "";
        var responseCode = response.statusCode;

        response.on('data', function(data) {
          result += data;
        });

        response.on('end', function() {
          if(responseCode >= 400)
            return done(result, null);
          else
            return done(false, result);
        });
      });

      request.on("error", function(error){
        return done("Error handling error", null);
      });

      if(timeout) {
        request.setTimeout(timeout, function(){
          request.abort();
        });
      }

      request.end();
    },
    DELETE : function(host, path, payload, done, timeout){
      var options = {
        host: host,
        port: 443,
        path: path,
        method: "DELETE",
        headers:  {"Content-Type" : "application/json"}
      };

      var request = https.request(options, function(response){
        var result = "";
        var responseCode = response.statusCode;

        response.on('data', function(data){
          result += data;
        });

        response.on('end', function(){
          if(responseCode >= 400)
            return done(result, null);
          else
            return done(false, result);
        });
      });

      request.on('error', function(error){
        return done("Error sending request", null);
      });

      if(timeout) {
        request.setTimeout(timeout, function(){
          request.abort();
        });
      }

      request.write(payload);
      request.end();
    },
    POSTFORM : function(host, path, formdata, done){
      var options = {
        host: host,
        port: 443,
        path: path,
        method: "POST",
        headers: formdata.getHeaders()
      };

      var request = https.request(options);
      formdata.pipe(request);
      var result = "";

      request.on('response', function(res) {
        var responseCode = res.statusCode;
        res.on('data', function(data){
          result += data;
        });

        res.on('end', function(){
          if(responseCode >= 400)
            return done(result, null, responseCode);
          else
            return done(false, result, responseCode);
        });

        res.on('error', function(error){
          return done(error, null, responseCode);
        });
      });
    }
  }
};

module.exports = Utils();