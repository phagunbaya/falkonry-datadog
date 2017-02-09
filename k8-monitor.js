/**
 * Copyright (c) 2016. Phagun Baya <http://phagunbaya.com>
 *
 */

var async = require("async");
var Utils = require("./Utils");
var _     = require("underscore");
var formData = require('form-data');
var json2csv = require('json2csv');
var fs = require("fs");

var datadogHost = "app.datadoghq.com";
var falkonryHost = "";
var api_key = "";
var app_key ="";
var interval = 10*1000;
var startTime = 1456365600;//Date.now()/1000 - interval;
var endTime   = Date.now()/1000;

var pipeline = "";
var account = "";
var inputs = [
  "system.cpu.user",
  "system.mem.used",
  "system.io.wkb_s",
  "system.io.rkb_s",
  "system.disk.used"
];
var thingIdentifier = "host";

function getDataFromDatadog(signal, done) {
  //var options = "from="+startTime+"&to="+endTime+"&query="+encodeURIComponent(signal+"{*}by{"+thingIdentifier+"}")+"&api_key="+api_key+"&application_key="+app_key;
  var options = "from="+(pipeline.latestDataPoint/1000)+"&to="+endTime+"&query="+encodeURIComponent(signal+"{*}by{"+thingIdentifier+"}")+"&api_key="+api_key+"&application_key="+app_key;
  return Utils.GET(datadogHost, "/api/v1/query?"+options, function(e, r){
    if(e) {
      console.log("Error retrieving data from datadog : "+e);
    }
    return done(null, e ? null : JSON.parse(r));
  });
}

function sendDataToFalkonry(file, done) {
  var options = "apiToken="+account;
  var form = new formData();
  //return done(null, null);
  form.append('data', fs.createReadStream(file));
  return Utils.POSTFORM(falkonryHost, "/pipeline/"+pipeline.id+"/input?"+options, form, function(e, r){
    if(e) {
      console.log("Error sending input data to falkonry : "+e);
    }
    else {
      console.log("Input data tracker - "+JSON.parse(r).__$id);
    }
    fs.unlink(file, function(e, r){
      if(e)
        console.log("Error deleting file : "+e);
    });
    return done(null, null);
  });
}

function createPipeline(done) {
  var payload = {
    "name": "K8 Minions",
    "inputConf": {
      "type": "FILE",
      "streaming": true
    },
    "thingIdentifier": thingIdentifier,
    "timeIdentifier": "time",
    "inputList": [],
    "assessmentList": [
      {
        "name": "System Activity",
        "inputList": inputs
      }
    ]
  };
  inputs.forEach(function(eachInput){
    payload.inputList.push({
      "name": eachInput,
      "valueType": {
        "type": "Numeric"
      }
    });
  });
  var options = "apiToken="+account;
  return Utils.POST(falkonryHost, "/pipeline?"+options, JSON.stringify(payload), function(e, r){
    if(e) {
      console.log("Error creating pipeline : "+e);
      throw new Error(e);
    }
    else {
      pipeline = JSON.parse(r);
      console.log("Created pipeline with id : " + pipeline.id);
    }
    return done(null, null);
  });
}

function getPipeline(id, done) {
  var options = "apiToken="+account;
  return Utils.GET(falkonryHost, "/pipeline/"+id+"?"+options, function(e, r){
    if(e) {
      console.log("Error retrieving pipeline : "+e);
      throw new Error(e);
    }
    else {
      pipeline = JSON.parse(r);
    }
    return done(e, r);
  });
}

function getJSON(data) {
  var datapoints = [];
  if(!Array.isArray(data)) {
    data = [data];
  }
  data.forEach(function(eachResponse){
    var series = eachResponse.series;
    series.forEach(function(eachSeries){
      var pointList = eachSeries.pointlist;
      pointList.forEach(function(eachPoint){
        var datapoint = _.find(datapoints, function(point){
          return point.time === eachPoint[0] && point[thingIdentifier] === eachSeries.scope.replace(thingIdentifier+":", "");
        });
        if(!datapoint) {
          datapoint = {
            "time" : eachPoint[0]
          };
          datapoint[thingIdentifier] = eachSeries.scope.replace(thingIdentifier+":", "");
          pipeline.inputList.forEach(function(eachInput){
            if(eachInput.name === eachSeries.metric)
              datapoint[eachInput.name] = eachPoint[1];
            //else
            //  datapoint[eachInput.name] = null;
          });
        }
        else {
          pipeline.inputList.forEach(function(eachInput){
            if(eachInput.name === eachSeries.metric)
              datapoint[eachInput.name] = eachPoint[1];
          });
        }
        datapoints.push(datapoint);
      });
    });
  });

  return datapoints;
}

function startStreaming(completed) {
  var asyncTasks = [];
  if(!pipeline) {
    asyncTasks.push(function(_cb) {
      createPipeline(_cb);
    });
  }
  else {
    if(typeof pipeline === "string") {
      asyncTasks.push(function(_cb){
        getPipeline(pipeline, _cb);
      });
    }
  }

  return async.series(asyncTasks, function(e, r){
    return async.parallel(function(){
      var tasks = [];
      var func = function(signal){
        return function(__cb) {
          return getDataFromDatadog(signal, __cb);
        }
      };
      pipeline.inputList.forEach(function(eachInput){
        tasks.push(func(eachInput.name));
      });
      return tasks;
    }(), function(e, r){
      var jsonData = getJSON(r);
      var fields = ["time", thingIdentifier];
      pipeline.inputList.forEach(function(eachInput){
        fields.push(eachInput.name);
      });
      json2csv({ data: jsonData, fields: fields }, function(err, csvData) {
        if (err)
          console.log(err);
        else {
          console.log("Got the data");
          var file = '/tmp/'+Date.now()+'.csv';
          return fs.writeFile(file, csvData, function(err) {
            if (err) throw err;
            return sendDataToFalkonry(file, function(e, r){
              console.log("Done streaming");
              console.log("Sleeping for "+interval/1000+"sec ....");
              if(completed) return completed(null, null);
            });
          });
        }
      });
    });
  });
}

startStreaming(function(){
  setInterval(startStreaming, interval*1000);
});