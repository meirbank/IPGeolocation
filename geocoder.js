'use strict';

/*

var geocoder = require("./geocoder.js");
var point = {latitude: 55.74639726375177, longitude: 37.42217259073362};
geocoder.lookUp(point, function(err, res) {
  console.log(JSON.stringify(res, null, 2));
});



*/

var DEBUG = true;
var fs = require('fs');
var path = require('path');
var parse = require('csv-parse');
var kdTree = require('kdt');
var request = require('request');
var async = require('async');
var readline = require('readline');
var unzip = require('unzip2');

/* jshint maxlen: false */
var GEONAMES_CITIES_COLUMNS = [
  'geoNameId', // integer id of record in geonames database
  'name', // name of geographical point (utf8) varchar(200)
  'asciiName', // name of geographical point in plain ascii characters, varchar(200)
  'alternateNames', // alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
  'latitude', // latitude in decimal degrees (wgs84)
  'longitude', // longitude in decimal degrees (wgs84)
  'featureClass', // see http://www.geonames.org/export/codes.html, char(1)
  'featureCode', // see http://www.geonames.org/export/codes.html, varchar(10)
  'countryCode', // ISO-3166 2-letter country code, 2 characters
  'cc2', // alternate country codes, comma separated, ISO-3166 2-letter country code, 60 characters
  'admin1Code', // fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
  'admin2Code', // code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
  'admin3Code', // code for third level administrative division, varchar(20)
  'admin4Code', // code for fourth level administrative division, varchar(20)
  'population', // bigint (8 byte int)
  'elevation', // in meters, integer
  'dem', // digital elevation model, srtm3 or gtopo30, average elevation 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
  'timezone', // the timezone id (see file timeZone.txt) varchar(40)
  'modificationDate', // date of last modification in yyyy-MM-dd format
];

var GEONAMES_STATES_COLUMNS = [
  'concatenatedCodes',
  'name',
  'asciiName',
  'geoNameId'
];

var GEONAMES_COUNTRIES_COLUMNS = [
'ISO',  
'ISO3', 
'ISONumeric',  
'fips', 
'Country',  
'Capital',  
'Area', 
'Population', 
'Continent',  
'tld',  
'CurrencyCode', 
'CurrencyName', 
'Phone',  
'PostalCodeFormat', 
'PostalCodeRegex',  
'Languages',  
'geonameid',  
'neighbours', 
'EquivalentFipsCode'
];

var CITIES_FILE = 'import-cities.txt';
var STATES_FILE = 'import-states.txt'
var COUNTRIES_FILE = 'import-countries.txt';
var GEONAMES_DUMP = __dirname+"/";

var geocoder = {

  _kdTree: null,
  _kdTreeRU: null,
  _admin1Codes: null,
  _countryNames:null,

  _distanceFunc: function distance(x, y) {
    var toRadians = function(num) {
      return num * Math.PI / 180;
    };
    var lat1 = x.latitude;
    var lon1 = x.longitude;
    var lat2 = y.latitude;
    var lon2 = y.longitude;

    var R = 6371; // km
    var φ1 = toRadians(lat1);
    var φ2 = toRadians(lat2);
    var Δφ = toRadians(lat2 - lat1);
    var Δλ = toRadians(lon2 - lon1);
    var a = Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
            Math.cos(φ1) * Math.cos(φ2) *
            Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
  },

  _parseGeoNamesCities: function(callback) {
    DEBUG && console.log('Started parsing cities.txt (this  may take a ' +
      'while)');
    var data = [];
    var lenI = GEONAMES_CITIES_COLUMNS.length;
    var that = this;
    var content = fs.readFileSync(GEONAMES_DUMP + CITIES_FILE);
    parse(content, {delimiter: '\t', quote: ''}, function(err, lines) {
      if (err) {
        return callback(err);
      }
      lines.forEach(function(line) {
        var lineObj = {};
        for (var i = 0; i < lenI; i++) {
          var column = line[i] || null;
          //if (column){
            lineObj[GEONAMES_CITIES_COLUMNS[i]] = column//.replace(/[^\w\s]/gi, '');
          //}
        } 
        data.push(lineObj);
      });

      DEBUG && console.log('Finished parsing cities.txt');
      DEBUG && console.log('Started building cities k-d tree');
      var dimensions = [
        'latitude',
        'longitude'
      ];
      that._kdTree = kdTree.createKdTree(data, that._distanceFunc, dimensions);
      DEBUG && console.log('Finished building cities k-d tree');
      return callback();
    });
  },

  _parseGeoNamesStates: function(callback) {
    DEBUG && console.log('Started parsing all states');
    var that = this;
    var lenI = GEONAMES_STATES_COLUMNS.length;
    that._admin1Codes = {};
    var lineReader = readline.createInterface({
      input: fs.createReadStream(GEONAMES_DUMP + STATES_FILE)
    });
    lineReader.on('line', function(line) {
      line = line.split('\t');
      for (var i = 0; i < lenI; i++) {
        var value = line[i] || null;
        if (i === 0) {
          that._admin1Codes[value] = {};
        } else {//if (value){
          that._admin1Codes[line[0]][GEONAMES_STATES_COLUMNS[i]] = value//.replace(/[^\w\s]/gi, '');
        }
      }
    });
    lineReader.on('close', function() {
      return callback();
    });
  },

  _parseGeoNamesCountries:function(callback){
    DEBUG && console.log('Started parsing all countries');
    var that = this;
    var lenI = GEONAMES_COUNTRIES_COLUMNS.length;
    that._countryNames = {};
    var lineReader = readline.createInterface({
      input: fs.createReadStream(GEONAMES_DUMP + COUNTRIES_FILE)
    });
    lineReader.on('line', function(line) {
      line = line.split('\t');
      for (var i = 0; i < lenI; i++) {
        var value = line[i] || null;
        if (i === 0) {
          that._countryNames[value] = {};
        } else {//if (value){
          that._countryNames[line[0]][GEONAMES_COUNTRIES_COLUMNS[i]] = value//.replace(/[^\w\s]/gi, '');
        }
      }
    });
    lineReader.on('close', function() {
      return callback();
    });
  },

  init: function(options, callback) {
    options = options || {};


    DEBUG && console.log('Initializing local reverse geocoder using dump ' +
        'directory: ' + GEONAMES_DUMP);

    // Create local cache folder
    if (!fs.existsSync(GEONAMES_DUMP)) {
      fs.mkdirSync(GEONAMES_DUMP);
    }

    var that = this;
    async.parallel([
      function(waterfallCallback) {
        async.waterfall([
          that._parseGeoNamesCities.bind(that)
        ], function() {
          return waterfallCallback();
        });
      },
      function(waterfallCallback) {
        async.waterfall([
          that._parseGeoNamesStates.bind(that)
        ], function() {
          console.log("DONE 2")
          return waterfallCallback();
        });
      },
      function(waterfallCallback) {
        async.waterfall([
          that._parseGeoNamesCountries.bind(that)
        ], function() {
          console.log("DONE 3")
          return waterfallCallback();
        });
      }
    ],
    // Main callback
    function(err) {
      console.log("SEEMS TO BE DONE INIT")
      if (err) {
        throw(err);
      }
      return callback();
    });
  },

  lookUp: function(points, callback) {
    var maxResults = 1;
    /*if (lang && (lang =="ru" || lang =="RU" || lang =="rub" || lang =="RUB")){
      console.log("RU LOOKUP")
      this._lookUpRU(points, maxResults, function(err, results) {
        return callback(null, results);
      });
    } else {
      console.log("Normal LOOKUP", lang)*/
      this._lookUp(points, function(err, results) {
        return callback(null, results);
      });
    //}

  },
/*
  _lookUpRU: function(points, maxResults, callback) {
    var that = this;
    // If not yet initialied, then initialize
    if (!this._kdTreeRU) {
      return this.init({}, function() {
        return that.lookUp(points, maxResults, callback);
      });
    }
    // Make sure we have an array of points
    if (!Array.isArray(points)) {
      points = [points];
    }
    var functions = [];
    points.forEach(function(point, i) {
      point = {
        latitude: parseFloat(point.latitude),
        longitude: parseFloat(point.longitude)
      };
      DEBUG && console.log('Look-up request for point ' +
          JSON.stringify(point));
      functions[i] = function(innerCallback) {
        var result = that._kdTreeRU.nearest(point, maxResults);
        result.reverse();
        console.log("RAW RESULT", result)
        for (var j = 0, lenJ = result.length; j < lenJ; j++) {
          if (result && result[j] && result[j][0]) {
            var countryCode = result[j][0].country_iso_code || '';
            var admin1Code = result[j][0].subdivision_1_iso_code || '';

            if (that._admin1Codes) {
              var admin1CodeKey = countryCode + '.' + admin1Code;
              result[j][0].subdivision_1_geoname_id = that._admin1Codes[admin1CodeKey] || "";
            }

            if (that._countryNames){
              result[j][0].country_geoname_id = that._countryNames[countryCode] || "";
            }

            result[j][0].distance = result[j][1];
            result[j] = result[j][0];
          }
        }

        console.log(result)

        return innerCallback(null, result);
      };
    });
    async.series(
      functions,
    function(err, results) {
      DEBUG && console.log('Delivering joint results', results);
      return callback(null, results);
    });
  },*/

  _lookUp: function(points, callback) {
    var maxResults=1;
    var that = this;
    // If not yet initialied, then initialize
    if (!this._kdTree) {
      return this.init({}, function() {
        return that.lookUp(points, callback);
      });
    }
    // Make sure we have an array of points
    if (!Array.isArray(points)) {
      points = [points];
    }
    var functions = [];
    points.forEach(function(point, i) {
      point = {
        latitude: parseFloat(point.latitude),
        longitude: parseFloat(point.longitude)
      };
      DEBUG && console.log('Look-up request for point ' +
          JSON.stringify(point));
      functions[i] = function(innerCallback) {
        var result = that._kdTree.nearest(point, maxResults);
        result.reverse();
        //console.log("RAW RESULT", result)
        for (var j = 0, lenJ = result.length; j < lenJ; j++) {
          if (result && result[j] && result[j][0]) {
            var countryCode = result[j][0].countryCode || '';
            var admin1Code = result[j][0].admin1Code || '';
            var admin1CodeKey = countryCode + '.' + admin1Code;

            //console.log("RESRAW", result[j][0])
            //console.log("DEMOCNTRY", that._countryNames["US"])
            //console.log("STATES", admin1Code, Object.keys(that._admin1Codes).length, that._admin1Codes[admin1CodeKey], (countryCode + '.' + admin1Code))
            //console.log("COUNTRY", countryCode, Object.keys(that._countryNames).length, that._countryNames[countryCode])

            if (that._admin1Codes && that._admin1Codes[admin1CodeKey]) {
              result[j][0].subdivision_1_name = (that._admin1Codes[admin1CodeKey].name || "").replace(/[^\w\s]/gi, '');
              result[j][0].subdivision_1_geoname_id = that._admin1Codes[admin1CodeKey].geoNameId || "";
            }

            if (that._countryNames && that._countryNames[countryCode]){
              result[j][0].country_geoname_id = that._countryNames[countryCode].geonameid || "";
              result[j][0].country_name = (that._countryNames[countryCode].Country || "").replace(/[^\w\s]/gi, '');
            }

            result[j][0].name = result[j][0].name.replace(/[^\w\s]/gi, '');

            result[j][0].distance = result[j][1];
            result[j] = result[j][0];
          }
        }

        //console.log(result)

        return innerCallback(null, result);
      };
    });
    async.series(
      functions,
    function(err, results) {
      //DEBUG && console.log('Delivering joint results', results);
      return callback(null, results);
    });
  }
};

module.exports = geocoder;