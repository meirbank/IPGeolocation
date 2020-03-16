var maxmind = require('maxmind');
var cityLookup = maxmind.open('./GeoLite2-City.mmdb');

var elasticsearch = require('elasticsearch');

var bodyParser = require('body-parser');
var express = require('express');
var request = require('request')
var app = express();

var geoLoaded = false;

app.enable('trust proxy');
app.use(bodyParser.json()); // for parsing application/json
app.use(bodyParser.urlencoded({ extended: true })); // for parsing application/x-www-form-urlencoded

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");//"https://globlee.com");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

var geocoder = require('./geocoder.js');

var client = new elasticsearch.Client({
	host: process.env.elasticsearch_connection_string,
	connectionClass: require('http-aws-es'),
	amazonES: {
	    region: 'us-west-2',
	    accessKey: '',
	    secretKey: ''
	}//,
	//log: 'trace'
});
var client_RU = new elasticsearch.Client({
  host:process.env.elasticsearch_connection_string_ru || "",
  connectionClass: require('http-aws-es'),
  amazonES: {
      region: 'us-west-2',
      accessKey: "",
      secretKey: ""
  }//,
  //log: 'trace'
});


function ipLocate(ip,callback){
	if (maxmind.validate(ip)){
		try {
			var lookup = cityLookup.get(ip);
			//console.log(lookup)
			var resp ={};
			if (lookup){
				if (lookup.subdivisions && lookup.subdivisions.length>0 && lookup.subdivisions[0].names && lookup.subdivisions[0].names.en){
					resp.state = lookup.subdivisions[0].names.en;
					resp.stateid = lookup.subdivisions[0].geoname_id
				} 
				if (lookup.city && lookup.city.names && lookup.city.names.en){
					resp.city = lookup.city.names.en;
					resp.cityid = lookup.city.geoname_id
				}
				if (lookup.country && lookup.country.names && lookup.country.names.en){
					resp.country = lookup.country.names.en;
					resp.countryid = lookup.country.geoname_id
				}
				callback(null, resp)
			} else {
				callback(true, null)
			}
		} catch(e){
			callback(true, null)
		}
	} else {
		callback(true, null)
	}
}

function coordsLocate(lat, long, lang, callback){
	if (geoLoaded){

		var points = {
			latitude: lat,
			longitude:long
		}

		//var points = {latitude:'39.26628442213066', longitude:'-76.937255859375'};

		geocoder.lookUp(points, function(err, addresses) { 
		  	if (err || !addresses || addresses.length==0 || addresses[0].length==0){
		  		console.log("GEO LOCAL REVERSE ERROR", err, addresses)
		  		callback((err || true), null);
		  	} else {
		  		
		  		//console.log("country", addresses[0][0].countryName)
		  		//console.log({city:addresses[0][0].name, state:addresses[0][0].admin1Code.name, country:addresses[0][0].admin2Code})
		  		console.log(addresses)

		  		var retObj = {};

		  		if (addresses[0][0].name && addresses[0][0].geoNameId){

		  			retObj.city = addresses[0][0].name;
		  			retObj.city_geoid = addresses[0][0].geoNameId
		  		
			  		if (addresses[0][0].subdivision_1_name && addresses[0][0].subdivision_1_geoname_id){
			  			retObj.state = addresses[0][0].subdivision_1_name;
			  			retObj.state_geoid = addresses[0][0].subdivision_1_geoname_id
			  		}
			  		if (addresses[0][0].country_name && addresses[0][0].country_geoname_id){
			  			retObj.country = addresses[0][0].country_name;
			  			retObj.country_geoid = addresses[0][0].country_geoname_id
			  		}
			  	}

		  		console.log("RES: ", retObj)

		  		callback(null, retObj)
		  	}
		});


	} else {
		request.get("http://api.geonames.org/findNearbyPostalCodesJSON?lat="+lat+"&lng="+long+"&maxRows=1&username=globlee", {json:true}, function (error, httpresponse, body) {
			if (!error) {
				if (body && body.postalCodes && body.postalCodes[0]){
					request.get("http://api.geonames.org/countrySubdivisionJSON?lat="+lat+"&lng="+long+"&username=globlee", {json:true}, function (errorTwo, httpresponseTwo, bodyTwo) {
						if (!errorTwo) {
							if (bodyTwo && bodyTwo.countryName){
								//console.log({city:body.postalCodes[0].placeName /*body.postalCodes[0].adminName2*/, state:body.postalCodes[0].adminName1, country:bodyTwo.countryName, zip:body.postalCodes[0].postalCode}, body.postalCodes[0], bodyTwo)
								callback(null, {city:body.postalCodes[0].placeName, state:body.postalCodes[0].adminName1, country:bodyTwo.countryName, zip:body.postalCodes[0].postalCode})
							} else {
								callback("Invalid Body Response Geonames", null)
							}

						} else {
							callback(errorTwo, null)
						}

					})

				} else {
					callback("Invalid Body Response Geonames", null)
				}
			} else {
				callback(error, null)
			}
		});
	}

}

function suggest(location, currency, callback){
	//console.log("STARTED")
	var resp =[];
	var currentClient = client;
	if (currency && currency=="RUB"){
		currentClient = client_RU;
	}
	currentClient.suggest({
		index: 'city',
		body: {
			text : location,
		    mysuggest : {
		        completion : {
		        	size:3,
		            field : "city"
		        }
		    }
		}
	}, function (error, response) {
		console.log("SUGGEST RESPONSE FORMAT CITY:", JSON.stringify(response, null, 2))
		if (response && response.mysuggest && response.mysuggest.length>0 && response.mysuggest[0].options){
			resp = resp.concat(response.mysuggest[0].options)
		}
		if (error){
			console.log(error, response)
		}
		currentClient.suggest({
			index: 'state',
			body: {
				text : location,
			    mysuggest : {
			        completion : {
			        	size:3,
			            field : "state"
			        }
			    }
			}
		}, function (error2, response2) {
			console.log("SUGGEST RESPONSE FORMAT STATE:", JSON.stringify(response2, null, 2))
			if (error2){
				console.log(error2, response2)
			}
			if (response2 && response2.mysuggest && response2.mysuggest.length>0 && response2.mysuggest[0].options){
				resp = resp.concat(response2.mysuggest[0].options)
			}
			currentClient.suggest({
				index: 'country',
				body: {
					text : location,
				    mysuggest : {
				        completion : {
				        	size:3,
				            field : "country"
				        }
				    }
				}
			}, function (error3, response3) {
				console.log("SUGGEST RESPONSE FORMAT COUNTRY:", JSON.stringify(response3, null, 2))
				if (error3){
					console.log(error3, response3)
				}
				if (response3 && response3.mysuggest && response3.mysuggest.length>0 && response3.mysuggest[0].options){
					resp =resp.concat(response3.mysuggest[0].options)
				}
				if (resp.length>0){
					callback(null, resp)
				} else {
					callback(true, null)
				}
			});
		});
	});
}

app.post('/', function(req, res){
	console.log("HELLO", req.body)
	try {
		if (req.body && req.body.latitude && req.body.longitude){
			coordsLocate(req.body.latitude, req.body.longitude, (req.body.currencyzone || "en"), function(err, resp){
				if (err || !resp){
					if (req.body.ip){
						ipLocate(req.body.ip, function(err, resp){
							if (err){
								res.sendStatus(500)
							} else if (resp){
								res.json({results:resp})
							} else {
								res.sendStatus(500)
							}
						})
					}
				} else{
					//console.log("USED COORDS FOR LOCATION", resp)
					res.json({results:resp})
				}
			})
		} else if (req.body && req.body.ip){
			ipLocate(req.body.ip, function(err, resp){
				if (err){
					res.sendStatus(500)
				} else if (resp){
					res.json({results:resp})
				} else {
					res.sendStatus(500)
				}
			})
		} else if (req.body && req.body.query){
			//console.log("HELLO")
			suggest(req.body.query, (req.body.currencyzone || "en"), function(err, resp){
				//console.log("FINISHED")
				if (err){
					res.sendStatus(500)
				} else if (resp && resp.length >0){
					res.json({results:resp})
				} else {
					res.sendStatus(500)
				}
			})
		} else {
			res.sendStatus(400)
		}
	} catch(e){
		console.log("ERROR", e)
	}
});

try {
	geocoder.init({alternateNames: false}, function() {
		console.log("initialized geocoder")
		geoLoaded = true;
	})
} catch(e){
	console.log("ERR", e)
}


setTimeout(function(){
	geoLoaded = true;
}, 500000)

	app.listen(process.env.PORT || 3001);
	console.log("Starting.")
