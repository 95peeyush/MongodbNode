var express = require('express');
var app = express();
let MongoClient = require("mongodb").MongoClient;
let database;
let twitter;
let database1;
let twitter1;
let bodyParser = require('body-parser')
app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.json())

//----------------------------------------------------------------------------------GET API's------------------------------------------------------------------------------------------------//

app.get("/getAllRecord", function (req, res){
	twitter.find({}).toArray(function (err , result ) {
		if(err) throw err;
		console.log("complete data is being fetched");
		res.json(result);
	})
})


app.get("/getFirst", function (req, res){	
	twitter.findOne( {} , function ( err, result ) {
		if(err) throw err;
		console.log("first record is been fetched");
		res.json(result);
	}) 
})

app.get("/getByTweetID", function (req, res){
	let id = req.query.tweetID
	id="\""+id+"\"";
	var filter = {"Tweet Id":id};
	getByField(filter,res);
})


app.get("/getByLocation" , function (req, res){
	let location=req.query.location;
	var filter = {"Tweet Location":location};
	getByField(filter,res);
})


app.get("/getByLanguage" , function (req, res){
	let Language=req.query.language;
	var filter = {"Tweet Language":Language};
	getByField(filter,res);
})



app.get("/getByUserName" , function (req, res) {
	let UserName=req.query.userName;
	var filter = {"Username":UserName};
	getByField(filter,res);
})




function getByField (filter,res){
	console.log(filter)
	return twitter.find(filter).toArray(function (err, result){
		if(err) throw err;
		res.json(result);
	})
}

//----------------------------------------------------------------------------------GET API's------------------------------------------------------------------------------------------------//

//----------------------------------------------------------------------------------GET API's NEW DB------------------------------------------------------------------------------------------------//




app.get("/getAllRecordOfNewDb" , function(req, res){
	twitter1.find({}).toArray(function (err , result ) {
		if(err) throw err;
		console.log("complete data is being fetched");
		res.json(result);
	})
})



app.get("/groupAndOrderByUserField", function(req, res){
	let groupby = "$user."+req.query.field;
	let field= req.query.field;
	let orderby = parseInt(req.query.order);
	twitter1.aggregate([{$group:{_id:groupby}},{$sort:{_id:orderby}},{$project:{_id:0,[field]:"$_id"}}]).toArray(function(err, result){
		if(err) throw err;
		console.log("Aggregation on user:"+groupby+" is performed by order:"+orderby);
		res.json(result);
	})
})


app.get("/countByUserField", function(req, res){
	let groupby = "$user."+req.query.field;
	let field= req.query.field;
	let orderby = parseInt(req.query.order);
	twitter1.aggregate([{$group:{_id:groupby , count:{$sum:1}}},{$sort:{_id:orderby}},{$project:{_id:0,[field]:"$_id"}}]).toArray(function(err, result){
		if(err) throw err;
		console.log("Aggregation on user:"+groupby+" is performed by order:"+orderby);
		res.json(result);
	})
})



app.get("/groupdByUserFieldWithFriendCountMoreThan", function(req, res){
	let groupby = "$user."+req.query.field;
	let field= req.query.field;
	let friendCount = parseInt(req.query.friendcount);
	twitter1.aggregate([{$match:{"user.friends_count":{$gt:friendCount}}},{$group:{_id:groupby}},{$project:{_id:0,[field]:"$_id","user.friends_count":1,groupby:1}}]).toArray(function (err, result){
		if(err) throw err;
		console.log("Aggregation on user:"+groupby+" is performed on friend count gt:"+friendCount);
		res.json(result);
	})
})



app.get("/groupdByUserFieldWithFriendCountMoreThanAndPushToCollectionByName",function (req, res){
	let groupby = "$user."+req.query.field;
	let field= req.query.field;
	let friendCount = parseInt(req.query.friendcount);
	let collectionName= req.query.collectionName;
	twitter1.aggregate([{$match:{"user.friends_count":{$gt:friendCount}}},{$group:{_id:groupby}},{$project:{_id:0,[field]:"$_id","user.friends_count":1,groupby:1}},{$out:collectionName}]).toArray(function (err, result){
		if(err) throw err;
		console.log("Aggregation on user:"+groupby+" is performed on friend count gt:"+friendCount);
		res.json(result);
	})
	
})




app.get("/getAllDataFromCollection",function (req,res){
	var col= req.query.collectionName;
	newCollection = database1.collection(col);
	newCollection.find({}).toArray(function (err,result){
		if(err) throw err;
		console.log("data from the collection:"+col+"is been fetched successfully");
		res.json(result);
	})
})

//----------------------------------------------------------------------------------GET API's NEW DB------------------------------------------------------------------------------------------------//



//----------------------------------------------------------------------------------POST API's------------------------------------------------------------------------------------------------//

app.post("/insertNewTweets",function (req , res){
	let newDocs=req.body;
	twitter.insertMany(newDocs , function (err, result){
	if(err) throw err;
	console.log("doc inserted successfully: "+result.insertedCount);
	res.json("doc inserted successfully: "+result.insertedCount);
	})
})



app.post("/updateByTweetId", function (req, res){
	let tweetID = req.query.tweetID;
	tweetID="\""+tweetID+"\"";
	let data = req.body;
	let filterquery ={"Tweet Id":tweetID};
	let set={$set:data}
	twitter.updateOne(filterquery,set, function(err, result){
		if(err) throw err;
	console.log("doc updated");
	res.send("doc updated");
	})
})


app.post("/addFieldToUsersByID" , function(req,res){
	let id = req.query.id_str;
	let content = req.body;
	twitter1.update({id_str:id},{$push:{"user.entities.url.urls":content}} , function(err, result){
		if(err) throw err;
	console.log("doc updated:"+result)
	res.send("doc updated:"+result)	
	})
})





//----------------------------------------------------------------------------------POST API's------------------------------------------------------------------------------------------------//


var server = app.listen(8081 , function(){
	var url = "mongodb://localhost/twitter";
	MongoClient.connect(url, function (err, db) {
		if(err) throw err
	database = db;	
	twitter = database.collection("tweets");
	console.log("connected to mongodb");
	})
	var url1 = "mongodb://localhost/twitterdb";
	MongoClient.connect(url1, function (err, db) {
		if(err) throw err
	database1 = db;	
	twitter1 = database1.collection("tweets");
	console.log("connected to mongodb");
	})
    console.log("server is started and listening on localhost at port 8081")
})