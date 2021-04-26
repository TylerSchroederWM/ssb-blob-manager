var argv = require("minimist")(process.argv.slice(2));
var clientFactory = require("ssb-client");
var config = require("ssb-config");
var pull = require("pull-stream");
var Pushable = require("pull-pushable");
var many = require("./pull-many-v2");

// Change to 'true' to get debugging output
DEBUG = false

// Change to switch the default monitored channel
DEFAULT_CHANNEL_NAME = "logbook"

GRAPH_TYPE = "follow"
MAX_HOPS = 1

// using global variables to this degree is horrific, but nodejs has forced my hand
var client;
var allowedBlobs = [];
var idsInMainChannel = {};

function getBlob(blobId, reportStream=null) { 
	allowedBlobs.push(blobId);

	debug("Checking for blob with ID " + blobId);
	client.blobs.has(blobId, function(err, has) {
		if(err) {
			debug("[ERROR] ssb.blobs.has failed on the blob with ID " + blobId);
		}
		if(!err && !has) {
			debug("Wanting blob with ID " + blobId);
			client.blobs.want(blobId, () => {});
		}
		
		if(reportStream) {
			reportStream.push(true); // report that we've handled this blob
		}
	});
}

function requestBlobsFromMsg(msg) {
	if(msg.value && msg.value.content) {
		if(msg.value.content.mentions && Symbol.iterator in Object(msg.value.content.mentions)) {
			for(let mention of msg.value.content.mentions) {
				if(mention.type && mention.link) {
					if(typeof mention.link == "string") {
						getBlob(mention.link);
					}
					else {
						debug("Message has non-string mention.link value: " + JSON.stringify(mention.link));
					}
				}
			}
		}
	}
}

function downloadAllBlobsWithoutDoomedRaceConditionsSeeHowEasyThisIsScuttlebuttPeople(blobIds) {
	let nBlobIds = blobIds.length;
	
	if(nBlobIds == 0) {
		exit();
	}
	
	let actionTakenOnBlob = Pushable();
	for(let blobId of blobIds) {
		getBlob(blobId, actionTakenOnBlob);
	}
	
	pull(
		actionTakenOnBlob,
		pull.drain(function(result) {
			nBlobIds--;
			if(nBlobIds == 0) { // ensure that we don't exit before action has been taken on each relevant blob id
				actionTakenOnBlob.end();
				exit();
			}
		})
	);
}

function getRelevantProfilePictures() {
	let relevantIds = Object.keys(idsInMainChannel);
	let profilePictureStreams = []
	let profilePictureFound = {};
	for(let userId of relevantIds) {
		profilePictureFound[userId] = false;
		profilePictureStreams.push(createMetadataStream(client, userId));
	}
	let collectedStream = many(profilePictureStreams);

	if(relevantIds.length == 0) {
		exit();
	}

	pull(
		collectedStream,
		pull.filter(function(item) { // only let through messages that announce the most recent profile picture of a user in the channel stream
			let msg = item.data;
			if(!profilePictureFound[item.source] && msg.value.content.image) { // the source of an item is the userId of the stream that item came from
				if(typeof msg.value.content.image == "string") {
					// getBlob(msg.value.content.image);
					profilePictureFound[item.source] = true;
					return true;
				}
				else if(typeof msg.value.content.image == "object" && typeof msg.value.content.image.link == "string") {
					// getBlob(msg.value.content.image.link);
					profilePictureFound[item.source] = true;
					return true;
				}
				else {
					debug("Message has unknown msg.value.content.image value: " + JSON.stringify(msg.value.content.image));
				}
				return false;
			}
		}),
		pull.map(function(item) { // in the end, we just need the blob ids
			if(typeof item.data.value.content.image == "string") {
				return item.data.value.content.image;
			}
			else if(typeof item.data.value.content.image == "object" && typeof item.data.value.content.image.link == "string") {
				return item.data.value.content.image.link;
			}
		}),
		pull.collect(function(err, blobIds) {
			if(err) {
				console.log("[FATAL] Error when collecting profile picture blob IDs: " + err);
				process.exit(2);
			}
			
			downloadAllBlobsWithoutDoomedRaceConditionsSeeHowEasyThisIsScuttlebuttPeople(blobIds);
		})
	);
}

function deleteUnrelatedBlobs() {
	pull(
		client.blobs.ls(),
		pull.filter(function(id) {
			return !allowedBlobs.includes(id);
		}),
		pull.drain(function(id) {
			debug("Removing blob with ID " + id);
			client.blobs.rm(id, () => {}); // if you are having terrible race condition bugs where blob delete messages are printed but blbos are not deleted, this is why, and god help you
		}, function() {
			process.exit(0);
		})
	);
}

function exit() {
	if(argv.d) {
		debug("delete flag detected, cleansing unrelated blobs...");
		deleteUnrelatedBlobs();
	}
	else {
		process.exit(0);
	}
}

function debug(message) {
	if(DEBUG) {
		var timestamp = new Date();
		console.log("[channels-lib] [" + timestamp.toISOString() + "] " +  message);
	}
}

function createHashtagStream(client, channelName) {
	var search = client.search && client.search.query;
        if(!search) {
                console.log("[FATAL] ssb-search plugin must be installed to use handle_blobs");
                process.exit(1);
        }

        var query = search({
                query: channelName
        });

	query.streamName = "hashtag"; // mark the stream object so we can tell which stream an object came from

	return query;
}

function createChannelStream(client, channelName) {
	var query = client.query.read({
		query: [{
                       	"$filter": {
                               	value: {
                                       	content: {
                                               	channel: channelName
                                        	}
                        		}
       	        	}
                }],
               reverse: true
	});

	query.streamName = "channel"; // mark the stream object so we can tell which stream a message came from later

	return query;
}

function createMetadataStream(client, userId) {
	var query = client.query.read({
		query: [{
                       	"$filter": {
                               	value: {
                               		author: userId,
                                       	content: {
                                               	type: "about"
                                        	}
                        		}
       	        	}
                }]
	});

	query.streamName = userId; // mark the stream object so we can tell which stream a message came from later

	return query;
}

function getBlobsFromChannel(channelName, followedIds) {
	var channelTag = "#" + channelName;
	var hashtagStream = createHashtagStream(client, channelName);
	var channelStream = createChannelStream(client, channelName);
	var stream = many([hashtagStream, channelStream]);

	pull(
		stream, 
		pull.filter(function(msg) {
			return followedIds.includes(msg.data.value.author.toLowerCase());
		}), pull.filter(function(msg) {
			var hasHashtag =  msg.data.value.content && msg.data.value.content.text && typeof(msg.data.value.content.text) == "string" && msg.data.value.content.text.includes(channelTag);
			if(msg.source == "hashtag") {
				return hasHashtag;
			}
			else {
				return !hasHashtag; // prevents us from double-counting messages with both the hashtag and the channel header
			}
		}), 
		pull.drain(function(msg) {
			idsInMainChannel[msg.data.value.author] = true;
			requestBlobsFromMsg(msg.data);
		}, function() {
			getRelevantProfilePictures();
		})
	);
}

function main(channelName, hops=MAX_HOPS) {
	client.friends.hops({
		dunbar: Number.MAX_SAFE_INTEGER,
		max: hops
	}, function(error, friends) {
		if(error) {
			throw "Couldn't get a list of friends from scuttlebot:\n" + error;
		}

		var followedIds = Object.keys(friends).map(id => id.toLowerCase());
		getBlobsFromChannel(channelName, followedIds);
	});
}

function getConfig() {
	return {
		host: config.connections.incoming.net.external || "localhost",
		port: config.connections.incoming.net.port || 8008
	}
}

clientFactory(getConfig(), function(err, newClient) {
	client = newClient;
	if(err) {
		console.log("[ERROR] Failed to open ssb-client: " + err);
	}

	if(argv["delete"] || argv["del"]) {
		argv.d = true;
	}
	main(argv._[0] || DEFAULT_CHANNEL_NAME);
});
