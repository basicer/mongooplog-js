var co = require('co');
var Timestamp = require('mongodb').Timestamp;
var MongoClient = require('mongodb').MongoClient;
var moment = require('moment');
var optionator = require('optionator')({
	prepend: 'Usage: mongooplog-js [options]',
	append: 'Version 1.0.0',
	options: [
	{
		option: 'help',
		type: 'Boolean',
		description: 'displays help'
	},
	{
		option: 'from',
		alias: ['h', 'host'],
		type: 'String',
		description: 'connection string to read from',
		required: true
	},
	{
		option: 'to',
		alias: 't',
		type: 'String',
		description: 'connection string to write to',
		default: 'mongodb://localhost:27017/test'
	},
	{
		option: 'follow',
		alias: 'f',
		type: 'Boolean',
		description: 'follow oplog after syncing'
	},
	{
		option: 'duration',
		alias: 's',
		type: 'Int',
		description: 'duration to replay oplog, in seconds',
		default: String(4 * 24 * 60 * 60)
	}]
});

var options = optionator.parseArgv(process.argv);

if ( (process.argv.length < 3) || options.help ) {
	console.log(optionator.generateHelp());
	process.exit(1);
}

let opsToProcess = [];

function sleep(time) {
	return new Promise(function(res, rej) {
		setTimeout(res, time);
	});
}

let lastTimestamp;
let written = 0;
let read = 0;
let done = false;
let writer = co(function*() {
	let dstClient = new MongoClient();
	let dstDb = yield dstClient.connect(options.to);
	let adb = dstDb.db('admin');
	while ( !done || opsToProcess.length > 0 ) {
		yield sleep(10);
		if ( opsToProcess.length == 0 ) continue;
		try {
			let myopts = opsToProcess.slice(0, 100);
			let r = yield adb.command({'applyOps': myopts});
			opsToProcess.splice(0, myopts.length);
			written += myopts.length;
		} catch ( e ) {
			console.log(e);
			opsToProcess.splice(0, e.results.length);
			written += e.results.length;
		}
	}

});


setInterval(function() {
	var num = lastTimestamp.getHighBits();
	var qs = opsToProcess.length;
	console.log(`\nStatus| Written: ${written}, LastTS: ${moment.unix(num).fromNow()}, queueSize: ${qs}, AR: ${((qs+written)/read*100)|0}%`);

}, 5000);

let reader = co(function*() {


	let srcClient = new MongoClient();


	let srcDbOrig = yield srcClient.connect(options.from);

	let srcDb = srcDbOrig.db('local');
	let time = options.duration;
	let ts = new Timestamp(0, Math.floor(new Date().getTime() / 1000) - time);
	let opts = {oplogReplay: true, numerOfRetries: -1};

	if ( options.follow ) {
		opts.tailable = true;
		opts.awaitdata = true;
	}

	let cursor = srcDb.collection('oplog.rs').find(
		{ts: {$gte: ts} },
		opts
	);

	if ( !options.follow ) {
		cursor = curor.sort({$natural: -1});
	}

	while ( !cursor.isClosed() ) {
		if ( opsToProcess.length > 10000 ) {
			yield sleep(100);
			continue;
		}
		let c = yield cursor.nextObject();
		++read;
		lastTimestamp = c.ts;
		if ( c.ns != 'coco.level.sessions' ) continue;
		opsToProcess.push(c);

	}

	done = true;

});


Promise.all([reader,writer]).then(function() {
	process.exit();
}, function(e) {
	console.log("ERROR", e)
	process.exit(1);
});
