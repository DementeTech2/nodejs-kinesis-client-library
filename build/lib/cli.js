"use strict";
var path_1 = require('path');
var child_process_1 = require('child_process');
var async_1 = require('async');
var bunyan_1 = require('bunyan');
var minimist = require('minimist');
var config_1 = require('./config');
var ConsumerCluster_1 = require('../ConsumerCluster');
var args = minimist(process.argv.slice(2));
var logger = bunyan_1.createLogger({
    name: 'KinesisClusterCLI',
    level: args['log-level']
});
if (args.help) {
    console.log("\n    Usage:\n\n    --help  (Display this message)\n\n    Required flags:\n    --consumer [Path to consumer file]\n    --stream [Kinesis stream name]\n\n    Optional flags:\n    --start-at [Starting iterator type] (\"trim_horizon\" or \"latest\", defaults to \"trim_horizon\")\n    --capacity.[read|write] [Throughput] (DynamoDB throughput for *new* tables, defaults to 10 for each)\n    --aws.[option] [Option value]  (e.g. --aws.region us-west-2)\n    --http [port]  (Start HTTP server, port defaults to $PORT)\n    --log-level [level] (Logging verbosity, uses Bunyan log levels)\n    --kinesis-endpoint (Use a custom endpoint for the Kinesis service)\n    --local-kinesis (Use a local implementation of Kinesis, defaults to false)\n    --local-kinesis-port (Port to access local Kinesis on, defaults to 4567)\n    --local-kinesis-no-start (Assume a local Kinesis server is already running, defaults to false)\n    --num-records (Maximum number of records to get in each Kinesis query, defaults to the Kinesis maximum of 10000)\n    --time-between-reads (Time to wait between fetching records in milliseconds, defaults to 1000)\n  ");
    process.exit();
}
var consumer = path_1.resolve(process.cwd(), args.consumer || '');
var opts = {
    streamName: args.stream,
    awsConfig: args.aws,
    startingIteratorType: args['start-at'],
    capacity: args.capacity,
    kinesisEndpoint: args['kinesis-endpoint'],
    localKinesis: !!args['local-kinesis'],
    localKinesisPort: args['local-kinesis-port'],
    logLevel: args['log-level'],
    numRecords: args['num-records'],
    timeBetweenReads: args['time-between-reads'],
};
logger.info('Consumer app path:', consumer);
var clusterOpts = Object.keys(opts).reduce(function (memo, key) {
    if (opts[key] !== undefined) {
        memo[key] = opts[key];
    }
    return memo;
}, {});
logger.info({ options: clusterOpts }, 'Cluster options');
async_1.auto({
    localKinesis: function (done) {
        if (!opts.localKinesis) {
            return done();
        }
        if (args['local-kinesis-no-start']) {
            return done();
        }
        var port = args['local-kinesis-port'] || config_1.default.localKinesisEndpoint.port;
        var proc = child_process_1.spawn('./node_modules/.bin/kinesalite', [
            '--port', port.toString()
        ], {
            cwd: path_1.resolve(__dirname, '../..')
        });
        proc.on('error', function (err) {
            logger.error(err, 'Error in local Kinesis');
            process.exit(1);
        });
        var timer = setTimeout(function () {
            done(new Error('Local Kinesis took too long to start'));
        }, 5000);
        var output = '';
        proc.stdout.on('data', function (chunk) {
            output += chunk;
            if (output.indexOf('Listening') === -1) {
                return;
            }
            done();
            done = function () { };
            clearTimeout(timer);
        });
    },
    cluster: ['localKinesis', function (done) {
            logger.info('Launching cluster');
            var cluster;
            try {
                cluster = new ConsumerCluster_1.ConsumerCluster(consumer, opts);
            }
            catch (e) {
                logger.error('Error launching cluster');
                logger.error(e);
                process.exit(1);
            }
            //logger.info('Spawned cluster %s', cluster.cluster.id)
            if (args.http) {
                var port = void 0;
                if (typeof args.http === 'number') {
                    port = args.http;
                }
                else {
                    port = process.env.PORT;
                }
                logger.info('Spawning HTTP server on port %d', port);
                cluster.serveHttp(port);
            }
        }]
}, function (err) {
    if (err) {
        logger.error(err);
        process.exit(1);
    }
});
