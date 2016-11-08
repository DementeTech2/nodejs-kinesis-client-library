"use strict";
var async_1 = require('async');
var aws_sdk_1 = require('aws-sdk');
exports.createKinesisClient = function (conf, endpoint) {
    var instance = new aws_sdk_1.Kinesis(conf || {});
    if (endpoint) {
        instance.setEndpoint(endpoint);
    }
    return instance;
};
exports.listShards = function (client, stream, callback) {
    var shards = [];
    var foundAllShards = false;
    var startShardId;
    var next = function (done) {
        var params = {
            StreamName: stream,
            ExclusiveStartShardId: startShardId,
        };
        client.describeStream(params, function (err, data) {
            if (err) {
                return done(err);
            }
            if (!data.StreamDescription.HasMoreShards) {
                foundAllShards = true;
            }
            var lastShard = data.StreamDescription.Shards[data.StreamDescription.Shards.length - 1];
            startShardId = lastShard.ShardId;
            shards = shards.concat(data.StreamDescription.Shards);
            done();
        });
    };
    var test = function () { return !!foundAllShards; };
    var finish = function (err) {
        if (err) {
            return callback(err);
        }
        callback(null, shards);
    };
    async_1.doUntil(next, test, finish);
};
