"use strict";
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var events_1 = require('events');
var cluster_1 = require('cluster');
var path_1 = require('path');
var url_1 = require('url');
var underscore_1 = require('underscore');
var async_1 = require('async');
var bunyan_1 = require('bunyan');
var config_1 = require('./lib/config');
var kinesis_1 = require('./lib/aws/kinesis');
var Stream_1 = require('./lib/models/Stream');
// Cluster of consumers.
var ConsumerCluster = (function (_super) {
    __extends(ConsumerCluster, _super);
    function ConsumerCluster(pathToConsumer, opts) {
        _super.call(this);
        this.isShuttingDownFromError = false;
        this.leases = [];
        this.consumers = {};
        this.consumerIds = [];
        this.running = false;
        this.opts = opts;
        this.logger = bunyan_1.createLogger({
            name: 'KinesisCluster',
            level: opts.logLevel,
        });
        cluster_1.setupMaster({
            exec: pathToConsumer,
            silent: true,
        });
        this.endpoints = {
            kinesis: this.getKinesisEndpoint()
        };
        this.kinesis = kinesis_1.createKinesisClient(this.opts.awsConfig, this.endpoints.kinesis);
        this.init();
    }
    ConsumerCluster.prototype.init = function () {
        var _this = this;
        this.running = true;
        this.isShuttingDownFromError = false;
        async_1.auto({
            createStream: function (done) {
                var streamName = _this.opts.streamName;
                var streamModel = new Stream_1.Stream(streamName, _this.kinesis);
                streamModel.exists(function (err, exists) {
                    if (err) {
                        return done(err);
                    }
                    if (exists) {
                        return done();
                    }
                    done(new Error('The stream "' + _this.opts.streamName + '" does not exist'));
                });
            },
        }, function (err) {
            if (err) {
                return _this.logAndEmitError(err, 'Error ensuring Dynamo table exists');
            }
            _this.bindListeners();
            _this.loopFetchAvailableShards();
        });
    };
    ConsumerCluster.prototype.getKinesisEndpoint = function () {
        var isLocal = this.opts.localKinesis;
        var port = this.opts.localKinesisPort;
        var customEndpoint = this.opts.kinesisEndpoint;
        var endpoint = null;
        if (isLocal) {
            var endpointConfig = config_1.default.localKinesisEndpoint;
            if (port) {
                endpointConfig.port = port;
            }
            endpoint = url_1.format(endpointConfig);
        }
        else if (customEndpoint) {
            endpoint = customEndpoint;
        }
        return endpoint;
    };
    ConsumerCluster.prototype.bindListeners = function () {
        var _this = this;
        this.on('availableShard', function (shardId, leaseCounter) {
            // Stops accepting consumers, since the cluster will be reset based one an error
            if (_this.isShuttingDownFromError) {
                return;
            }
            _this.spawn(shardId, leaseCounter);
        });
    };
    // Fetch data about unleased shards.
    ConsumerCluster.prototype.fetchAvailableShard = function () {
        var _this = this;
        // Hack around typescript
        var _asyncResults = {};
        async_1.parallel({
            allShardIds: function (done) {
                kinesis_1.listShards(_this.kinesis, _this.opts.streamName, function (err, shards) {
                    if (err) {
                        return done(err);
                    }
                    _asyncResults.shards = shards;
                    done();
                });
            },
            leases: function (done) {
                _asyncResults.leases = _this.leases;
                done();
            },
        }, function (err) {
            if (err) {
                return _this.logAndEmitError(err, 'Error fetching available shards');
            }
            var shards = _asyncResults.shards, leases = _asyncResults.leases;
            var leaseItems = leases;
            var finishedShardIds = leaseItems.filter(function (lease) {
                return lease.isFinished;
            }).map(function (lease) {
                return lease.shardId;
            });
            var allUnfinishedShardIds = shards.filter(function (shard) {
                return finishedShardIds.indexOf(shard.ShardId) === -1;
            });
            var leasedShardIds = leaseItems.map(function (item) {
                return item.shardId;
            });
            var newShardIds = [];
            allUnfinishedShardIds.filter(function (shard) {
                // skip already leased shards
                if (leasedShardIds.indexOf(shard.ShardId) >= 0) {
                    return false;
                }
                // skip if parent shard is not finished (split case)
                if (shard.ParentShardId && !(finishedShardIds.indexOf(shard.ParentShardId) >= 0)) {
                    _this.logger.info({ ParentShardId: shard.ParentShardId, ShardId: shard.ShardId }, 'Ignoring shard because ParentShardId is not finished');
                    return false;
                }
                // skip if adjacent parent shard is not finished (merge case)
                if (shard.AdjacentParentShardId && !(finishedShardIds.indexOf(shard.AdjacentParentShardId) >= 0)) {
                    _this.logger.info({ AdjacentParentShardId: shard.AdjacentParentShardId, ShardId: shard.ShardId }, 'Ignoring shard because AdjacentParentShardId is not finished');
                    return false;
                }
                newShardIds.push(shard);
                return true;
            });
            // If there are shards theat have not been leased, pick one
            if (newShardIds.length > 0) {
                _this.logger.info({ newShardIds: newShardIds }, 'Unleased shards available');
                return _this.emit('availableShard', newShardIds[0].ShardId, null);
            }
            // Try to find the first expired lease
            var currentLease;
            for (var i = 0; i < leaseItems.length; i++) {
                currentLease = leaseItems[i];
                if (currentLease.expiredAt > Date.now()) {
                    continue;
                }
                if (currentLease.isFinished) {
                    continue;
                }
                var shardId = currentLease.shardId;
                var leaseCounter = currentLease.leaseCounter;
                _this.logger.info({ shardId: shardId, leaseCounter: leaseCounter }, 'Found available shard');
                return _this.emit('availableShard', shardId, leaseCounter);
            }
        });
    };
    // Create a new consumer processes.
    ConsumerCluster.prototype.spawn = function (shardId, leaseCounter) {
        this.logger.info({ shardId: shardId, leaseCounter: leaseCounter }, 'Spawning consumer');
        var consumerOpts = {
            awsConfig: this.opts.awsConfig,
            streamName: this.opts.streamName,
            startingIteratorType: (this.opts.startingIteratorType || '').toUpperCase(),
            shardId: shardId,
            leaseCounter: leaseCounter,
            kinesisEndpoint: this.endpoints.kinesis,
            numRecords: this.opts.numRecords,
            timeBetweenReads: this.opts.timeBetweenReads,
            logLevel: this.opts.logLevel,
        };
        var env = {
            CONSUMER_INSTANCE_OPTS: JSON.stringify(consumerOpts),
            CONSUMER_SUPER_CLASS_PATH: path_1.join(__dirname, 'AbstractConsumer.js'),
        };
        var consumer = cluster_1.fork(env);
        consumer.opts = consumerOpts;
        consumer.process.stdout.pipe(process.stdout);
        consumer.process.stderr.pipe(process.stderr);
        this.addConsumer(consumer);
    };
    // Add a consumer to the cluster.
    ConsumerCluster.prototype.addConsumer = function (consumer) {
        var _this = this;
        this.consumerIds.push(consumer.id);
        this.consumers[consumer.id] = consumer;
        consumer.once('exit', function (code) {
            var logMethod = code === 0 ? 'info' : 'error';
            _this.logger[logMethod]({ shardId: consumer.opts.shardId, exitCode: code }, 'Consumer exited');
            _this.consumerIds = underscore_1.without(_this.consumerIds, consumer.id);
            delete _this.consumers[consumer.id];
        });
        consumer.once('message', function (item) {
            var leaseItem = item.leaseItem;
            var checkLease = underscore_1.find(_this.leases, function (lease) { return lease.shardId == leaseItem.shardId; });
            if (checkLease) {
                _this.leases[underscore_1.indexOf(_this.leases, checkLease)] = leaseItem;
            }
            else {
                _this.leases.push(leaseItem);
            }
        });
    };
    // Kill a specific consumer in the cluster.
    ConsumerCluster.prototype.killConsumerById = function (id, callback) {
        var _this = this;
        this.logger.info({ id: id }, 'Killing consumer');
        var callbackWasCalled = false;
        var wrappedCallback = function (err) {
            if (callbackWasCalled) {
                return;
            }
            callbackWasCalled = true;
            callback(err);
        };
        // Force kill the consumer in 40 seconds, giving enough time for the consumer's shutdown
        // process to finish
        var timer = setTimeout(function () {
            if (_this.consumers[id]) {
                _this.consumers[id].kill();
            }
            wrappedCallback(new Error('Consumer did not exit in time'));
        }, 40000);
        this.consumers[id].once('exit', function (code) {
            clearTimeout(timer);
            var err = null;
            if (code > 0) {
                err = new Error('Consumer process exited with code: ' + code);
            }
            wrappedCallback(err);
        });
        this.consumers[id].send(config_1.default.shutdownMessage);
    };
    ConsumerCluster.prototype.killAllConsumers = function (callback) {
        this.logger.info('Killing all consumers');
        async_1.each(this.consumerIds, this.killConsumerById.bind(this), callback);
    };
    // Continuously fetch data about the rest of the network.
    ConsumerCluster.prototype.loopFetchAvailableShards = function () {
        var _this = this;
        this.logger.info('Starting external network fetch loop');
        var fetchThenWait = function (done) {
            if (_this.isShuttingDownFromError) {
                return done(new Error('Is shutting down'));
            }
            _this.fetchAvailableShard();
            _this.loopTimer = setTimeout(done, 5000);
        };
        var handleError = function (err) {
            _this.logAndEmitError(err, 'Error fetching external network data');
        };
        async_1.forever(fetchThenWait, handleError);
    };
    ConsumerCluster.prototype.shutDown = function () {
        this.logAndEmitError(new Error('Killed by parent'));
    };
    ConsumerCluster.prototype.getStatus = function () {
        return {
            status: this.running,
            countProcess: this.consumerIds.length,
            stream: this.opts.streamName
        };
    };
    ConsumerCluster.prototype.logAndEmitError = function (err, desc) {
        var _this = this;
        this.logger.error(desc);
        this.logger.error(err);
        // Only start the shutdown process once
        if (this.isShuttingDownFromError) {
            return;
        }
        this.running = false;
        this.isShuttingDownFromError = true;
        // Kill all consumers and then emit an error so that the cluster can be re-spawned
        this.killAllConsumers(function (killErr) {
            if (killErr) {
                _this.logger.error(killErr);
            }
            // Emit the original error that started the shutdown process
            _this.emit('error', err);
        });
    };
    return ConsumerCluster;
}(events_1.EventEmitter));
exports.ConsumerCluster = ConsumerCluster;
