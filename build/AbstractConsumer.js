"use strict";
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var async_1 = require('async');
var bunyan_1 = require('bunyan');
var underscore_1 = require('underscore');
var kinesis_1 = require('./lib/aws/kinesis');
var config_1 = require('./lib/config');
var Lease_1 = require('./lib/models/Lease');
// Stream consumer, meant to be extended.
var AbstractConsumer = (function () {
    function AbstractConsumer(opts) {
        var _this = this;
        this.hasStartedExit = false;
        this.opts = opts;
        this.timeBetweenReads = opts.timeBetweenReads || AbstractConsumer.DEFAULT_TIME_BETWEEN_READS;
        this.resetThroughputErrorDelay();
        if (!this.opts.startingIteratorType) {
            this.opts.startingIteratorType = AbstractConsumer.DEFAULT_SHARD_ITERATOR_TYPE;
        }
        this.kinesis = kinesis_1.createKinesisClient(this.opts.awsConfig, this.opts.kinesisEndpoint);
        process.on('message', function (msg) {
            if (msg === config_1.default.shutdownMessage) {
                _this.exit(null);
            }
        });
        this.logger = bunyan_1.createLogger({
            name: 'KinesisConsumer',
            level: opts.logLevel,
            streamName: opts.streamName,
            shardId: opts.shardId,
        });
        this.init();
        if (!this.opts.shardId) {
            this.exit(new Error('Cannot spawn a consumer without a shard ID'));
        }
    }
    // Called before record processing starts. This method may be implemented by the child.
    // If it is implemented, the callback must be called for processing to begin.
    AbstractConsumer.prototype.initialize = function (callback) {
        this.log('No initialize method defined, skipping');
        callback();
    };
    // Process a batch of records. This method, or processResponse, must be implemented by the child.
    AbstractConsumer.prototype.processRecords = function (records, callback) {
        throw new Error('processRecords must be defined by the consumer class');
    };
    // Process raw kinesis response.  Override it to get access to the MillisBehindLatest field.
    AbstractConsumer.prototype.processResponse = function (response, callback) {
        this.processRecords(response.Records, callback);
    };
    // Called before a consumer exits. This method may be implemented by the child.
    AbstractConsumer.prototype.shutdown = function (callback) {
        this.log('No shutdown method defined, skipping');
        callback();
    };
    AbstractConsumer.prototype.init = function () {
        var _this = this;
        this.setupLease();
        async_1.series([
            this.initialize.bind(this),
            this.reserveLease.bind(this),
            function (done) {
                _this.lease.getCheckpoint(function (err, checkpoint) {
                    if (err) {
                        return done(err);
                    }
                    _this.log({ checkpoint: checkpoint }, 'Got starting checkpoint');
                    _this.maxSequenceNumber = checkpoint;
                    _this.updateShardIterator(checkpoint, done);
                });
            },
        ], function (err) {
            if (err) {
                return _this.exit(err);
            }
            _this.loopGetRecords();
            _this.loopReserveLease();
        });
    };
    AbstractConsumer.prototype.log = function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i - 0] = arguments[_i];
        }
        this.logger.info.apply(this.logger, args);
    };
    // Continuously fetch records from the stream.
    AbstractConsumer.prototype.loopGetRecords = function () {
        var _this = this;
        var timeBetweenReads = this.timeBetweenReads;
        this.log('Starting getRecords loop');
        async_1.forever(function (done) {
            var gotRecordsAt = Date.now();
            _this.getRecords(function (err) {
                if (err) {
                    return done(err);
                }
                var timeToWait = Math.max(0, timeBetweenReads - (Date.now() - gotRecordsAt));
                if (timeToWait > 0) {
                    setTimeout(done, timeToWait);
                }
                else {
                    done();
                }
            });
        }, function (err) {
            _this.exit(err);
        });
    };
    // Continuously update this consumer's lease reservation.
    AbstractConsumer.prototype.loopReserveLease = function () {
        var _this = this;
        this.log('Starting reserveLease loop');
        async_1.forever(function (done) {
            setTimeout(_this.reserveLease.bind(_this, done), 5000);
        }, function (err) {
            _this.exit(err);
        });
    };
    // Setup the initial lease reservation state.
    AbstractConsumer.prototype.setupLease = function () {
        var id = this.opts.shardId;
        var leaseCounter = this.opts.leaseCounter || null;
        this.log({ leaseCounter: leaseCounter }, 'Setting up lease');
        this.lease = new Lease_1.Lease(id, leaseCounter);
    };
    // Update the lease in the network database.
    AbstractConsumer.prototype.reserveLease = function (callback) {
        this.logger.debug('Reserving lease');
        this.lease.reserve(callback);
    };
    // Mark the consumer's shard as finished, then exit.
    AbstractConsumer.prototype.markFinished = function () {
        var _this = this;
        this.log('Marking shard as finished');
        this.lease.markFinished(function (err) { return _this.exit(err); });
    };
    // Get records from the stream and wait for them to be processed.
    AbstractConsumer.prototype.getRecords = function (callback) {
        var _this = this;
        var getRecordsParams = { ShardIterator: this.nextShardIterator };
        if (this.opts.numRecords && this.opts.numRecords > 0) {
            getRecordsParams = { ShardIterator: this.nextShardIterator, Limit: this.opts.numRecords };
        }
        this.kinesis.getRecords(getRecordsParams, function (err, data) {
            // Handle known errors
            if (err && err.code === 'ExpiredIteratorException') {
                _this.log('Shard iterator expired, updating before next getRecords call');
                return _this.updateShardIterator(_this.maxSequenceNumber, function (err) {
                    if (err) {
                        return callback(err);
                    }
                    _this.getRecords(callback);
                });
            }
            if (err && err.code === 'ProvisionedThroughputExceededException') {
                _this.log('Provisioned throughput exceeded, pausing before next getRecords call', {
                    delay: _this.throughputErrorDelay,
                });
                return setTimeout(function () {
                    _this.increaseThroughputErrorDelay();
                    _this.getRecords(callback);
                }, _this.throughputErrorDelay);
            }
            _this.resetThroughputErrorDelay();
            // We have an error but don't know how to handle it
            if (err) {
                return callback(err);
            }
            // Save this in case we need to checkpoint it in a future request before we get more records
            if (data.NextShardIterator != null) {
                _this.nextShardIterator = data.NextShardIterator;
            }
            // We have processed all the data from a closed stream
            if (data.NextShardIterator == null && (!data.Records || data.Records.length === 0)) {
                _this.log({ data: data }, 'Marking shard as finished');
                return _this.markFinished();
            }
            var lastSequenceNumber = underscore_1.pluck(data.Records, 'SequenceNumber').pop();
            _this.maxSequenceNumber = lastSequenceNumber || _this.maxSequenceNumber;
            _this.wrappedProcessResponse(data, callback);
        });
    };
    // Wrap the child's processResponse method to handle checkpointing.
    AbstractConsumer.prototype.wrappedProcessResponse = function (data, callback) {
        var _this = this;
        this.processResponse(data, function (err, checkpointSequenceNumber) {
            if (err) {
                return callback(err);
            }
            // Don't checkpoint
            if (!checkpointSequenceNumber) {
                return callback();
            }
            // We haven't actually gotten any records so there is nothing to checkpoint
            if (!_this.maxSequenceNumber) {
                return callback();
            }
            // Default case to checkpoint the latest sequence number
            if (checkpointSequenceNumber === true) {
                checkpointSequenceNumber = _this.maxSequenceNumber;
            }
            _this.lease.checkpoint(checkpointSequenceNumber, callback);
        });
    };
    // Get a new shard iterator from Kinesis.
    AbstractConsumer.prototype.updateShardIterator = function (sequenceNumber, callback) {
        var _this = this;
        var type;
        if (sequenceNumber) {
            type = AbstractConsumer.ShardIteratorTypes.AFTER_SEQUENCE_NUMBER;
        }
        else {
            type = this.opts.startingIteratorType;
        }
        this.log({ iteratorType: type, sequenceNumber: sequenceNumber }, 'Updating shard iterator');
        var params = {
            StreamName: this.opts.streamName,
            ShardId: this.opts.shardId,
            ShardIteratorType: type,
            StartingSequenceNumber: sequenceNumber,
        };
        this.log(params, 'Params for getShardIterator');
        this.kinesis.getShardIterator(params, function (e, data) {
            if (e) {
                return callback(e);
            }
            _this.log(data, 'Updated shard iterator');
            _this.nextShardIterator = data.ShardIterator;
            callback();
        });
    };
    // Exit the consumer with its optional shutdown process.
    AbstractConsumer.prototype.exit = function (err) {
        var _this = this;
        if (this.hasStartedExit) {
            return;
        }
        this.hasStartedExit = true;
        if (err) {
            this.logger.error(err);
        }
        setTimeout(function () {
            _this.logger.error('Forcing exit based on shutdown timeout');
            // Exiting with 1 because the shutdown process took too long
            process.exit(1);
        }, 30000);
        this.log('Starting shutdown');
        this.shutdown(function () {
            var exitCode = err == null ? 0 : 1;
            process.exit(exitCode);
        });
    };
    AbstractConsumer.prototype.increaseThroughputErrorDelay = function () {
        this.throughputErrorDelay = this.throughputErrorDelay * 2;
    };
    AbstractConsumer.prototype.resetThroughputErrorDelay = function () {
        this.throughputErrorDelay = this.timeBetweenReads;
    };
    // Create a child consumer.
    AbstractConsumer.extend = function (args) {
        var opts = JSON.parse(process.env.CONSUMER_INSTANCE_OPTS);
        var Consumer = (function (_super) {
            __extends(Consumer, _super);
            function Consumer() {
                var _this = this;
                _super.call(this, opts);
                AbstractConsumer.ABSTRACT_METHODS
                    .filter(function (method) { return args[method]; })
                    .forEach(function (method) { return _this[method] = args[method]; });
            }
            return Consumer;
        }(AbstractConsumer));
        new Consumer();
    };
    AbstractConsumer.ABSTRACT_METHODS = ['processRecords', 'initialize', 'shutdown'];
    AbstractConsumer.DEFAULT_SHARD_ITERATOR_TYPE = 'TRIM_HORIZON';
    AbstractConsumer.DEFAULT_TIME_BETWEEN_READS = 1000;
    AbstractConsumer.ShardIteratorTypes = {
        AT_SEQUENCE_NUMBER: 'AT_SEQUENCE_NUMBER',
        AFTER_SEQUENCE_NUMBER: 'AFTER_SEQUENCE_NUMBER',
        TRIM_HORIZON: 'TRIM_HORIZON',
        LATEST: 'LATEST',
    };
    return AbstractConsumer;
}());
exports.AbstractConsumer = AbstractConsumer;
