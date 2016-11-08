"use strict";
var async_1 = require('async');
var Stream = (function () {
    function Stream(name, kinesis) {
        this.name = name;
        this.kinesis = kinesis;
    }
    Stream.prototype.exists = function (callback) {
        this.describe(function (err) {
            if (err && err.code === 'ResourceNotFoundException') {
                return callback(null, false);
            }
            if (err) {
                return callback(err);
            }
            callback(null, true);
        });
    };
    Stream.prototype.onActive = function (callback) {
        var _this = this;
        var state = { isActive: false, isDeleting: false };
        async_1.auto({
            isActive: function (done) {
                _this.isActive(function (err, isActive) {
                    state.isActive = isActive;
                    done(err);
                });
            },
            isDeleting: ['isActive', function (done) {
                    if (state.isActive) {
                        return done();
                    }
                    _this.isDeleting(function (err, isDeleting) {
                        state.isDeleting = isDeleting;
                        done(err);
                    });
                }],
        }, function (err) {
            if (err) {
                return callback(err);
            }
            if (state.isActive) {
                return callback();
            }
            if (state.isDeleting) {
                return callback(new Error('Stream is deleting'));
            }
            var isActive;
            async_1.doUntil(function (done) {
                _this.isActive(function (err, _isActive) {
                    if (err) {
                        return done(err);
                    }
                    isActive = _isActive;
                    done();
                });
            }, function () {
                return isActive;
            }, callback);
        });
    };
    Stream.prototype.isActive = function (callback) {
        this.hasStatus('ACTIVE', callback);
    };
    Stream.prototype.isDeleting = function (callback) {
        this.hasStatus('DELETING', callback);
    };
    Stream.prototype.hasStatus = function (status, callback) {
        this.describe(function (err, description) {
            if (err) {
                return callback(err);
            }
            var isActive = description.StreamDescription.StreamStatus === status;
            callback(null, isActive);
        });
    };
    Stream.prototype.describe = function (callback) {
        this.kinesis.describeStream({ StreamName: this.name }, callback);
    };
    return Stream;
}());
exports.Stream = Stream;
