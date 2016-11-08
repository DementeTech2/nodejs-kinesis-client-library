"use strict";
var Lease = (function () {
    function Lease(shardId, counter) {
        this.isFinished = false;
        this.shardId = shardId;
        this.leaseCounter = counter;
    }
    Lease.prototype.getCheckpoint = function (callback) {
        callback(null, this.checkpointedSequence);
    };
    Lease.prototype.update = function (callback) {
        this.leaseCounter += 1;
        this.expiredAt = Date.now() + (1000 * 15);
        process.send({ leaseItem: this });
        callback(null);
    };
    Lease.prototype.reserve = function (callback) {
        this.update(callback);
    };
    Lease.prototype.checkpoint = function (checkpointedSequence, callback) {
        // Skip redundant writes
        if (checkpointedSequence === this.checkpointedSequence) {
            return process.nextTick(callback);
        }
        this.checkpointedSequence = checkpointedSequence;
        this.update(callback);
    };
    Lease.prototype.markFinished = function (callback) {
        this.isFinished = true;
        this.update(callback);
    };
    return Lease;
}());
exports.Lease = Lease;
