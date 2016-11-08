import {EventEmitter} from 'events';
import {Worker, fork, setupMaster} from 'cluster';
import {join} from 'path';
import {format as formatUrl} from 'url';

import {without, find, indexOf} from 'underscore';
import {auto, parallel, each, forever} from 'async';
import {ClientConfig, Kinesis, kinesis} from 'aws-sdk';
import {Logger, createLogger} from 'bunyan';

import config from './lib/config';
import {listShards, createKinesisClient} from './lib/aws/kinesis';
import {Lease} from './lib/models/Lease';
import {Stream} from './lib/models/Stream';
import {create as createServer} from './lib/server';

declare var clearTimeout;

interface ClusterWorkerWithOpts extends Worker {
  opts: { shardId: string }
}

interface AWSEndpoints {
  kinesis: string;
}

export interface ConsumerClusterOpts {
  streamName: string;
  awsConfig: ClientConfig;
  kinesisEndpoint?: string;
  localKinesis: Boolean;
  localKinesisPort?: string;
  startingIteratorType?: string;
  logLevel?: string;
  numRecords?: number;
  timeBetweenReads?: number;
}


// Cluster of consumers.
export class ConsumerCluster extends EventEmitter {
  private opts: ConsumerClusterOpts;
  private logger: Logger;
  private kinesis: Kinesis;
  private isShuttingDownFromError = false;
  private leases:Lease[] = [];
  private consumers = {};
  private consumerIds = [];
  private endpoints: AWSEndpoints;

  constructor(pathToConsumer: string, opts: ConsumerClusterOpts) {
    super();
    this.opts = opts;

    this.logger = createLogger({
      name: 'KinesisCluster',
      level: opts.logLevel,
    });

    setupMaster({
      exec: pathToConsumer,
      silent: true,
    });

    this.endpoints = {
      kinesis: this.getKinesisEndpoint()
    };

    this.kinesis = createKinesisClient(this.opts.awsConfig, this.endpoints.kinesis);
    this.init();
  }

  private init() {
    auto({
      createStream: done => {
        const streamName = this.opts.streamName;
        const streamModel = new Stream(streamName, this.kinesis);

        streamModel.exists((err, exists) => {
          if (err) {
            return done(err);
          }

          if (exists) {
            return done();
          }

          done(new Error('The stream "'+this.opts.streamName+'" does not exist'));
        })
      },
    }, err => {
      if (err) {
        return this.logAndEmitError(err, 'Error ensuring Dynamo table exists');
      }

      this.bindListeners();
      this.loopFetchAvailableShards();
    })
  }

  private getKinesisEndpoint() {
    const isLocal = this.opts.localKinesis;
    const port = this.opts.localKinesisPort;
    const customEndpoint = this.opts.kinesisEndpoint;
    let endpoint = null;

    if (isLocal) {
      const endpointConfig = config.localKinesisEndpoint;
      if (port) {
        endpointConfig.port = port;
      }
      endpoint = formatUrl(endpointConfig);
    } else if (customEndpoint) {
      endpoint = customEndpoint;
    }

    return endpoint
  }

  // Run an HTTP server. Useful as a health check.
  public serveHttp(port: string | number) {
    this.logger.debug('Starting HTTP server on port %s', port);
    createServer(port, () => this.consumerIds.length)
  }

  private bindListeners() {

    this.on('availableShard', (shardId, leaseCounter) => {
      // Stops accepting consumers, since the cluster will be reset based one an error
      if (this.isShuttingDownFromError) {
        return
      }

      this.spawn(shardId, leaseCounter)
    });

  }

  // Fetch data about unleased shards.
  private fetchAvailableShard() {
    // Hack around typescript
    var _asyncResults = <{
      shards: kinesis.Shard[];
      leases: Lease[];
    }>{};

    parallel({
      allShardIds: done => {
        listShards(this.kinesis, this.opts.streamName, (err, shards) => {
          if (err) {
            return done(err);
          }

          _asyncResults.shards = shards;
          done();
        })
      },
      leases: done => {
        _asyncResults.leases = this.leases;
        done();
      },
    }, err => {
      if (err) {
        return this.logAndEmitError(err, 'Error fetching available shards');
      }

      const {shards, leases} = _asyncResults;
      const leaseItems = leases;

      const finishedShardIds = leaseItems.filter((lease:Lease) => {
        return lease.isFinished;
      }).map(lease => {
        return <string>lease.shardId;
      });

      const allUnfinishedShardIds = shards.filter(shard => {
        return finishedShardIds.indexOf(shard.ShardId) === -1;
      });

      const leasedShardIds: Array<string> = leaseItems.map((item:Lease) => {
        return item.shardId;
      });

      let newShardIds = [];
      allUnfinishedShardIds.filter(shard => {

        // skip already leased shards
        if (leasedShardIds.indexOf(shard.ShardId) >= 0) {
          return false;
        }

        // skip if parent shard is not finished (split case)
        if (shard.ParentShardId && !(finishedShardIds.indexOf(shard.ParentShardId) >= 0)) {
          this.logger.info({ ParentShardId: shard.ParentShardId, ShardId: shard.ShardId },
            'Ignoring shard because ParentShardId is not finished');
          return false;
        }

        // skip if adjacent parent shard is not finished (merge case)
        if (shard.AdjacentParentShardId && !(finishedShardIds.indexOf(shard.AdjacentParentShardId) >= 0)) {
          this.logger.info({ AdjacentParentShardId: shard.AdjacentParentShardId, ShardId: shard.ShardId },
            'Ignoring shard because AdjacentParentShardId is not finished');
          return false;
        }

        newShardIds.push(shard);
        return true;
      });

      // If there are shards theat have not been leased, pick one
      if (newShardIds.length > 0) {
        this.logger.info({ newShardIds: newShardIds }, 'Unleased shards available');
        return this.emit('availableShard', newShardIds[0].ShardId, null);
      }

      // Try to find the first expired lease
      let currentLease;
      for (var i = 0; i < leaseItems.length; i++) {
        currentLease = leaseItems[i];
        if (currentLease.expiredAt > Date.now()) {
          continue
        }
        if (currentLease.isFinished ) {
          continue
        }

        let shardId = currentLease.shardId;
        let leaseCounter = currentLease.leaseCounter;
        this.logger.info({ shardId: shardId, leaseCounter: leaseCounter }, 'Found available shard');
        return this.emit('availableShard', shardId, leaseCounter)
      }
    })
  }

  // Create a new consumer processes.
  private spawn(shardId: string, leaseCounter: number) {
    this.logger.info({ shardId: shardId, leaseCounter: leaseCounter }, 'Spawning consumer');
    const consumerOpts = {
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

    const env = {
      CONSUMER_INSTANCE_OPTS: JSON.stringify(consumerOpts),
      CONSUMER_SUPER_CLASS_PATH: join(__dirname, 'AbstractConsumer.js'),
    };

    const consumer = <ClusterWorkerWithOpts>fork(env);
    consumer.opts = consumerOpts;
    consumer.process.stdout.pipe(process.stdout);
    consumer.process.stderr.pipe(process.stderr);
    this.addConsumer(consumer);
  }

  // Add a consumer to the cluster.
  private addConsumer(consumer: ClusterWorkerWithOpts) {
    this.consumerIds.push(consumer.id);
    this.consumers[consumer.id] = consumer;

    consumer.once('exit', code => {
      const logMethod = code === 0 ? 'info' : 'error';
      this.logger[logMethod]({ shardId: consumer.opts.shardId, exitCode: code }, 'Consumer exited');

      this.consumerIds = without(this.consumerIds, consumer.id);
      delete this.consumers[consumer.id];
    });

    consumer.once('message', item => {
      let leaseItem = item.leaseItem;
      let checkLease = find(this.leases, (lease) => lease.shardId == leaseItem.shardId );

      if ( checkLease ) {
        this.leases[indexOf(this.leases, checkLease)] = leaseItem;
      } else {
        this.leases.push(leaseItem);
      }
    });
  }


  // Kill a specific consumer in the cluster.
  private killConsumerById(id: number, callback: (err: any) => void) {
    this.logger.info({ id: id }, 'Killing consumer');

    let callbackWasCalled = false;
    const wrappedCallback = (err: any) => {
      if (callbackWasCalled) {
        return;
      }
      callbackWasCalled = true;
      callback(err);
    };

    // Force kill the consumer in 40 seconds, giving enough time for the consumer's shutdown
    // process to finish
    const timer = setTimeout(() => {
      if (this.consumers[id]) {
        this.consumers[id].kill();
      }
      wrappedCallback(new Error('Consumer did not exit in time'));
    }, 40000);

    this.consumers[id].once('exit', code => {
      clearTimeout(timer);
      let err = null;
      if (code > 0) {
        err = new Error('Consumer process exited with code: ' + code);
      }
      wrappedCallback(err);
    });

    this.consumers[id].send(config.shutdownMessage);
  }

  private killAllConsumers(callback: (err: any) => void) {
    this.logger.info('Killing all consumers');
    each(this.consumerIds, this.killConsumerById.bind(this), callback);
  }

  // Continuously fetch data about the rest of the network.
  private loopFetchAvailableShards() {
    this.logger.info('Starting external network fetch loop');

    const fetchThenWait = done => {
        if(this.isShuttingDownFromError) {
          return done(new Error('Is shutting down'));
        }

        this.fetchAvailableShard();
        setTimeout(done, 5000);
    };

    const handleError = err => {
      this.logAndEmitError(err, 'Error fetching external network data')
    };

    forever(fetchThenWait, handleError)
  }

  private logAndEmitError(err: Error, desc?: string) {
    this.logger.error(desc);
    this.logger.error(err);

    // Only start the shutdown process once
    if (this.isShuttingDownFromError) {
      return;
    }

    this.isShuttingDownFromError = true;

    // Kill all consumers and then emit an error so that the cluster can be re-spawned
    this.killAllConsumers((killErr?: Error) => {
      if (killErr) {
        this.logger.error(killErr);
      }

      // Emit the original error that started the shutdown process
      this.emit('error', err);
    })
  }
}
