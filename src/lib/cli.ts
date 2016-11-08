import {resolve} from 'path'
import {spawn} from 'child_process'

import {auto} from 'async'
import {ClientConfig} from 'aws-sdk'
import {createLogger} from 'bunyan'
import * as minimist from 'minimist'

import config from './config'
import {ConsumerCluster} from '../ConsumerCluster'

interface KinesisCliArgs extends minimist.ParsedArgs {
  help: Boolean
  consumer: string
  stream: string
  'start-at'?: string
  capacity?: {
    read?: number
    write?: number
  }
  aws?: ClientConfig
  http?: (Boolean | number)
  'kinesis-endpoint'?: string
  'local-kinesis'?: Boolean
  'local-kinesis-port'?: string
  'local-kinesis-no-start'?: Boolean
  'log-level': string
  'num-records'?: number
  'time-between-reads'?: number
}

const args = <KinesisCliArgs>minimist(process.argv.slice(2));
const logger = createLogger({
  name: 'KinesisClusterCLI',
  level: args['log-level']
});

if (args.help) {
  console.log(`
    Usage:

    --help  (Display this message)

    Required flags:
    --consumer [Path to consumer file]
    --stream [Kinesis stream name]

    Optional flags:
    --start-at [Starting iterator type] ("trim_horizon" or "latest", defaults to "trim_horizon")
    --capacity.[read|write] [Throughput] (DynamoDB throughput for *new* tables, defaults to 10 for each)
    --aws.[option] [Option value]  (e.g. --aws.region us-west-2)
    --http [port]  (Start HTTP server, port defaults to $PORT)
    --log-level [level] (Logging verbosity, uses Bunyan log levels)
    --kinesis-endpoint (Use a custom endpoint for the Kinesis service)
    --local-kinesis (Use a local implementation of Kinesis, defaults to false)
    --local-kinesis-port (Port to access local Kinesis on, defaults to 4567)
    --local-kinesis-no-start (Assume a local Kinesis server is already running, defaults to false)
    --num-records (Maximum number of records to get in each Kinesis query, defaults to the Kinesis maximum of 10000)
    --time-between-reads (Time to wait between fetching records in milliseconds, defaults to 1000)
  `);
  process.exit();
}

const consumer = resolve(process.cwd(), args.consumer || '');
const opts = {
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
const clusterOpts = Object.keys(opts).reduce((memo, key) => {
  if (opts[key] !== undefined) {
    memo[key] = opts[key]
  }

  return memo
}, {});
logger.info({ options: clusterOpts }, 'Cluster options');

auto({
  localKinesis: done => {
    if (!opts.localKinesis) {
      return done()
    }
    if (args['local-kinesis-no-start']) {
      return done()
    }

    const port = args['local-kinesis-port'] || config.localKinesisEndpoint.port;

    const proc = spawn('./node_modules/.bin/kinesalite', [
      '--port', port.toString()
    ], {
        cwd: resolve(__dirname, '../..')
      });

    proc.on('error', err => {
      logger.error(err, 'Error in local Kinesis');
      process.exit(1);
    });

    const timer = setTimeout(() => {
      done(new Error('Local Kinesis took too long to start'))
    }, 5000);

    let output = '';
    proc.stdout.on('data', chunk => {
      output += chunk;
      if (output.indexOf('Listening') === -1) {
        return
      }

      done();
      done = () => { /* Don't call twice */ };
      clearTimeout(timer);
    })
  },
  cluster: ['localKinesis', done => {
    logger.info('Launching cluster');
    let cluster;
    try {
      cluster = new ConsumerCluster(consumer, opts);
    } catch (e) {
      logger.error('Error launching cluster');
      logger.error(e);
      process.exit(1);
    }

    //logger.info('Spawned cluster %s', cluster.cluster.id)

    if (args.http) {
      let port;
      if (typeof args.http === 'number') {
        port = args.http
      } else {
        port = process.env.PORT;
      }

      logger.info('Spawning HTTP server on port %d', port);
      cluster.serveHttp(port);
    }
  }]
}, err => {
  if (err) {
    logger.error(err);
    process.exit(1);
  }
});
