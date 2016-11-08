import { EventEmitter } from 'events';
import { ClientConfig } from 'aws-sdk';
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
export declare class ConsumerCluster extends EventEmitter {
    private opts;
    private logger;
    private kinesis;
    private isShuttingDownFromError;
    private leases;
    private consumers;
    private consumerIds;
    private endpoints;
    constructor(pathToConsumer: string, opts: ConsumerClusterOpts);
    private init();
    private getKinesisEndpoint();
    serveHttp(port: string | number): void;
    private bindListeners();
    private fetchAvailableShard();
    private spawn(shardId, leaseCounter);
    private addConsumer(consumer);
    private killConsumerById(id, callback);
    private killAllConsumers(callback);
    private loopFetchAvailableShards();
    private logAndEmitError(err, desc?);
}
