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
    private running;
    private loopTimer;
    constructor(pathToConsumer: string, opts: ConsumerClusterOpts);
    init(): void;
    private getKinesisEndpoint();
    private bindListeners();
    private fetchAvailableShard();
    private spawn(shardId, leaseCounter);
    private addConsumer(consumer);
    private killConsumerById(id, callback);
    private killAllConsumers(callback);
    private loopFetchAvailableShards();
    shutDown(): void;
    getStatus(): {
        status: boolean;
        countProcess: number;
        stream: string;
    };
    private logAndEmitError(err, desc?);
}
