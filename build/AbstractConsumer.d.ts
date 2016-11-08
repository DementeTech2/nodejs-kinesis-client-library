import { kinesis } from 'aws-sdk';
import { Logger } from 'bunyan';
export interface ProcessRecordsCallback {
    (err: any, checkpointSequenceNumber?: Boolean | string): void;
}
export interface ConsumerExtension {
    processResponse?: (request: kinesis.GetRecordsResult, callback: ProcessRecordsCallback) => void;
    processRecords?: (records: kinesis.Record[], callback: ProcessRecordsCallback) => void;
    initialize?: (callback: (err?: any) => void) => void;
    shutdown?: (callback: (err?: any) => void) => void;
}
export declare class AbstractConsumer {
    static ABSTRACT_METHODS: string[];
    static DEFAULT_SHARD_ITERATOR_TYPE: string;
    static DEFAULT_TIME_BETWEEN_READS: number;
    static ShardIteratorTypes: {
        AT_SEQUENCE_NUMBER: string;
        AFTER_SEQUENCE_NUMBER: string;
        TRIM_HORIZON: string;
        LATEST: string;
    };
    logger: Logger;
    private opts;
    private lease;
    private maxSequenceNumber;
    private kinesis;
    private nextShardIterator;
    private hasStartedExit;
    private timeBetweenReads;
    private throughputErrorDelay;
    initialize(callback: (err?: Error) => void): void;
    processRecords(records: kinesis.Record[], callback: ProcessRecordsCallback): void;
    processResponse(response: kinesis.GetRecordsResult, callback: ProcessRecordsCallback): void;
    shutdown(callback: (err?: Error) => void): void;
    constructor(opts: any);
    private init();
    log(...args: any[]): void;
    private loopGetRecords();
    private loopReserveLease();
    private setupLease();
    private reserveLease(callback);
    private markFinished();
    private getRecords(callback);
    private wrappedProcessResponse(data, callback);
    private updateShardIterator(sequenceNumber, callback);
    private exit(err);
    private increaseThroughputErrorDelay();
    private resetThroughputErrorDelay();
    static extend(args: ConsumerExtension): void;
}
