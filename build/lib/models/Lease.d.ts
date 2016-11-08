export declare class Lease {
    shardId: string;
    leaseCounter: number;
    expiredAt: number;
    isFinished: boolean;
    private checkpointedSequence;
    constructor(shardId: string, counter: number);
    getCheckpoint(callback: (err: any, checkpoint?: string) => void): void;
    private update(callback);
    reserve(callback: (err: any) => void): void;
    checkpoint(checkpointedSequence: string, callback: (err: any) => void): void;
    markFinished(callback: (err: any) => void): void;
}
