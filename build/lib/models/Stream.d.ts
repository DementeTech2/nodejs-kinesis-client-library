import { Kinesis } from 'aws-sdk';
export declare class Stream {
    private name;
    private kinesis;
    constructor(name: string, kinesis: Kinesis);
    exists(callback: any): void;
    onActive(callback: any): void;
    isActive(callback: any): void;
    isDeleting(callback: any): void;
    private hasStatus(status, callback);
    private describe(callback);
}
