import { ClientConfig, Kinesis, kinesis } from 'aws-sdk';
export declare const createKinesisClient: (conf: ClientConfig, endpoint?: string) => Kinesis;
export interface ListShardsCallback {
    (err: any, data?: kinesis.Shard[]): void;
}
export declare const listShards: (client: Kinesis, stream: string, callback: ListShardsCallback) => void;
