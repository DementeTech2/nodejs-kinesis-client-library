export declare const create: (port: number | string, callback: () => Stringable) => void;
export interface Stringable {
    toString: () => string;
}
