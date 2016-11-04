
export class Lease {
  public shardId: string;
  public leaseCounter: number;
  public expiredAt: number;
  public isFinished: boolean = false;
  private checkpointedSequence: string;

  constructor(shardId: string, counter: number) {
    this.shardId = shardId;
    this.leaseCounter = counter;
  }

  public getCheckpoint(callback: (err: any, checkpoint?: string) => void) {
      callback(null, this.checkpointedSequence);
  }

  private update(callback: (err: any) => void) {

    this.leaseCounter += 1;
    this.expiredAt = Date.now() + (1000 * 15);

    process.send({ leaseItem: this });

    callback(null);
  }

  public reserve(callback: (err: any) => void) {
    this.update(callback)
  }

  public checkpoint(checkpointedSequence: string, callback: (err: any) => void) {
    // Skip redundant writes
    if (checkpointedSequence === this.checkpointedSequence) {
      return process.nextTick(callback)
    }

    this.checkpointedSequence = checkpointedSequence;

    this.update(callback)
  }

  public markFinished(callback: (err: any) => void) {
    this.isFinished = true;
    this.update(callback)
  }
}
