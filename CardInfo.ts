import { Entity, RedisContext, Repository } from '.';
import { SubBucket } from './SubBucket';

export class CardInfo implements Entity<number> {
  public key: number;
  public updated: boolean = false;
  constructor(
    public context: RedisContext,
    public originalKey: number,
    private _length: number,
    private _subBucket: SubBucket,
  ) {
    this.key = originalKey;
  }

  get length() {
    return this._length;
  }
  set length(value) {
    this.updated = true;
    this._length = value;
  }

  get subBucket() {
    return this._subBucket;
  }
  set subBucket(value) {
    this.updated = true;
    this._subBucket = value;
  }

  toRedis() {
    return { l: this.length.toString(), sb: this.subBucket.key.toString() };
  }
}

export class CardInfoRepository extends Repository<CardInfo, number> {
  protected prefix = 'C:';

  create(key: number, length: number, subBucket: SubBucket) {
    const entity = new CardInfo(this.context, key, length, subBucket);
    this.cache[key] = entity;
    return entity;
  }

  async fromRedis(obj: { l: string; sb: string }, key: number) {
    return new CardInfo(this.context, key, +obj.l, await this.context.subBucketRepository.get(+obj.sb));
  }
}
