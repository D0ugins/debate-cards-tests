import { Entity, RedisContext, Repository } from '.';
import { SubBucketEntity } from './SubBucket';

class CardInfo implements Entity<number> {
  constructor(
    public context: RedisContext,
    public key: number,
    public updated: boolean = false,
    private _length: number,
    private _subBucket: SubBucketEntity,
  ) {}

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
    return { l: this.length?.toString(), sb: this.subBucket?.key.toString() };
  }
}

export class CardInfoRepository extends Repository<CardInfo, number> {
  protected prefix = 'C:';

  create(key: number, length: number, subBucket: SubBucketEntity) {
    const entity = new CardInfo(this.context, key, true, length, subBucket);

    if (length && subBucket) this.cache[key] = entity;
    return entity;
  }

  async fromRedis(obj: { l: string; sb: string }, key: number) {
    return new CardInfo(this.context, key, false, +obj.l, await this.context.subBucketRepository.get(+obj.sb));
  }
}
