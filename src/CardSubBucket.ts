import { Entity, RedisContext, Repository } from '.';
import { SubBucketEntity } from './SubBucket';

class CardSubBucket implements Entity<number> {
  constructor(
    public context: RedisContext,
    public key: number,
    public updated: boolean = false,
    private _subBucket: SubBucketEntity,
  ) {}

  get subBucket() {
    return this._subBucket;
  }
  set subBucket(value) {
    this.updated = true;
    this._subBucket = value;
  }

  toRedis() {
    return { sb: this.subBucket.key.toString() };
  }
}

export class CardSubBucketRepository extends Repository<CardSubBucket, number> {
  protected prefix = 'C:';

  create(key: number, length: number, subBucket: SubBucketEntity) {
    const entity = new CardSubBucket(this.context, key, true, subBucket);

    if (length && subBucket) this.cache[key] = entity;
    return entity;
  }

  async fromRedis(obj: { l: string; sb: string }, key: number) {
    return new CardSubBucket(this.context, key, false, await this.context.subBucketRepository.get(+obj.sb));
  }
}
