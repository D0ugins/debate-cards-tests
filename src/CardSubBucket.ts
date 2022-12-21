import { BaseEntity, RedisContext, Repository } from '.';
import { SubBucketEntity } from './SubBucket';

class CardSubBucket implements BaseEntity<number> {
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
    return { sb: this.subBucket?.key.toString() };
  }
}

export class CardSubBucketRepository extends Repository<CardSubBucket, number> {
  protected prefix = 'C:';

  async reset(key: number) {
    this.context.transaction.hDel(this.prefix + key, 'sb');
    (await this.get(key)).subBucket = null;
  }

  createNew(key: number, subBucket: SubBucketEntity) {
    return new CardSubBucket(this.context, key, true, subBucket);
  }

  async fromRedis(obj: { sb: string }, key: number) {
    if (!obj.sb) return null;
    return new CardSubBucket(this.context, key, false, await this.context.subBucketRepository.get(+obj.sb));
  }
}
