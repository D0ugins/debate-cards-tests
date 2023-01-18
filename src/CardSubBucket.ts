import { BaseEntity, EntityManager, RedisContext } from './redis';
import { SubBucket } from './SubBucket';

class CardSubBucket implements BaseEntity<number> {
  constructor(
    public context: RedisContext,
    public key: number,
    public updated: boolean = false,
    private _subBucket: SubBucket,
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
export type { CardSubBucket };

export class CardSubBucketManager implements EntityManager<CardSubBucket, number> {
  public prefix = 'TEST:C:';
  constructor(public context: RedisContext) {}

  loadKeys(prefixedKeys: string[]): Promise<string[]> {
    return Promise.all(prefixedKeys.map((key) => this.context.client.hGet(key, 'sb')));
  }
  async parse(subBucketId: string, key: number): Promise<CardSubBucket> {
    return new CardSubBucket(this.context, key, false, await this.context.subBucketRepository.get(+subBucketId));
  }
  create(key: number, subBucket: SubBucket): CardSubBucket {
    return new CardSubBucket(this.context, key, true, subBucket);
  }
  save(entity: CardSubBucket): unknown {
    return this.context.transaction.hSet(this.prefix + entity.key, 'sb', entity.toRedis().sb);
  }
}
