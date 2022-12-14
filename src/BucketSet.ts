import { union } from 'lodash';
import { DynamicKeyEntity, RedisContext, Repository, SHOULD_MERGE } from '.';
import { CardSet, SubBucketEntity } from './SubBucket';

export function cardSet(subBuckets: readonly SubBucketEntity[]): CardSet {
  const matching = subBuckets.reduce((acc, cur) => {
    for (const [cardId, count] of cur.matching) acc.set(cardId, (acc.get(cardId) ?? 0) + count);
    return acc;
  }, new Map<number, number>());
  const members = union(...subBuckets.map((b) => b.members));
  return { size: members.length, members, matching };
}

function checkAdd(a: CardSet, b: CardSet) {
  const matches = b.members.filter((cardId) => SHOULD_MERGE(a.matching.get(cardId), a.size));
  return SHOULD_MERGE(matches.length, b.size);
}

export function shouldMerge(a: readonly SubBucketEntity[], b: readonly SubBucketEntity[]) {
  return checkAdd(cardSet(a), cardSet(b));
}

export type BucketSetEntity = BucketSet;
class BucketSet implements DynamicKeyEntity<number, string[]> {
  private _subBucketIds: Set<number>;
  public key: number;
  constructor(public context: RedisContext, subBucketIds: number[], public updated: boolean = false) {
    this._subBucketIds = new Set(subBucketIds);
    this.key = this.createKey();
  }

  public async propogateKey() {
    const newKey = this.createKey();
    if (this.key === newKey) return;
    const subBuckets = await this.getSubBuckets();
    subBuckets.forEach((subBucket) => (subBucket.bucketSetId = newKey)); // Have to explicitly set
    this.context.bucketSetRepository.renameCacheKey(this.key, newKey);
    this.key = newKey;
  }

  get subBucketIds(): readonly number[] {
    return [...this._subBucketIds];
  }
  createKey() {
    return Math.min(...this.subBucketIds);
  }

  async getSubBuckets(): Promise<readonly SubBucketEntity[]> {
    return Promise.all(this.subBucketIds.map((id) => this.context.subBucketRepository.get(id)));
  }

  async renameBucket(oldKey: number, newKey: number): Promise<void> {
    this.updated = true;
    this._subBucketIds.delete(oldKey);
    this._subBucketIds.add(newKey);
  }

  async merge(bucketSet: BucketSet) {
    this.updated = true;
    console.log(`Merging ${bucketSet.key} into ${this.key}`); // Debug
    this._subBucketIds = new Set([...this._subBucketIds, ...bucketSet.subBucketIds]);
    (await bucketSet.getSubBuckets()).forEach((subBucket) => (subBucket.bucketSetId = this.key));
  }

  async resolve(): Promise<void> {
    if (this._subBucketIds.size <= 1) return;
    const subBuckets = await this.getSubBuckets();
    for (const subBucket of subBuckets) {
      // Check if bucket would still get added if you tried now
      if (
        !shouldMerge(
          subBuckets.filter((b) => b !== subBucket),
          [subBucket],
        )
      ) {
        this.updated = true;
        subBucket.bucketSetId = subBucket.key;
        this._subBucketIds.delete(subBucket.key);
        await subBucket.resolveUpdates([...subBucket.matching.keys()]);
        console.log(`Removed ${subBucket.key} from ${this.createKey()}`); // Debug
        return this.resolve();
      }
    }
  }

  toRedis() {
    return this.subBucketIds.map(String);
  }
}

export class BucketSetRepository extends Repository<BucketSet, number> {
  protected prefix = 'BS:';

  createNew(key: number, subBucketIds: number[]) {
    return new BucketSet(this.context, subBucketIds, true);
  }

  protected async loadRedis(key: number): Promise<BucketSet> {
    await this.context.client.watch(this.prefix + key);
    const subBuckets = await this.context.client.sMembers(this.prefix + key);
    if (!subBuckets?.length) return this.fromRedis({ subBucketIds: [key] });
    return this.fromRedis({ subBucketIds: subBuckets.map(Number) });
  }

  async fromRedis({ subBucketIds }: { subBucketIds: number[] }) {
    return new BucketSet(this.context, subBucketIds, true);
  }

  async propogateAllKeys() {
    return Promise.all([...this.cache.values()].map(async (subBucket) => (await subBucket).propogateKey()));
  }

  async save(e: BucketSet) {
    e.updated = false;
    if (e.subBucketIds.length <= 1) return; // Dont bother saving sigle member bucket sets4
    this.context.transaction.del(this.prefix + e.key);
    return this.context.transaction.sAdd(this.prefix + e.key, e.toRedis());
  }
}
