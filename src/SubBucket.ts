import { intersection, uniq } from 'lodash';
import { dedupQueue, DynamicKeyEntity, getMatching, RedisContext, Repository, SHOULD_MATCH, SHOULD_MERGE } from '.';
import { cardSet, shouldMerge } from './BucketSet';

export interface CardSet {
  size: number;
  members: readonly number[];
  matching: ReadonlyMap<number, number>;
}

export type SubBucketEntity = SubBucket;
class SubBucket implements DynamicKeyEntity<number>, CardSet {
  public key: number;
  constructor(
    public context: RedisContext,
    private _cards: Map<number, number>,
    private _matching: Map<number, number>,
    private _bucketSetId: number,
    public updated: boolean = false,
  ) {
    this.key = this.createKey();
  }

  public createKey(): number {
    return Math.min(...this.cards.keys());
  }

  public async propogateKey() {
    const newKey = this.createKey();
    if (newKey === this.key) return;

    const cards = await this.getCards();
    if (cards.length === 0) {
      console.log(`Deleting SubBucket ${this.key}`);
      await (await this.getBucketSet()).removeSubBucket(this);
      return this.context.subBucketRepository.delete(this.key);
    }

    cards.forEach((card) => (card.updated = true)); // Has reference to this, so key gets updated on save
    this.context.subBucketRepository.renameCacheKey(this.key, newKey);
    (await this.getBucketSet()).renameBucket(this.key, newKey);
    this.key = newKey;
  }

  get members(): number[] {
    return [...this.cards.keys()];
  }
  get size(): number {
    return this.cards.size;
  }

  get cards(): ReadonlyMap<number, number> {
    return this._cards;
  }
  get matching(): ReadonlyMap<number, number> {
    return this._matching;
  }

  get bucketSetId() {
    return this._bucketSetId;
  }

  set bucketSetId(value: number) {
    this.updated = true;
    this._bucketSetId = value;
  }

  async getCards() {
    return this.context.cardSubBucketRepository.getMany(this.members);
  }

  async getBucketSet() {
    return this.context.bucketSetRepository.get(this._bucketSetId);
  }

  doesBucketMatch(matches: number[]) {
    return SHOULD_MATCH(intersection(matches, this.members).length, this.size);
  }

  async addCard(id: number, matches: number[]) {
    this.updated = true;
    this._matching.delete(id);
    if (this.cards.has(id)) return console.log(`Warning: ${id} already in bucket ${this.key}`);

    this._cards.set(id, 1);
    for (const match of matches) {
      if (this.cards.has(match)) {
        this._cards.set(id, this.cards.get(id) + 1);
        this._cards.set(match, this.cards.get(match) + 1);
      } else this._matching.set(match, (this.matching.get(match) ?? 0) + 1);
    }
    this.context.cardSubBucketRepository.create(id, this);
    return this.propogateKey();
  }

  setMatches(id: number, matches: number[]) {
    this.updated = true;
    this._matching.set(id, intersection(this.members, matches).length);
  }

  async removeCard(id: number) {
    this.updated = true;
    this._cards.delete(id);
    await this.context.cardSubBucketRepository.reset(id);
    for (const match of (await getMatching(this.context, id)).matches) {
      const counter = this.cards.has(match) ? this._cards : this._matching;
      if (counter.get(match) <= 1) counter.delete(match);
      else counter.set(match, counter.get(match) - 1);
    }
    dedupQueue.add(id);
    return this.propogateKey();
  }

  async resolveUpdates(canidates: readonly number[]) {
    const thisBucketSet = await this.getBucketSet();
    const canidateSubBuckets = (await this.context.cardSubBucketRepository.getMany(canidates))
      .filter((s) => s?.subBucket)
      .map((s) => s.subBucket)
      .filter((subBucket) => subBucket.bucketSetId != this.bucketSetId);
    const canidateBucketSets = new Set(
      await Promise.all(canidateSubBuckets.map((subBucket) => subBucket.getBucketSet())),
    );

    if (!canidateBucketSets.size) return;
    const setSubBuckets = await thisBucketSet.getSubBuckets();
    for (const bucketSet of canidateBucketSets) {
      if (shouldMerge(setSubBuckets, await bucketSet.getSubBuckets())) {
        await thisBucketSet.merge(bucketSet);
        return this.resolveUpdates([...this.matching.keys()]); // Anything might match now
      }
    }
  }

  private async resolveRemoves() {
    for (const [cardId, count] of this.cards) {
      if (!SHOULD_MATCH(count, this.size)) {
        await this.removeCard(cardId);
        return this.resolveRemoves();
      }
    }
  }

  async resolve(updates: readonly number[], removed: boolean = false) {
    const thisBucketSet = await this.getBucketSet();

    await this.resolveRemoves();
    // Filter out things that we can garuntee were already counted as matching
    const dontMatch = updates
      .filter((id) => !this.cards.has(id))
      .filter((subBucketId) => !SHOULD_MERGE(this.matching.get(subBucketId) - 1, Infinity));
    const { matching: setMatching, size: setSize } = cardSet(await thisBucketSet.getSubBuckets());
    // If there are updated cards that might not have already matched, check if they didnt match before and do now
    const newMatches = dontMatch.filter(
      (id) => SHOULD_MERGE(setMatching.get(id), setSize) && !SHOULD_MERGE(setMatching.get(id) - 1, setSize),
    );

    await this.resolveUpdates(newMatches);
    await this.propogateKey();
    return thisBucketSet.resolve();
  }

  toRedis() {
    const obj: Record<string, string> = { sb: this._bucketSetId.toString() };
    for (const [cardId, count] of this.cards) obj[`c${cardId}`] = count.toString();
    for (const [matchId, count] of this.matching) obj[`m${matchId}`] = count.toString();
    return obj;
  }
}

export class SubBucketRepository extends Repository<SubBucket, number> {
  protected prefix = 'SB:';

  createNew(root: number, matches: number[]) {
    const cards = new Map([[root, 1]]);
    const matchMap = new Map(matches.map((match) => [match, 1]));
    const subBucket = new SubBucket(this.context, cards, matchMap, root, true);
    this.context.cardSubBucketRepository.create(root, subBucket);
    return subBucket;
  }

  save(e: SubBucket) {
    this.context.transaction.del(`${this.prefix}${e.key.toString()}`);
    return super.save(e);
  }

  fromRedis(obj: Record<string, string>, key: number) {
    const cards: Map<number, number> = new Map();
    const matches: Map<number, number> = new Map();
    let bucketSetId = key;
    for (const key in obj) {
      const [type, id] = [key.charAt(0), +key.slice(1)];
      const value = +obj[key];
      if (type === 'c') cards.set(id, value);
      else if (type === 'm') matches.set(id, value);
      else if (key === 'sb') bucketSetId = value;
      else throw new Error(`Invalid key ${key} loading SubBucket`);
    }
    return new SubBucket(this.context, cards, matches, bucketSetId, false);
  }
}
