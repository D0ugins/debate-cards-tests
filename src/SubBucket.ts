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
    cards.forEach((card) => (card.updated = true)); // Has reference to this, so key gets updated on save
    (await this.getBucketSet()).renameBucket(this.key, newKey);
    this.context.subBucketRepository.renameCacheKey(this.key, newKey);
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
    return Promise.all(this.members.map(async (cardId) => await this.context.cardSubBucketRepository.get(cardId)));
  }

  async getBucketSet() {
    return this.context.bucketSetRepository.get(this._bucketSetId);
  }

  doesBucketMatch(matches: number[]) {
    return SHOULD_MATCH(intersection(matches, this.members).length, this.size);
  }

  addCard(id: number, matches: number[]) {
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
  }

  setMatches(id: number, matches: number[]) {
    this.updated = true;
    this._matching.set(id, intersection(this.members, matches).length);
  }

  async removeCard(id: number) {
    this.updated = true;
    this._cards.delete(id);
    this.context.cardSubBucketRepository.delete(id);
    for (const match of await getMatching(this.context, id)) {
      const counter = this.cards.has(match) ? this._cards : this._matching;
      if (counter.get(match) <= 1) counter.delete(match);
      else counter.set(match, counter.get(match) - 1);
    }
    dedupQueue.add(id);
  }

  async resolve(updates: readonly number[], removed: boolean = false) {
    if (this.size === 0) return;
    for (const [cardId, count] of this.cards) {
      if (!SHOULD_MATCH(count, this.size)) {
        await this.removeCard(cardId);
        return this.resolve(updates, true);
      }
    }
    if (removed) await (await this.getBucketSet()).resolve();

    updates = updates.filter((id) => !this.cards.has(id));
    // Filter out things that we can garuntee were already counted as matching
    const dontMatch = updates.filter((subBucketId) => !SHOULD_MERGE(this.matching.get(subBucketId) - 1, Infinity));
    if (dontMatch.length === 0) return;

    const thisBucketSet = await this.getBucketSet();
    const { matching: setMatching, size: setSize } = cardSet(await thisBucketSet.getSubBuckets());
    // If there are updated cards that might not have already matched, check if they didnt match before and do now
    const newMatches = dontMatch.filter(
      (id) => SHOULD_MERGE(setMatching.get(id), setSize) && !SHOULD_MERGE(setMatching.get(id) - 1, setSize),
    );

    const newMatchSubBuckets = (await this.context.cardSubBucketRepository.getMany(newMatches))
      .filter((s) => s?.subBucket)
      .map((s) => s.subBucket)
      .filter((subBucket) => subBucket.bucketSetId != this.bucketSetId);
    const newMatchBucketSets = new Set(
      await Promise.all(newMatchSubBuckets.map((subBucket) => subBucket.getBucketSet())),
    );

    const setSubBuckets = await thisBucketSet.getSubBuckets();
    for (const bucketSet of newMatchBucketSets) {
      // if (this.key === 23274) debugger;
      if (shouldMerge(setSubBuckets, await bucketSet.getSubBuckets())) thisBucketSet.merge(bucketSet);
      // TODO restart with changed updates array since a new card might match entire bucketSet
    }
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
    return new SubBucket(this.context, cards, matchMap, root, true);
  }

  save(e: SubBucket) {
    this.context.transaction.del(`${this.prefix}${e.key.toString()}`);
    return super.save(e);
  }

  async propogateAllKeys() {
    return Promise.all([...this.cache.values()].map(async (subBucket) => (await subBucket).propogateKey()));
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
