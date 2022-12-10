import { intersection } from 'lodash';
import { dedupQueue, Entity, getMatching, RedisContext, Repository, SHOULD_MATCH } from '.';

export type SubBucketEntity = InstanceType<typeof SubBucket>;
class SubBucket implements Entity<number> {
  constructor(
    public context: RedisContext,
    private _cards: Map<number, number>,
    private _matching: Map<number, number>,
    public updated: boolean = false,
    public readonly originalKey?: number,
  ) {}

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

  async resolve() {
    for (const [cardId, count] of this.cards) {
      if (!SHOULD_MATCH(count, this.size)) {
        await this.removeCard(cardId);
        return this.resolve();
      }
    }
  }

  get key() {
    return Math.min(...this.cards.keys());
  }
  toRedis() {
    const obj: Record<string, string> = {};
    for (const [cardId, count] of this.cards) obj[`c${cardId}`] = count.toString();
    for (const [matchId, count] of this.matching) obj[`m${matchId}`] = count.toString();
    return obj;
  }
}

export class SubBucketRepository extends Repository<SubBucket, number> {
  protected prefix = 'SB:';

  create(root: number, matches: number[]) {
    const entity = new SubBucket(this.context, new Map(), new Map(), true);
    entity.addCard(root, matches);
    this.cache[root] = entity;
    return entity;
  }

  save(e: SubBucket) {
    if (e.originalKey) this.context.transaction.del(`${this.prefix}${e.originalKey.toString()}`);
    super.save(e);
  }

  fromRedis(obj: Record<string, string>, key: number) {
    const cards: Map<number, number> = new Map();
    const matches: Map<number, number> = new Map();
    for (const key in obj) {
      const [type, id] = [key.charAt(0), +key.slice(1)];
      if (type === 'c') cards.set(id, +obj[key]);
      else if (type === 'm') matches.set(id, +obj[key]);
      else throw new Error(`Invalid key ${key} loading SubBucket`);
    }
    return new SubBucket(this.context, cards, matches, false, key);
  }
}
