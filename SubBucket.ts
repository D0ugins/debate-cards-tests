import { intersection } from 'lodash';
import { Entity, RedisContext, Repository, SHOULD_MATCH } from '.';

export type SubBucketEntity = InstanceType<typeof SubBucket>;
class SubBucket implements Entity<number> {
  constructor(
    public context: RedisContext,
    private _cards: Record<number, number>,
    private _matching: Record<number, number>,
    public updated: boolean = false,
    public readonly originalKey?: number,
  ) {}

  get members(): number[] {
    return Object.keys(this.cards).map(Number);
  }
  get size(): number {
    return Object.keys(this.cards).length;
  }

  get cards(): Readonly<Record<number, number>> {
    return this._cards;
  }
  get matching(): Readonly<Record<number, number>> {
    return this._matching;
  }

  doesBucketMatch(matches: number[]) {
    return SHOULD_MATCH(intersection(matches, this.members).length, this.size);
  }

  addCard(id: number, matches: number[]) {
    this.updated = true;
    delete this._matching[id];
    if (id in this.cards) return console.log(`Warning: ${id} already in bucket ${this.key}`);

    this._cards[id] = 1;
    for (const match in matches) {
      if (match in this.cards) {
        this._cards[id]++;
        this._cards[match]++;
      } else this._matching[match] = (this.matching[match] ?? 0) + 1;
    }
  }

  setMatches(id: number, matches: number[]) {
    this.updated = true;
    this._matching[id] = intersection(this.members, matches).length;
  }

  get key() {
    return Math.min(...Object.keys(this.cards).map(Number));
  }
  toRedis() {
    const obj: Record<string, string> = {};
    for (const cardId in this.cards) {
      obj[`c${cardId}`] = this.cards[cardId].toString();
    }
    for (const matchId in this.matching) {
      obj[`m${matchId}`] = this.matching[matchId].toString();
    }
    return obj;
  }
}

export class SubBucketRepository extends Repository<SubBucket, number> {
  protected prefix = 'SB:';

  create(root: number, matches: number[]) {
    const entity = new SubBucket(this.context, {}, {}, true);
    entity.addCard(root, matches);
    this.cache[root] = entity;
    return entity;
  }

  fromRedis(obj: Record<string, string>, key: number) {
    const cards: Record<string, number> = {};
    const matches: Record<string, number> = {};
    for (const key in obj) {
      const [type, value] = [key.charAt(0), +key.slice(1)];
      if (type === 'c') cards[value] = +obj[key];
      else if (type === 'm') matches[value] = +obj[key];
      else throw new Error(`Invalid key ${key} loading SubBucket`);
    }
    return new SubBucket(this.context, cards, matches, false, key);
  }
}
