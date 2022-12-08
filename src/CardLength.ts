import { Entity, RedisContext, Repository } from '.';
import { SubBucketEntity } from './SubBucket';

class CardLength implements Entity<number> {
  constructor(
    public context: RedisContext,
    public key: number,
    public updated: boolean = false,
    private _length: number,
  ) {}

  get length() {
    return this._length;
  }
  set length(value) {
    this.updated = true;
    this._length = value;
  }

  toRedis() {
    return { l: this.length.toString() };
  }
}

export class CardLengthRepository extends Repository<CardLength, number> {
  protected prefix = 'C:';

  create(key: number, length: number) {
    const entity = new CardLength(this.context, key, true, length);

    this.cache[key] = entity;
    return entity;
  }

  async fromRedis(obj: { l: string; sb: string }, key: number) {
    return new CardLength(this.context, key, false, +obj.l);
  }
}
