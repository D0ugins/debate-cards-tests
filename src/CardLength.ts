import { BaseEntity, RedisContext, Repository } from '.';

class CardLength implements BaseEntity<number> {
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

  createNew(key: number, length: number) {
    return new CardLength(this.context, key, true, length);
  }

  async fromRedis(obj: { l: string; sb: string }, key: number) {
    return new CardLength(this.context, key, false, +obj.l);
  }
}
