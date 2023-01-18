import { BaseEntity, EntityManager, RedisContext } from './redis';

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
export type { CardLength };

export class CardLengthManager implements EntityManager<CardLength, number> {
  public prefix = 'TEST:C:';
  constructor(public context: RedisContext) {}

  loadKeys(prefixedKeys: string[]): Promise<string[]> {
    return Promise.all(prefixedKeys.map((key) => this.context.client.hGet(key, 'l')));
  }
  parse(length: string, key: number): CardLength {
    return new CardLength(this.context, key, false, +length);
  }
  create(key: number, length: number): CardLength {
    return new CardLength(this.context, key, true, length);
  }
  save(entity: CardLength): unknown {
    return this.context.transaction.hSet(this.prefix + entity.key, 'l', entity.toRedis().l);
  }
}
