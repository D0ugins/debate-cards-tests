import { createHash } from 'crypto';
import { commandOptions } from 'redis';
import { BaseEntity, RedisContext, Repository, SentenceMatch } from '.';

const paddedHex = (num: number, len: number) => num.toString(16).padStart(len, '0');
class Sentence implements BaseEntity<string, string> {
  public key: string;
  public subKey: string;
  private _additions: SentenceMatch[];
  constructor(
    public context: RedisContext,
    public sentence: string,
    private _matches: SentenceMatch[],
    public updated: boolean = false,
  ) {
    const { bucket, subKey } = Sentence.createKey(sentence);
    this.key = bucket;
    this.subKey = subKey;
    this._additions = [];
  }

  static createKey(sentence: string) {
    const hash = createHash('md5').update(sentence).digest('hex');
    // Uses top 20 bits as bucket, and next 40 as key
    // Will create 65k buckets, each containing a thousand or so sentences with the full dataset.
    return { bucket: hash.slice(0, 5), subKey: hash.slice(5, 15) };
  }

  get matches(): readonly SentenceMatch[] {
    return this._matches.concat(this._additions);
  }

  get additions(): readonly SentenceMatch[] {
    return this._additions;
  }

  addMatch(match: SentenceMatch) {
    this.updated = true;
    this._additions.push(match);
  }

  toRedis() {
    return this._additions
      .map(({ cardId, index }) => this.subKey + paddedHex(cardId, 8) + paddedHex(index, 4))
      .join('');
  }
}

export class SentenceRepository extends Repository<Sentence, string> {
  protected prefix = 'S:';

  createNew(sentence: string, matches: SentenceMatch[]): Sentence {
    return new Sentence(this.context, sentence, matches, true);
  }
  fromRedis(obj: { data: Buffer }, sentence: string) {
    const { data } = obj;
    if (!data) return new Sentence(this.context, sentence, []);
    if (data.length % 11 != 0) throw new Error(`Data for bucket ${sentence} has invalid length of ${data.length}`);

    const { subKey } = Sentence.createKey(sentence);
    const matches: SentenceMatch[] = [];
    for (let i = 0; i < data.length; i += 11) {
      if (data.readUIntBE(i, 5) != parseInt(subKey, 16)) continue;
      matches.push({ cardId: data.readUIntBE(i + 5, 4), index: data.readUIntBE(i + 9, 2) });
    }
    return new Sentence(this.context, sentence, matches);
  }

  protected async loadRedis(sentence: string) {
    const { bucket } = Sentence.createKey(sentence);
    this.context.client.watch(this.prefix + bucket);
    const data = await this.context.client.get(commandOptions({ returnBuffers: true }), this.prefix + bucket);

    const entity = data ? this.fromRedis({ data }, sentence) : new Sentence(this.context, sentence, []);
    this.cache[sentence] = entity;
    return entity;
  }

  public async save(e: Sentence): Promise<unknown> {
    e.updated = false;
    return this.context.transaction.append(this.prefix + e.key, Buffer.from(e.toRedis(), 'hex'));
  }
}
