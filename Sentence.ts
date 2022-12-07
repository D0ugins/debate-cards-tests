import { createHash } from 'crypto';
import { commandOptions } from 'redis';
import { Entity, RedisContext, Repository, SentenceMatch } from '.';

const paddedHex = (num: number, len: number) => num.toString(16).padStart(len, '0');
export class Sentence implements Entity<string> {
  public key: string;
  public subKey: string;
  public updated: boolean = false;
  constructor(public context: RedisContext, public sentence: string, private _matches: SentenceMatch[]) {
    const { bucket, subKey } = Sentence.createKey(sentence);
    this.key = bucket;
    this.subKey = subKey;
  }

  get matches(): readonly SentenceMatch[] {
    return this._matches;
  }

  addMatch(match: SentenceMatch) {
    this.updated = true;
    this._matches.push(match);
  }

  static createKey(sentence: string) {
    const hash = createHash('md5').update(sentence).digest('hex');
    // Uses top 20 bits as bucket, and next 40 as key
    // Will create 65k buckets, each containing a thousand or so sentences with the full dataset.
    return { bucket: hash.slice(0, 5), subKey: hash.slice(5, 15) };
  }

  toRedis() {
    return {
      matches: this.matches
        .map(({ cardId, index }) => this.subKey + paddedHex(cardId, 8) + paddedHex(index, 4))
        .join(''),
    };
  }
}

export class SentenceRepository extends Repository<Sentence, string> {
  protected prefix = 'S:';

  create(sentence: string, matches: SentenceMatch[]): Sentence {
    const entity = new Sentence(this.context, sentence, matches);
    this.cache[sentence] = entity;
    return entity;
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

  public async get(sentence: string): Promise<Sentence> {
    if (sentence in this.cache) return this.cache[sentence];
    const { bucket } = Sentence.createKey(sentence);
    const data = await this.context.client.get(commandOptions({ returnBuffers: true }), bucket);
    if (!data) return new Sentence(this.context, sentence, []); // TMP

    const entity = this.fromRedis({ data }, sentence);
    this.cache[sentence] = entity;
    return entity;
  }

  public async save(e: Sentence): Promise<unknown> {
    const data = e.matches.map(({ cardId, index }) => e.subKey + paddedHex(cardId, 8) + paddedHex(index, 4));
    return this.context.transaction.append(e.key, Buffer.from(data.join(''), 'hex'));
  }
}
