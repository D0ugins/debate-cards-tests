import { createClient } from 'redis';
import { SubBucketEntity, SubBucketRepository } from './SubBucket';
import { SentenceRepository } from './Sentence';
import { CardInfoRepository } from './CardInfo';
import { PrismaClient } from '@prisma/client';
import { groupBy, map, max, maxBy, min, uniq } from 'lodash';

export const db = new PrismaClient();
export const redis = createClient({
  // url: 'redis://redis:6379',
  password: 'password',
  isolationPoolOptions: {
    max: 10,
  },
});

export const EDGE_TOLERANCE = 1;
export const INSIDE_TOLERANCE = 2;
export const SENTENCE_REGEX = /([.?!])+(?=\d*\s+[A-Z])/;

export const SHOULD_MATCH = (matching: number, total: number) => matching / total > 0.5;
export const SHOULD_MERGE = (matching: number, total: number) => matching > 5 || matching / total >= 0.2;

export const getSentences = (text: string, cutoff = 20): string[] | undefined => {
  return text
    ?.split(SENTENCE_REGEX)
    .map((el) => el.replace(/[^A-Z]/gi, '').toLowerCase())
    .filter((el: string) => el.length >= cutoff);
};
export type SentenceMatch = { cardId: number; index: number };
export type RedisTransaction = ReturnType<typeof redis['multi']>;

export interface Entity<K extends string | number> {
  context: RedisContext;
  updated: boolean;
  originalKey?: K;
  key: K;
  toRedis(): Partial<Record<string, string>>;
}

export abstract class Repository<E extends Entity<K>, K extends string | number> {
  protected cache: Record<K, E>;
  protected abstract prefix: string;

  constructor(protected context: RedisContext) {
    this.cache = {} as Record<K, E>;
  }

  protected getKeys(keys: K[]): Promise<Record<string, string>[]> {
    this.context.client.watch(keys.filter((key) => !(key in this.cache)).map(String));
    return Promise.all(keys.map((key) => this.context.client.hGetAll(`${this.prefix}${key}`)));
  }
  protected async getKey(key: K) {
    return (await this.getKeys([key]))[0];
  }

  abstract create(...args: any[]): E;
  abstract fromRedis(obj: Record<string, string | Buffer>, key: K): E | Promise<E>;

  public async get(key: K): Promise<E> {
    if (key in this.cache) return this.cache[key];
    const obj = await this.getKey(key);
    const entity = await this.fromRedis(obj, key);
    this.cache[key] = entity;
    return entity;
  }

  public save(e: E): unknown {
    e.updated = false;
    const key = e.key.toString();
    if (e.originalKey) this.context.transaction.del(`${this.prefix}${e.originalKey.toString()}`);
    return Object.entries(e.toRedis()).map(
      ([subKey, value]) => value && this.context.transaction.hSet(`${this.prefix}${key}`, subKey, value),
    );
  }

  public saveAll() {
    for (const key in this.cache) {
      const entity = this.cache[key];
      if (entity.updated) this.save(entity);
    }
  }
}

// Kind of inefficient way to manage context, but performance effect is only a few microseconds

const loadSentences = async (id: number) => {
  const card = await db.evidence.findUnique({ where: { id }, select: { id: true, fulltext: true } });
  if (!card?.fulltext) throw new Error(`Card with id ${id} does not exist`);

  return getSentences(card.fulltext);
};
type MatchInfo = { cardLen: number; indexes: number[] };
type MatchPair = { cardId: number; a: MatchInfo; b: MatchInfo };

const checkMatch = (a: MatchInfo, b: MatchInfo) => {
  const insideMatch =
    a.cardLen > 3 && a.cardLen - ((max(a.indexes) ?? 0) + 1 - (min(a.indexes) ?? 0)) <= INSIDE_TOLERANCE; // If the enterity of A matches
  return (
    insideMatch || ((min(a.indexes) ?? 0) <= EDGE_TOLERANCE && b.cardLen - (max(b.indexes) ?? 0) <= EDGE_TOLERANCE)
  ); // If matches the start of A and the end of B
};
// Check in both orders
const isMatch = (info: MatchPair) => checkMatch(info.a, info.b) || checkMatch(info.b, info.a);

async function getMatching(context: RedisContext, cardId: number) {
  const sentences = (await loadSentences(cardId)) ?? [];
  /* 
    Watch for change in sentences, prevents new card being added that this card should match and it being missed
    Will have a decent amonunt of false positives due to bucketing of sentences
    Probability a card completes without a retry is roughly
    (1 - ((sentencesPerCard * concurrentDeduplication) / numBuckets))^sentencesPerCard
    sentencesPerCard seems to be roughly 30
    With 25 concurrent deduplications happening, and 2^20 buckets, probability is around 0.98
  */
  const matches = await Promise.all(sentences.map((s) => context.sentenceRepository.get(s)));
  const matchIndexes = map(matches, 'matches')
    .flatMap((sentence, i) => sentence.map(({ cardId, index }) => ({ cardId, aIndex: i, bIndex: index })))
    .filter((match) => match.cardId !== cardId);

  const matchInfo: MatchPair[] = await Promise.all(
    map(groupBy(matchIndexes, 'cardId'), async (match, cardId) => ({
      cardId: +cardId,
      a: { indexes: map(match, 'aIndex'), cardLen: matches.length },
      b: { indexes: map(match, 'bIndex'), cardLen: (await context.cardInfoRepository.get(+cardId)).length },
    })),
  );
  return map(matchInfo.filter(isMatch), 'cardId');
}

async function processCard(context: RedisContext, id: number, sentences: string[]) {
  const matchedCards = await getMatching(context, id);

  const bucketCandidates = uniq(
    await Promise.all(matchedCards.map(async (id) => (await context.cardInfoRepository.get(id)).subBucket)),
  );
  bucketCandidates.forEach((b) => b.setMatches(id, matchedCards));
  const matchedBuckets = bucketCandidates.filter((b) => b.doesBucketMatch(matchedCards));

  let addBucket: SubBucketEntity;
  if (!matchedBuckets) {
    addBucket = context.subBucketRepository.create(id, matchedCards);
  } else {
    addBucket = maxBy(matchedBuckets, (b) => b.size) ?? matchedBuckets[0];
    addBucket.addCard(id, matchedCards);
  }

  context.cardInfoRepository.create(id, sentences.length, addBucket);
  const sentenceEntities = await Promise.all(sentences.map((s) => context.sentenceRepository.get(s)));
  sentenceEntities.forEach((entity, i) => entity.addMatch({ cardId: id, index: i }));
}

export class RedisContext {
  sentenceRepository: SentenceRepository;
  cardInfoRepository: CardInfoRepository;
  subBucketRepository: SubBucketRepository;
  transaction: RedisTransaction;

  constructor(public client: typeof redis) {
    this.transaction = client.multi();
    this.sentenceRepository = new SentenceRepository(this);
    this.cardInfoRepository = new CardInfoRepository(this);
    this.subBucketRepository = new SubBucketRepository(this);
  }

  finish(save: boolean = true) {
    if (save) {
      this.subBucketRepository.saveAll();
      this.cardInfoRepository.saveAll();
      this.sentenceRepository.saveAll();
    }
    return this.transaction.exec();
  }
}

(async () => {
  console.log('Started');
  await redis.connect();
  console.time();
  const context = new RedisContext(redis);

  console.timeEnd();

  redis.disconnect();
})();
