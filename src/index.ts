import { createClient } from 'redis';
import { PrismaClient } from '@prisma/client';
import _, { isEmpty, map, maxBy, uniq } from 'lodash';
import { Queue } from 'typescript-collections';

export const dedupQueue = new Queue<number>();
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

export interface Entity<K extends string | number, V = Record<string, string>> {
  context: RedisContext;
  updated: boolean;
  originalKey?: K;
  key: K;
  toRedis(): V;
}

export abstract class Repository<E extends Entity<string | number, unknown>, K extends string | number> {
  public cache: Record<K, E | Promise<E>>;
  protected abstract prefix: string;

  constructor(protected context: RedisContext) {
    this.cache = {} as Record<K, E>;
  }

  protected getKeys(keys: K[]): Promise<Record<string, string>[]> {
    this.context.client.watch(keys.filter((key) => !(key in this.cache)).map(String));
    return Promise.all(keys.map((key) => this.context.client.hGetAll(`${this.prefix}${key}`)));
  }
  protected async getKey(key: K) {
    const obj = (await this.getKeys([key]))[0];
    return isEmpty(obj) ? null : obj;
  }

  abstract create(...args: any[]): E;
  abstract fromRedis(obj: Record<string, string | Buffer>, key: K): E | Promise<E>;

  protected async loadRedis(key: K) {
    const obj = await this.getKey(key);
    if (isEmpty(obj)) return null;
    const entity = this.fromRedis(obj, key);
    if (!entity) return null;
    this.cache[key] = entity;
    return entity;
  }

  public async get(key: K): Promise<E> | null {
    // Add to cache right away so concurrent requests get the same object
    if (!(key in this.cache)) this.cache[key] = this.loadRedis(key);
    return this.cache[key] as Promise<E>;
  }

  public save(e: E): unknown {
    e.updated = false;
    const key = e.key.toString();
    return Object.entries(e.toRedis()).map(
      ([subKey, value]) => value && this.context.transaction.hSet(`${this.prefix}${key}`, subKey, value),
    );
  }

  public async saveAll() {
    for (const key in this.cache) {
      const entity = (await this.cache[key]) as E;
      if (!entity) continue;
      if (entity.updated) this.save(entity);
    }
  }
}

const loadSentences = async (id: number) => {
  if (!id) return [];
  const card = await db.evidence.findUnique({ where: { id }, select: { id: true, fulltext: true } });
  if (!card?.fulltext) throw new Error(`Card with id ${id} does not exist`);

  return getSentences(card.fulltext);
};
type MatchInfo = { cardLen: number; min: number; max: number };
type MatchPair = { a: MatchInfo; b: MatchInfo };

const checkMatch = (
  { cardLen: aLen, min: aMin, max: aMax }: MatchInfo,
  { cardLen: bLen, min: bMin, max: bMax }: MatchInfo,
) => {
  const insideMatch = aLen > 3 && aLen - (aMax + 1 - aMin) <= INSIDE_TOLERANCE; // If the enterity of A matches
  return insideMatch || (aMin <= EDGE_TOLERANCE && bLen - bMax <= EDGE_TOLERANCE); // If matches the start of A and the end of B
};
// // Check in both orders
const isMatch = (info: MatchPair) => checkMatch(info.a, info.b) || checkMatch(info.b, info.a);

export async function getMatching(context: RedisContext, cardId: number): Promise<number[]> {
  const sentences = (await loadSentences(cardId)) ?? [];
  /* 
    Watch for change in sentences, prevents new card being added that this card should match and it being missed
    Will have a decent amonunt of false positives due to bucketing of sentences
    Probability a card completes without a retry is roughly
    (1 - ((sentencesPerCard * concurrentDeduplication) / numBuckets))^sentencesPerCard
    sentencesPerCard seems to be roughly 30
    With 25 concurrent deduplications happening, and 2^20 buckets, probability is around 0.98
  */
  const sentenceEntities = await Promise.all(sentences.map((s) => context.sentenceRepository.get(s)));
  const cardLens: Record<number, number> = {};
  await Promise.all(
    _(sentenceEntities)
      .map('matches')
      .flatten()
      .map('cardId')
      .uniq()
      .map(async (id) => (cardLens[id] = (await context.cardLengthRepository.get(id)).length))
      .value(),
  );
  const matchInfo: Record<string, MatchPair> = {};
  for (let aIndex = 0; aIndex < sentenceEntities.length; aIndex++) {
    const matches = sentenceEntities[aIndex].matches;
    for (const { cardId, index: bIndex } of matches) {
      if (!(cardId in matchInfo))
        matchInfo[cardId] = {
          a: { cardLen: sentenceEntities.length, min: aIndex, max: aIndex },
          b: { cardLen: cardLens[cardId], min: bIndex, max: bIndex },
        };
      else {
        matchInfo[cardId].a.max = aIndex;
        matchInfo[cardId].b.max = bIndex;
      }
    }
  }

  const matches: number[] = [];
  for (const id in matchInfo) {
    if (isMatch(matchInfo[id])) matches.push(+id);
  }
  return matches;
}

import { SubBucketEntity, SubBucketRepository } from './SubBucket';
import { CardSubBucketRepository } from './CardSubBucket';
import { SentenceRepository } from './Sentence';
import { CardLengthRepository } from './CardLength';
export class RedisContext {
  sentenceRepository: SentenceRepository;
  cardLengthRepository: CardLengthRepository;
  cardSubBucketRepository: CardSubBucketRepository;
  subBucketRepository: SubBucketRepository;
  transaction: RedisTransaction;

  constructor(public client: typeof redis) {
    this.transaction = client.multi();
    this.sentenceRepository = new SentenceRepository(this);
    this.cardLengthRepository = new CardLengthRepository(this);
    this.cardSubBucketRepository = new CardSubBucketRepository(this);
    this.subBucketRepository = new SubBucketRepository(this);
  }

  async finish(save: boolean = true) {
    if (save) {
      await this.subBucketRepository.saveAll();
      await this.cardLengthRepository.saveAll();
      await this.cardSubBucketRepository.saveAll();
      await this.sentenceRepository.saveAll();
    }
    return this.transaction.exec();
  }
}

async function processCard(context: RedisContext, id: number, sentences: string[]) {
  const matchedCards = await getMatching(context, id);
  const bucketCandidates = uniq(
    await Promise.all(matchedCards.map(async (id) => (await context.cardSubBucketRepository.get(id))?.subBucket)),
  ).filter((el) => el); //
  bucketCandidates.forEach((b) => b.setMatches(id, matchedCards));
  const matchedBuckets = bucketCandidates.filter((b) => b.doesBucketMatch(matchedCards));

  let addBucket: SubBucketEntity;
  if (!matchedBuckets.length) {
    addBucket = context.subBucketRepository.create(id, matchedCards);
  } else {
    addBucket = maxBy(matchedBuckets, (b) => b.size) ?? matchedBuckets[0];
    addBucket.addCard(id, matchedCards);
  }

  await addBucket.resolve();
  context.cardLengthRepository.create(id, sentences.length);
  context.cardSubBucketRepository.create(id, addBucket);
  const sentenceEntities = await Promise.all(sentences.map((s) => context.sentenceRepository.get(s)));
  sentenceEntities.forEach((entity, i) => entity.addMatch({ cardId: id, index: i }));
  return context.finish(true);
}

const drain = async () => {
  if (dedupQueue.size() % 10 == 0) console.timeLog('dedup', dedupQueue.size());

  const id = dedupQueue.dequeue();
  if (!id) {
    console.timeEnd('dedup');
    return redis.disconnect();
  }
  const context = new RedisContext(redis);
  await processCard(context, id, (await loadSentences(id)) ?? []);
  setImmediate(drain);
};

(async () => {
  console.log('Started');
  await redis.connect();
  await redis.flushDb();
  const ids = await db.evidence.findMany({ where: { bucketId: 876 }, select: { id: true }, orderBy: { id: 'asc' } });
  for (const { id } of ids) dedupQueue.add(id);
  console.time('dedup');
  drain();
})();
