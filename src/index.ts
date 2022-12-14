import { createClient, WatchError } from 'redis';
import { PrismaClient } from '@prisma/client';
import _, { isEmpty, maxBy, uniq } from 'lodash';
import { Queue } from 'typescript-collections';

export const BUCKETID = 853;
export const dedupQueue = new Queue<number>();
export const db = new PrismaClient();
export const redis = createClient({
  // url: 'redis://redis:6379',
  password: 'password',
  isolationPoolOptions: { max: 10 },
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

export interface BaseEntity<K extends string | number, V = Record<string, string>> {
  context: RedisContext;
  updated: boolean;
  key: K;
  toRedis(): V;
}

export interface DynamicKeyEntity<K extends string | number, V = Record<string, string>> extends BaseEntity<K, V> {
  createKey(): K;
  propogateKey(): Promise<unknown>;
}

export abstract class Repository<E extends BaseEntity<string | number, unknown>, K extends string | number> {
  public cache: Map<K, E | Promise<E>>;
  protected abstract prefix: string;

  constructor(protected context: RedisContext) {
    this.cache = new Map();
  }

  protected getKeys(keys: K[]): Promise<Record<string, string>[]> {
    this.context.client.watch(keys.filter((key) => !this.cache.has(key)).map(String));
    return Promise.all(keys.map((key) => this.context.client.hGetAll(`${this.prefix}${key}`)));
  }
  protected async getKey(key: K) {
    const obj = (await this.getKeys([key]))[0];
    return isEmpty(obj) ? null : obj;
  }

  abstract createNew(key: K, ...args: any[]): E;
  abstract fromRedis(obj: Record<string, unknown>, key: K): E | Promise<E>;

  public create(key: K, ...args: any[]) {
    const entity = this.createNew(key, ...args);
    this.cache.set(key, entity);
    return entity;
  }

  public async renameCacheKey(oldKey: K, newKey: K) {
    const value = this.cache.get(oldKey);
    this.cache.set(newKey, value);
    this.cache.delete(oldKey);
    this.context.transaction.del(this.prefix + oldKey);
  }

  protected async loadRedis(key: K) {
    const obj = await this.getKey(key);
    if (isEmpty(obj)) return null;
    const entity = this.fromRedis(obj, key);
    if (!entity) return null;
    this.cache.set(key, entity);
    return entity;
  }

  public async get(key: K): Promise<E> | null {
    // Add to cache right away so concurrent requests get the same object
    if (!this.cache.has(key)) this.cache.set(key, this.loadRedis(key));
    return this.cache.get(key);
  }

  public async getMany(keys: readonly K[]): Promise<(E | null)[]> {
    return Promise.all(keys.map((key) => this.get(key)));
  }

  public async save(e: E): Promise<unknown> {
    e.updated = false;
    const key = e.key.toString();
    return Object.entries(await e.toRedis()).map(
      ([subKey, value]) => value && this.context.transaction.hSet(`${this.prefix}${key}`, subKey, value),
    );
  }

  public async saveAll() {
    for (const [key, value] of this.cache) {
      const entity = await value;
      if (!entity) continue;
      if (entity.updated) await this.save(entity);
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
import { BucketSetRepository, cardSet, shouldMerge } from './BucketSet';
import { readFile, writeFile } from 'fs/promises';
export class RedisContext {
  sentenceRepository: SentenceRepository;
  cardLengthRepository: CardLengthRepository;
  cardSubBucketRepository: CardSubBucketRepository;
  subBucketRepository: SubBucketRepository;
  transaction: RedisTransaction;
  bucketSetRepository: BucketSetRepository;

  constructor(public client: typeof redis) {
    this.transaction = client.multi();
    this.sentenceRepository = new SentenceRepository(this);
    this.cardLengthRepository = new CardLengthRepository(this);
    this.cardSubBucketRepository = new CardSubBucketRepository(this);
    this.subBucketRepository = new SubBucketRepository(this);
    this.bucketSetRepository = new BucketSetRepository(this);
  }

  async finish(save: boolean = true) {
    if (save) {
      await this.subBucketRepository.propogateAllKeys();
      await this.bucketSetRepository.propogateAllKeys();

      await this.subBucketRepository.saveAll();
      await this.cardLengthRepository.saveAll();
      await this.cardSubBucketRepository.saveAll();
      await this.sentenceRepository.saveAll();
      await this.bucketSetRepository.saveAll();
    }
    return this.transaction.exec();
  }
}

async function processCard(context: RedisContext, id: number, sentences: string[]) {
  try {
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

    await addBucket.resolve(matchedCards);
    context.cardLengthRepository.create(id, sentences.length);
    context.cardSubBucketRepository.create(id, addBucket);
    const sentenceEntities = await Promise.all(sentences.map((s) => context.sentenceRepository.get(s)));
    sentenceEntities.forEach((entity, i) => entity.addMatch({ cardId: id, index: i }));
    return context.finish(true);
  } catch (err) {
    if (err instanceof WatchError) return processCard(context, id, sentences);
    else throw err;
  }
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

const loadCards = (id: number = BUCKETID) =>
  db.evidence.findMany({
    where: { bucketId: id },
    select: { id: true },
    orderBy: { id: 'asc' },
  });

async function dedup() {
  await redis.flushDb();
  const ids = await loadCards();
  for (const { id } of ids) dedupQueue.add(id);
  console.time('dedup');
  return drain();
}

async function membership(useBucketSet: boolean) {
  const ids = await loadCards();
  const { cardSubBucketRepository } = new RedisContext(redis);

  const memberships = new Map<number, number[]>();
  await Promise.all(
    ids.map(async ({ id }) => {
      const { subBucket } = await cardSubBucketRepository.get(id);
      const parent = useBucketSet ? (await subBucket.getBucketSet()).key : subBucket.key;
      if (!memberships.has(parent)) memberships.set(parent, []);
      memberships.get(parent).push(id);
    }),
  );
  const data = Object.fromEntries([...memberships.entries()].map(([key, value]) => [key, value.sort((a, b) => a - b)]));
  const p = useBucketSet ? './data/853jsSetMembership.json' : './data/853jsMembership.json';

  console.log(
    Object.entries(data)
      .map(([key, value]) => value.length)
      .sort((a, b) => a - b),
  );
  console.log('Writing');
  await writeFile(p, JSON.stringify(data, null, 2));
}

async function movePy() {
  const data: Record<string, number[]> = JSON.parse(await readFile('joined853_membershipSets.json', 'utf-8'));
  const sorted = Object.fromEntries(
    Object.entries(data)
      .sort((a, b) => +a[0] - +b[0])
      .map((entry) => [entry[0], entry[1].sort((a, b) => a - b)]),
  );
  await writeFile('./data/853pySetMembership.json', JSON.stringify(sorted, null, 2));
}

(async () => {
  console.log('Started');
  await redis.connect();
  // await movePy();
  await dedup();
  // await membership(true);
  // await membership(false);
  // const context = new RedisContext(redis);
  // const a = await context.subBucketRepository.get(522457);
  // const b = await context.subBucketRepository.get(632787);
  // const c = await context.subBucketRepository.get(1337638);
  // const c = await context.subBucketRepository.get()
  // console.log(shouldMerge([b], [a, c]));
  // await b.resolve([...b.]);

  // redis.disconnect();
})();
