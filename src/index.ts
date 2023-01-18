import { Prisma, PrismaClient } from '@prisma/client';
import { Queue } from 'typescript-collections';
import { RedisContext, redis } from './redis';
import { readFile, writeFile } from 'fs/promises';
import { uniq } from 'lodash';
import { getMatching, loadSentences, processCard } from './duplicate';

export const BUCKETID = 33934;
export const dedupQueue = new Queue<number>();
export const db = new PrismaClient();
export type TagInput = {
  name: string;
  label: string;
};
export const connectOrCreateTag = ({ name, label }: TagInput): Prisma.TagCreateOrConnectWithoutFilesInput => ({
  where: { name },
  create: { name, label },
});

const drain = async () => {
  if (dedupQueue.size() % 1000 == 0) console.timeLog('dedup', dedupQueue.size());

  const id = dedupQueue.dequeue();
  if (!id) {
    console.timeEnd('dedup');
    // await saveMockDB();
    return redis.disconnect();
  }
  const { updates, deletes } = await processCard(id, (await loadSentences(id)) ?? []);

  // for (const del of deletes) mockDB.delete(del);
  // for (const { bucketId, cardIds } of updates) mockDB.set(bucketId, cardIds);

  setImmediate(drain);
};

const loadBucket = (id: number = BUCKETID) =>
  db.evidence.findMany({
    where: { bucketId: id },
    select: { id: true },
    orderBy: { id: 'asc' },
  });
const loadCards = (take = 10_000, skip = 0) =>
  db.evidence.findMany({
    select: { id: true },
    orderBy: { id: 'asc' },
    take,
    skip,
  });

const loadOpenEv = (take?: number) =>
  db.evidence.findMany({
    where: { file: { tags: { some: { name: 'openev' } } } },
    select: { id: true },
    orderBy: { id: 'asc' },
    take,
  });

async function dedup() {
  const keys = await redis.keys('TEST:*');
  if (keys.length) await redis.del(keys);
  console.log('Deleted tests');
  const ids = await loadCards();
  for (const { id } of ids) dedupQueue.add(id);
  console.time('dedup');
  return drain();
}

async function membership(useBucketSet: boolean) {
  const ids = await loadBucket();
  const { cardSubBucketRepository } = new RedisContext(redis);

  const memberships = new Map<number, number[]>();
  await Promise.all(
    ids.map(async ({ id }) => {
      const { subBucket } = (await cardSubBucketRepository.get(id)) ?? { subBucket: null };
      if (!subBucket) return;
      const parent = useBucketSet ? (await subBucket.getBucketSet()).key : subBucket.key;
      if (!memberships.has(parent)) memberships.set(parent, []);
      memberships.get(parent).push(id);
    }),
  );
  const data = Object.fromEntries([...memberships.entries()].map(([key, value]) => [key, value.sort((a, b) => a - b)]));
  const p = useBucketSet ? `./data/${BUCKETID}jsSetMembership.json` : `./data/${BUCKETID}jsMembership.json`;

  console.log(
    Object.entries(data)
      .map(([key, value]) => value.length)
      .sort((a, b) => a - b),
  );
  console.log('Writing');
  await writeFile(p, JSON.stringify(data, null, 2));
}

async function movePy() {
  const data: Record<string, number[]> = JSON.parse(
    await readFile(`./data/joined${BUCKETID}_membershipSets.json`, 'utf-8'),
  );
  const sorted = Object.fromEntries(
    Object.entries(data)
      .sort((a, b) => +a[0] - +b[0])
      .map((entry) => [entry[0], entry[1].sort((a, b) => a - b)]),
  );
  await writeFile(`./data/${BUCKETID}pySetMembership.json`, JSON.stringify(sorted, null, 2));
}

async function broken() {
  const ids = await loadBucket();
  const context = new RedisContext(redis);

  const subBuckets = uniq(
    (await context.cardSubBucketRepository.getMany(ids.map((card) => card.id))).map((bucket) => bucket.subBucket),
  );

  for (const subBucket of subBuckets) {
    console.log(subBucket.key, subBucket.size, Math.max(...subBucket.cards.values()));
  }
}

async function matchCounts(context: RedisContext, ids: number[]) {
  return new Map(
    await Promise.all(ids.map(async (id) => [id, (await getMatching(context, id)).matches.length] as [number, number])),
  );
}

(async () => {
  console.log('Started');
  await redis.connect();
  // await movePy();
  await dedup();
  // await broken();
  // const context = new RedisContext(redis);
  // await membership(false);
  // await membership(true);
  // await redis.quit();
})();
