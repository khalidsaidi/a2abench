import { PrismaClient } from '@prisma/client';
import { markdownToText } from '../src/markdown.ts';

const prisma = new PrismaClient();
const dryRun = process.argv.includes('--dry-run');
const batchSize = Number(process.env.BACKFILL_BATCH_SIZE ?? 100);

async function backfillQuestions() {
  let updated = 0;
  let scanned = 0;
  let cursor: { id: string } | undefined;

  while (true) {
    const batch = await prisma.question.findMany({
      take: batchSize,
      ...(cursor ? { skip: 1, cursor } : {}),
      orderBy: { id: 'asc' },
      select: { id: true, bodyMd: true, bodyText: true }
    });
    if (!batch.length) break;

    for (const row of batch) {
      scanned += 1;
      const nextText = markdownToText(row.bodyMd ?? '');
      if (row.bodyText !== nextText) {
        updated += 1;
        if (!dryRun) {
          await prisma.question.update({
            where: { id: row.id },
            data: { bodyText: nextText }
          });
        }
      }
      cursor = { id: row.id };
    }
  }

  return { scanned, updated };
}

async function backfillAnswers() {
  let updated = 0;
  let scanned = 0;
  let cursor: { id: string } | undefined;

  while (true) {
    const batch = await prisma.answer.findMany({
      take: batchSize,
      ...(cursor ? { skip: 1, cursor } : {}),
      orderBy: { id: 'asc' },
      select: { id: true, bodyMd: true, bodyText: true }
    });
    if (!batch.length) break;

    for (const row of batch) {
      scanned += 1;
      const nextText = markdownToText(row.bodyMd ?? '');
      if (row.bodyText !== nextText) {
        updated += 1;
        if (!dryRun) {
          await prisma.answer.update({
            where: { id: row.id },
            data: { bodyText: nextText }
          });
        }
      }
      cursor = { id: row.id };
    }
  }

  return { scanned, updated };
}

async function main() {
  const startedAt = new Date().toISOString();
  console.log(`[backfill] start ${startedAt} dryRun=${dryRun} batchSize=${batchSize}`);

  const questions = await backfillQuestions();
  const answers = await backfillAnswers();

  const finishedAt = new Date().toISOString();
  console.log('[backfill] questions', questions);
  console.log('[backfill] answers', answers);
  console.log(`[backfill] done ${finishedAt}`);
}

main()
  .catch((err) => {
    console.error('[backfill] error', err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
