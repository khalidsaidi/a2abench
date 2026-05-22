#!/usr/bin/env tsx
import 'dotenv/config';
import { getApps, initializeApp, cert, applicationDefault } from 'firebase-admin/app';
import { getFirestore, FieldValue, Timestamp } from 'firebase-admin/firestore';

type QuestionDoc = {
  id: string;
  title?: string;
  bodyMd?: string;
  bodyText?: string;
  source?: string;
  createdAt?: Timestamp | string | null;
  tags?: string[];
  acceptedAnswerId?: string;
};

type AnswerDoc = {
  id: string;
  questionId?: string;
  bodyMd?: string;
  bodyText?: string;
  isAccepted?: boolean;
  source?: string;
};

function initFirebase() {
  if (!getApps().length) {
    const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    if (serviceAccountJson) {
      initializeApp({ credential: cert(JSON.parse(serviceAccountJson)) });
      return;
    }
    initializeApp({ credential: applicationDefault() });
  }
}

function normalizeText(input: string): string {
  return input.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

function slugSource(source?: string): string {
  if (!source) return 'unknown';
  try {
    const host = new URL(source).hostname.toLowerCase();
    if (host.includes('stackoverflow.com')) return 'stackoverflow.com';
    if (host.includes('github.com')) return 'github.com';
    return host;
  } catch {
    return source.toLowerCase();
  }
}

function scoreQuestion(q: QuestionDoc, a: AnswerDoc): number {
  const prompt = normalizeText(`${q.title ?? ''}\n${q.bodyMd ?? q.bodyText ?? ''}`);
  const answer = normalizeText(a.bodyMd ?? a.bodyText ?? '');
  if (prompt.length < 40 || answer.length < 100 || answer.length > 2000) return -1;
  let score = 0;
  if (q.tags?.length) score += 2;
  if (slugSource(q.source) === 'stackoverflow.com') score += 2;
  score += Math.min(6, Math.floor(prompt.length / 250));
  return score;
}

function trigramSet(text: string): Set<string> {
  const cleaned = normalizeText(text).toLowerCase();
  const set = new Set<string>();
  for (let i = 0; i < cleaned.length - 2; i++) set.add(cleaned.slice(i, i + 3));
  return set;
}

function jaccard(a: Set<string>, b: Set<string>): number {
  if (!a.size || !b.size) return 0;
  let overlap = 0;
  for (const token of a) if (b.has(token)) overlap += 1;
  return overlap / (a.size + b.size - overlap);
}

async function main() {
  initFirebase();
  const db = getFirestore();

  const [questionSnap, answerSnap] = await Promise.all([
    db.collection('questions').get(),
    db.collection('answers').where('isAccepted', '==', true).get()
  ]);

  const answers = new Map<string, AnswerDoc>();
  for (const doc of answerSnap.docs) {
    const data = doc.data() as AnswerDoc;
    answers.set(doc.id, { ...data, id: doc.id });
  }

  const candidates: Array<{
    questionId: string;
    prompt: string;
    referenceAnswer: string;
    source: string;
    category: string;
    createdAt: Timestamp;
    sim: Set<string>;
    score: number;
  }> = [];

  for (const doc of questionSnap.docs) {
    const q = { ...(doc.data() as QuestionDoc), id: doc.id };
    const acceptedId = q.acceptedAnswerId;
    if (!acceptedId) continue;
    const answer = answers.get(acceptedId);
    if (!answer) continue;
    const rank = scoreQuestion(q, answer);
    if (rank < 0) continue;

    const prompt = normalizeText(`${q.title ?? ''}\n${q.bodyMd ?? q.bodyText ?? ''}`);
    const referenceAnswer = normalizeText(answer.bodyMd ?? answer.bodyText ?? '');
    const source = q.source ?? answer.source ?? '';
    const category = (q.tags?.[0] ?? 'uncategorized').toLowerCase();
    const createdAt = q.createdAt instanceof Timestamp ? q.createdAt : Timestamp.now();

    candidates.push({
      questionId: q.id,
      prompt,
      referenceAnswer,
      source,
      category,
      createdAt,
      sim: trigramSet(prompt),
      score: rank
    });
  }

  candidates.sort((left, right) => right.score - left.score);

  const selected: typeof candidates = [];
  const categoryBuckets = new Map<string, number>();
  const perCategoryCap = 125;

  for (const candidate of candidates) {
    if (selected.length >= 500) break;

    const usedInCategory = categoryBuckets.get(candidate.category) ?? 0;
    if (usedInCategory >= perCategoryCap) continue;

    let duplicate = false;
    for (const existing of selected) {
      if (jaccard(candidate.sim, existing.sim) >= 0.86) {
        duplicate = true;
        break;
      }
    }
    if (duplicate) continue;

    selected.push(candidate);
    categoryBuckets.set(candidate.category, usedInCategory + 1);
  }

  if (selected.length < 500) {
    throw new Error(`Only selected ${selected.length} eligible questions; need 500`);
  }

  const batch = db.batch();
  const collection = db.collection('benchmark_questions');
  const now = FieldValue.serverTimestamp();

  for (const item of selected.slice(0, 500)) {
    const docId = item.questionId.startsWith('so_') ? item.questionId : `so_${item.questionId}`;
    batch.set(collection.doc(docId), {
      id: docId,
      prompt: item.prompt,
      reference_answer: item.referenceAnswer,
      source: slugSource(item.source),
      category: item.category,
      created_at: item.createdAt.toDate().toISOString(),
      rebuilt_at: now
    });
  }

  await batch.commit();

  console.log(JSON.stringify({ selected: 500, categories: Object.fromEntries(categoryBuckets) }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
