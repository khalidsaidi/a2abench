import 'dotenv/config';
import crypto from 'node:crypto';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import { cert, getApps, initializeApp } from 'firebase-admin/app';
import { getFirestore, FieldValue, Timestamp, type Firestore } from 'firebase-admin/firestore';
import { z } from 'zod';

const PORT = Number(process.env.PORT ?? 3000);
const HOST = process.env.HOST ?? '0.0.0.0';
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL ?? 'https://a2abench-api.web.app').replace(/\/$/, '');
const JUDGE_MODEL = process.env.JUDGE_MODEL ?? 'claude-sonnet-4-20250514';
const JUDGE_PROVIDER = (process.env.JUDGE_PROVIDER ?? 'anthropic').toLowerCase();
const JUDGE_LLM_KEY = process.env.JUDGE_LLM_KEY ?? '';
const JUDGE_DAILY_TOKEN_CAP = Math.max(1, Number(process.env.JUDGE_DAILY_TOKEN_CAP ?? 200_000));
const JUDGE_CONCURRENCY = Math.max(1, Math.min(10, Number(process.env.JUDGE_CONCURRENCY ?? 10)));
const QUESTIONS_PAGE_SIZE = 50;

const submitSchema = z.object({
  entrant_name: z.string().trim().min(1).max(80),
  submissions: z.array(z.object({
    question_id: z.string().trim().min(1).max(200),
    answer: z.string().trim().min(1).max(12_000)
  })).min(1).max(500)
});

function parseServiceAccount() {
  const projectId = process.env.FIREBASE_PROJECT_ID;
  const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
  const privateKey = process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, '\n');
  if (projectId && clientEmail && privateKey) {
    return { projectId, clientEmail, privateKey };
  }
  return null;
}

function initFirestore(): Firestore {
  const app = getApps()[0] ?? (() => {
    const serviceAccount = parseServiceAccount();
    if (serviceAccount) return initializeApp({ credential: cert(serviceAccount), projectId: serviceAccount.projectId });
    return initializeApp();
  })();
  return getFirestore(app);
}

const db = initFirestore();

function hashApiKey(key: string) {
  return crypto.createHash('sha256').update(key).digest('hex');
}

function parseApiKey(rawAuth: unknown, rawHeader: unknown): string | null {
  const auth = typeof rawAuth === 'string' ? rawAuth.trim() : '';
  const keyHeader = typeof rawHeader === 'string' ? rawHeader.trim() : '';
  if (keyHeader) return keyHeader;
  if (!auth) return null;
  const [scheme, ...parts] = auth.split(' ');
  if (!scheme || scheme.toLowerCase() !== 'bearer') return null;
  const token = parts.join(' ').trim();
  return token || null;
}

function chunked<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function estimateTokens(prompt: string, reference: string, answer: string) {
  return Math.ceil((prompt.length + reference.length + answer.length) / 4);
}

function stripFence(text: string) {
  return text.replace(/^```(?:json)?\s*/i, '').replace(/```\s*$/i, '').trim();
}

function asTextContent(content: unknown): string {
  if (!Array.isArray(content)) return '';
  return content
    .map((entry) => {
      if (entry && typeof entry === 'object' && (entry as { type?: unknown }).type === 'text') {
        return String((entry as { text?: unknown }).text ?? '');
      }
      return '';
    })
    .join('\n')
    .trim();
}

async function reserveDailyTokens(tokenEstimate: number) {
  const day = new Date().toISOString().slice(0, 10);
  const docRef = db.collection('judge_daily_tokens').doc(day);
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(docRef);
    const current = Number(snap.get('tokens') ?? 0);
    if (current + tokenEstimate > JUDGE_DAILY_TOKEN_CAP) {
      throw new Error('JUDGE_DAILY_TOKEN_CAP reached');
    }
    tx.set(docRef, {
      day,
      tokens: current + tokenEstimate,
      updated_at: FieldValue.serverTimestamp()
    }, { merge: true });
  });
}

async function judgeAnswer(prompt: string, referenceAnswer: string, submittedAnswer: string) {
  if (!JUDGE_LLM_KEY) {
    return {
      score: 0,
      judge_reasoning: `Judge key missing for provider ${JUDGE_PROVIDER}; submission recorded with zero score.`,
      usageTokens: 0
    };
  }

  const promptText = [
    'You are grading an agent benchmark answer.',
    'Return strict JSON only with keys: score (0-100 integer), judge_reasoning (one sentence).',
    'Score should reward correctness and usefulness vs reference answer.',
    '',
    `QUESTION:\n${prompt}`,
    '',
    `REFERENCE_ANSWER:\n${referenceAnswer}`,
    '',
    `SUBMITTED_ANSWER:\n${submittedAnswer}`
  ].join('\n');

  let text = '';
  let usageTokens = 0;

  if (JUDGE_PROVIDER === 'xai') {
    const response = await fetch('https://api.x.ai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${JUDGE_LLM_KEY}`
      },
      body: JSON.stringify({
        model: JUDGE_MODEL,
        temperature: 0,
        max_tokens: 220,
        messages: [{ role: 'user', content: promptText }]
      })
    });
    if (!response.ok) {
      const errorText = await response.text();
      return {
        score: 0,
        judge_reasoning: `Judge request failed (${response.status}): ${errorText.slice(0, 200)}`,
        usageTokens: 0
      };
    }
    const json = await response.json() as {
      choices?: Array<{ message?: { content?: string } }>;
      usage?: { prompt_tokens?: number; completion_tokens?: number; total_tokens?: number };
    };
    text = stripFence(String(json.choices?.[0]?.message?.content ?? '').trim());
    usageTokens = Number(json.usage?.total_tokens ?? (Number(json.usage?.prompt_tokens ?? 0) + Number(json.usage?.completion_tokens ?? 0)));
  } else {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': JUDGE_LLM_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: JUDGE_MODEL,
        max_tokens: 220,
        temperature: 0,
        messages: [{ role: 'user', content: promptText }]
      })
    });

    if (!response.ok) {
      const errorText = await response.text();
      return {
        score: 0,
        judge_reasoning: `Judge request failed (${response.status}): ${errorText.slice(0, 200)}`,
        usageTokens: 0
      };
    }

    const json = await response.json() as {
      content?: unknown;
      usage?: { input_tokens?: number; output_tokens?: number };
    };
    text = stripFence(asTextContent(json.content));
    usageTokens = Number(json.usage?.input_tokens ?? 0) + Number(json.usage?.output_tokens ?? 0);
  }

  try {
    const parsed = JSON.parse(text) as { score?: unknown; judge_reasoning?: unknown };
    const score = Math.max(0, Math.min(100, Math.round(Number(parsed.score ?? 0) || 0)));
    const judge_reasoning = String(parsed.judge_reasoning ?? 'No judge reasoning provided.').slice(0, 240);
    return { score, judge_reasoning, usageTokens };
  } catch {
    return {
      score: 0,
      judge_reasoning: `Judge response parse failure: ${text.slice(0, 200) || 'empty response'}`,
      usageTokens
    };
  }
}

type EntrantRecord = {
  id: string;
  entrant_name?: string;
  [key: string]: unknown;
};

async function getEntrantByApiKey(apiKey: string): Promise<EntrantRecord | null> {
  const apiKeyHash = hashApiKey(apiKey);
  const snap = await db.collection('entrants').where('api_key_hash', '==', apiKeyHash).limit(1).get();
  if (snap.empty) return null;
  const doc = snap.docs[0];
  return { id: doc.id, ...(doc.data() as Record<string, unknown>) };
}

function toIso(value: unknown): string | null {
  if (value instanceof Timestamp) return value.toDate().toISOString();
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'string') {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) return parsed.toISOString();
  }
  if (typeof value === 'number') {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) return parsed.toISOString();
  }
  if (value && typeof value === 'object') {
    const candidate = value as { _seconds?: unknown; _nanoseconds?: unknown };
    if (typeof candidate._seconds === 'number') {
      const nanos = typeof candidate._nanoseconds === 'number' ? candidate._nanoseconds : 0;
      const millis = candidate._seconds * 1000 + Math.floor(nanos / 1_000_000);
      const parsed = new Date(millis);
      if (!Number.isNaN(parsed.getTime())) return parsed.toISOString();
    }
  }
  return null;
}

const fastify = Fastify({ logger: true });
await fastify.register(cors, { origin: true });

fastify.get('/healthz', async () => ({ ok: true, service: 'a2abench-benchmark-api' }));
fastify.get('/health', async () => ({ ok: true, service: 'a2abench-benchmark-api' }));

fastify.get('/.well-known/agent.json', async () => ({
  name: 'A2ABench',
  description: 'Public benchmark where agents submit Q&A answers and get scored on a leaderboard.',
  url: PUBLIC_BASE_URL,
  version: '1.0.1',
  actions: [
    { name: 'list_benchmark_questions', method: 'GET', path: '/v1/eval/questions' },
    { name: 'submit_benchmark_run', method: 'POST', path: '/v1/eval/submit' },
    { name: 'get_leaderboard', method: 'GET', path: '/v1/eval/leaderboard' }
  ]
}));
fastify.get('/.well-known/agent-card.json', async () => ({
  name: 'A2ABench',
  description: 'Public benchmark where agents submit Q&A answers and get scored on a leaderboard.',
  url: PUBLIC_BASE_URL,
  version: '1.0.1',
  preferredTransport: 'https',
  skills: [
    { id: 'list_benchmark_questions', description: 'List benchmark questions.' },
    { id: 'submit_benchmark_run', description: 'Submit answers for scoring.' },
    { id: 'get_leaderboard', description: 'Fetch ranked benchmark runs.' }
  ]
}));

fastify.get('/v1/eval/questions', async (request, reply) => {
  const page = Math.max(1, Number((request.query as { page?: string }).page ?? '1'));
  const offset = (page - 1) * QUESTIONS_PAGE_SIZE;

  const pagesToScan = Math.floor(offset / QUESTIONS_PAGE_SIZE) + 1;
  let cursor: FirebaseFirestore.QueryDocumentSnapshot | undefined;
  for (let i = 1; i < pagesToScan; i += 1) {
    let pageQuery: FirebaseFirestore.Query = db.collection('benchmark_questions').orderBy('created_at', 'asc').limit(QUESTIONS_PAGE_SIZE);
    if (cursor) pageQuery = pageQuery.startAfter(cursor);
    const pageSnap = await pageQuery.get();
    if (pageSnap.empty) {
      reply.code(200).send({ page, pageSize: QUESTIONS_PAGE_SIZE, results: [] });
      return;
    }
    cursor = pageSnap.docs[pageSnap.docs.length - 1];
  }

  let query: FirebaseFirestore.Query = db.collection('benchmark_questions').orderBy('created_at', 'asc').limit(QUESTIONS_PAGE_SIZE);
  if (cursor) query = query.startAfter(cursor);
  const snap = await query.get();
  const results = snap.docs.map((doc) => {
    const data = doc.data();
    return {
      id: doc.id,
      prompt: String(data.prompt ?? ''),
      source: String(data.source ?? ''),
      category: String(data.category ?? 'general'),
      created_at: toIso(data.created_at)
    };
  });

  reply.code(200).send({
    page,
    pageSize: QUESTIONS_PAGE_SIZE,
    results,
    nextPage: results.length === QUESTIONS_PAGE_SIZE ? page + 1 : null
  });
});

fastify.get('/v1/eval/leaderboard', async (request, reply) => {
  const limit = Math.max(1, Math.min(200, Number((request.query as { limit?: string }).limit ?? '100')));
  const scanLimit = Math.max(limit * 5, 500);
  const snap = await db.collection('runs').orderBy('completed_at', 'desc').limit(scanLimit).get();

  const ranked = snap.docs
    .map((doc) => ({ doc, data: doc.data() }))
    .filter(({ data }) => String(data.status ?? '') === 'completed')
    .sort((a, b) => {
      const scoreA = Number(a.data.total_score ?? 0);
      const scoreB = Number(b.data.total_score ?? 0);
      if (scoreA !== scoreB) return scoreB - scoreA;
      return String(a.doc.id).localeCompare(String(b.doc.id));
    })
    .slice(0, limit);

  const results = ranked.map(({ doc, data }, idx) => {
    return {
      rank: idx + 1,
      run_id: doc.id,
      entrant_name: String(data.entrant_name ?? 'unknown'),
      score: Number(data.total_score ?? 0),
      question_count: Number(data.question_count ?? 0),
      date: toIso(data.completed_at)
    };
  });

  reply.code(200).send({ results });
});

fastify.post('/v1/eval/submit', async (request, reply) => {
  const parsed = submitSchema.safeParse(request.body);
  if (!parsed.success) {
    reply.code(400).send({ error: 'Invalid request body', details: parsed.error.issues });
    return;
  }

  const apiKey = parseApiKey(request.headers.authorization, request.headers['x-api-key']);
  if (!apiKey) {
    reply.code(401).send({ error: 'Missing API key. Use Authorization: Bearer <key> or X-API-Key.' });
    return;
  }

  const entrant = await getEntrantByApiKey(apiKey);
  if (!entrant) {
    reply.code(401).send({ error: 'Invalid API key.' });
    return;
  }

  const body = parsed.data;
  if (String(entrant.entrant_name ?? '').trim() !== body.entrant_name.trim()) {
    reply.code(403).send({ error: 'API key does not match entrant_name.' });
    return;
  }

  const uniqueByQuestion = new Map<string, string>();
  for (const submission of body.submissions) {
    if (!uniqueByQuestion.has(submission.question_id)) {
      uniqueByQuestion.set(submission.question_id, submission.answer);
    }
  }

  const questionIds = Array.from(uniqueByQuestion.keys());
  const questionDocs = await Promise.all(chunked(questionIds, 10).map(async (group) => {
    const refs = group.map((id) => db.collection('benchmark_questions').doc(id));
    const snaps = await db.getAll(...refs);
    return snaps.filter((snap) => snap.exists);
  }));
  const questions = new Map<string, FirebaseFirestore.DocumentData>();
  for (const snap of questionDocs.flat()) questions.set(snap.id, snap.data() ?? {});

  if (questions.size === 0) {
    reply.code(400).send({ error: 'None of the submitted question_id values exist.' });
    return;
  }

  const runRef = db.collection('runs').doc();
  await runRef.set({
    entrant_name: body.entrant_name,
    status: 'running',
    question_count: questions.size,
    total_score: 0,
    completed_at: null,
    created_at: FieldValue.serverTimestamp()
  });

  let totalScore = 0;
  let judgedCount = 0;
  let perRunEstimatedTokens = 0;

  const workItems = Array.from(questions.entries()).map(([questionId, data]) => ({
    questionId,
    prompt: String(data.prompt ?? ''),
    reference: String(data.reference_answer ?? ''),
    answer: uniqueByQuestion.get(questionId) ?? ''
  }));

  for (const group of chunked(workItems, JUDGE_CONCURRENCY)) {
    const judged = await Promise.all(group.map(async (item) => {
      const estimate = estimateTokens(item.prompt, item.reference, item.answer);
      if (perRunEstimatedTokens + estimate > JUDGE_DAILY_TOKEN_CAP) {
        return {
          questionId: item.questionId,
          answer: item.answer,
          score: 0,
          judge_reasoning: `Run token cap ${JUDGE_DAILY_TOKEN_CAP} reached before scoring this answer.`
        };
      }

      try {
        await reserveDailyTokens(estimate);
      } catch {
        return {
          questionId: item.questionId,
          answer: item.answer,
          score: 0,
          judge_reasoning: `Daily token cap ${JUDGE_DAILY_TOKEN_CAP} reached.`
        };
      }

      perRunEstimatedTokens += estimate;
      const judgedResult = await judgeAnswer(item.prompt, item.reference, item.answer);
      return {
        questionId: item.questionId,
        answer: item.answer,
        score: judgedResult.score,
        judge_reasoning: judgedResult.judge_reasoning
      };
    }));

    const batch = db.batch();
    for (const result of judged) {
      totalScore += result.score;
      judgedCount += 1;
      const submissionRef = db.collection('submissions').doc();
      batch.set(submissionRef, {
        entrant_name: body.entrant_name,
        question_id: result.questionId,
        answer: result.answer,
        score: result.score,
        judge_reasoning: result.judge_reasoning,
        submitted_at: FieldValue.serverTimestamp(),
        run_id: runRef.id
      });
    }
    await batch.commit();
  }

  const averageScore = judgedCount > 0 ? Number((totalScore / judgedCount).toFixed(2)) : 0;
  await runRef.set({
    status: 'completed',
    total_score: averageScore,
    question_count: judgedCount,
    completed_at: FieldValue.serverTimestamp(),
    token_estimate: perRunEstimatedTokens
  }, { merge: true });

  reply.code(200).send({
    run_id: runRef.id,
    entrant_name: body.entrant_name,
    question_count: judgedCount,
    total_score: averageScore,
    status: 'completed'
  });
});

fastify.setNotFoundHandler((_request, reply) => {
  reply.code(404).send({
    error: 'Not found',
    endpoints: [
      'GET /v1/eval/questions',
      'POST /v1/eval/submit',
      'GET /v1/eval/leaderboard'
    ]
  });
});

try {
  await fastify.listen({ port: PORT, host: HOST });
} catch (error) {
  fastify.log.error(error);
  process.exit(1);
}
