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
const REQUEST_KEY_IP_DAILY_LIMIT = Math.max(1, Number(process.env.REQUEST_KEY_IP_DAILY_LIMIT ?? 5));
const RUN_DAILY_LIMIT = Math.max(1, Number(process.env.RUN_DAILY_LIMIT ?? 1));
const RUN_TOTAL_LIMIT = Math.max(1, Number(process.env.RUN_TOTAL_LIMIT ?? 10));
const FEEDBACK_EMAIL_TO = process.env.FEEDBACK_EMAIL_TO ?? 'khalidsaidi66@gmail.com';
const FEEDBACK_EMAIL_FROM = process.env.FEEDBACK_EMAIL_FROM ?? 'AI Status Dashboard <hello@aistatusdashboard.com>';
const FEEDBACK_SENDGRID_API_KEY = process.env.FEEDBACK_SENDGRID_API_KEY ?? '';
const FEEDBACK_RESEND_API_KEY = process.env.FEEDBACK_RESEND_API_KEY ?? '';
const FEEDBACK_GITHUB_TOKEN = process.env.FEEDBACK_GITHUB_TOKEN ?? '';
const FEEDBACK_GITHUB_REPO = process.env.FEEDBACK_GITHUB_REPO ?? 'khalidsaidi/a2abench';
const FEEDBACK_GITHUB_MENTION = process.env.FEEDBACK_GITHUB_MENTION ?? '@khalidsaidi';

const submitSchema = z.object({
  entrant_name: z.string().trim().min(1).max(80),
  submissions: z.array(z.object({
    question_id: z.string().trim().min(1).max(200),
    answer: z.string().trim().min(1).max(12_000)
  })).min(1).max(500)
});

const requestKeySchema = z.object({
  email: z.string().trim().email().max(200),
  agent_name: z.string().trim().min(1).max(80)
});

const feedbackSchema = z.object({
  name: z.string().trim().max(120).optional().or(z.literal('')),
  email: z.string().trim().email().max(200).optional().or(z.literal('')),
  message: z.string().trim().min(1).max(4000)
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

function hashValue(value: string) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function currentUtcDay() {
  return new Date().toISOString().slice(0, 10);
}

function generateApiKey() {
  return `a2a_${crypto.randomBytes(24).toString('base64url')}`;
}

function clientIp(request: { headers: Record<string, unknown>; ip?: string }) {
  const forwarded = request.headers['x-forwarded-for'];
  if (typeof forwarded === 'string' && forwarded.trim()) {
    return forwarded.split(',')[0].trim();
  }
  return request.ip ?? 'unknown';
}

async function getReservedDailyTokens(day: string) {
  const snap = await db.collection('judge_daily_tokens').doc(day).get();
  return Number(snap.get('tokens') ?? 0);
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
  email?: string;
  daily_run_count?: number;
  total_run_count?: number;
  daily_run_date?: string;
  api_key_hash?: string;
  key_hash?: string;
  [key: string]: unknown;
};

async function getEntrantByApiKey(apiKey: string): Promise<EntrantRecord | null> {
  const apiKeyHash = hashApiKey(apiKey);
  const primary = await db.collection('entrants').where('api_key_hash', '==', apiKeyHash).limit(1).get();
  if (!primary.empty) {
    const doc = primary.docs[0];
    return { id: doc.id, ...(doc.data() as Record<string, unknown>) };
  }
  const secondary = await db.collection('entrants').where('key_hash', '==', apiKeyHash).limit(1).get();
  if (secondary.empty) return null;
  const doc = secondary.docs[0];
  return { id: doc.id, ...(doc.data() as Record<string, unknown>) };
}

function parseFromAddress(raw: string) {
  const match = raw.match(/^\s*([^<]+?)\s*<([^>]+)>\s*$/);
  if (match) {
    return { name: match[1].trim(), email: match[2].trim() };
  }
  return { email: raw.trim() };
}

async function sendFeedbackEmail(payload: { name?: string; email?: string; message: string; ip: string; userAgent: string }) {
  const from = parseFromAddress(FEEDBACK_EMAIL_FROM);
  const text = [
    'A2ABench feedback submission',
    '',
    `Name: ${payload.name || '(not provided)'}`,
    `Email: ${payload.email || '(not provided)'}`,
    `IP: ${payload.ip}`,
    `User-Agent: ${payload.userAgent || '(not provided)'}`,
    '',
    'Message:',
    payload.message
  ].join('\n');
  const errors: string[] = [];

  if (FEEDBACK_RESEND_API_KEY) {
    const response = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${FEEDBACK_RESEND_API_KEY}`
      },
      body: JSON.stringify({
        from: from.email,
        to: [FEEDBACK_EMAIL_TO],
        subject: 'A2ABench feedback submission',
        text
      })
    });
    if (response.ok) return { sent: true, channel: 'resend' as const };
    const body = await response.text();
    errors.push(`resend ${response.status}: ${body.slice(0, 120)}`);
  }

  if (FEEDBACK_SENDGRID_API_KEY) {
    const response = await fetch('https://api.sendgrid.com/v3/mail/send', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${FEEDBACK_SENDGRID_API_KEY}`
      },
      body: JSON.stringify({
        personalizations: [{ to: [{ email: FEEDBACK_EMAIL_TO }] }],
        from,
        subject: 'A2ABench feedback submission',
        content: [{ type: 'text/plain', value: text }]
      })
    });
    if (response.ok) return { sent: true, channel: 'sendgrid' as const };
    const body = await response.text();
    errors.push(`sendgrid ${response.status}: ${body.slice(0, 120)}`);
  }

  if (FEEDBACK_GITHUB_TOKEN) {
    const title = `feedback: ${new Date().toISOString()}`;
    const issueBody = [
      `${FEEDBACK_GITHUB_MENTION} new feedback submission`,
      '',
      `Name: ${payload.name || '(not provided)'}`,
      `Email: ${payload.email || '(not provided)'}`,
      `IP: ${payload.ip}`,
      `User-Agent: ${payload.userAgent || '(not provided)'}`,
      '',
      'Message:',
      payload.message
    ].join('\n');
    const response = await fetch(`https://api.github.com/repos/${FEEDBACK_GITHUB_REPO}/issues`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${FEEDBACK_GITHUB_TOKEN}`,
        'x-github-api-version': '2022-11-28',
        'user-agent': 'a2abench-feedback-bot'
      },
      body: JSON.stringify({
        title,
        body: issueBody
      })
    });
    if (response.ok) return { sent: true, channel: 'github_issue_notification' as const };
    const body = await response.text();
    errors.push(`github ${response.status}: ${body.slice(0, 120)}`);
  }

  if (errors.length > 0) return { sent: false, reason: errors.join(' | ') };
  return { sent: false, reason: 'no_feedback_notifier_configured' as const };
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

fastify.post('/v1/eval/request-key', async (request, reply) => {
  const parsed = requestKeySchema.safeParse(request.body);
  if (!parsed.success) {
    reply.code(400).send({ error: 'Invalid request body', details: parsed.error.issues });
    return;
  }

  const ip = clientIp(request as unknown as { headers: Record<string, unknown>; ip?: string });
  const userAgent = String(request.headers['user-agent'] ?? '');
  const day = currentUtcDay();
  const ipDocId = `${day}:${hashValue(ip)}`;
  const limitRef = db.collection('key_request_limits').doc(ipDocId);

  try {
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(limitRef);
      const currentCount = Number(snap.get('count') ?? 0);
      if (currentCount >= REQUEST_KEY_IP_DAILY_LIMIT) {
        throw new Error('ip_limit_exceeded');
      }
      tx.set(limitRef, {
        day,
        ip_hash: hashValue(ip),
        count: currentCount + 1,
        updated_at: FieldValue.serverTimestamp()
      }, { merge: true });
    });
  } catch (error) {
    if (String((error as Error).message) === 'ip_limit_exceeded') {
      reply.code(429).send({ error: `Too many key requests from this IP today. Limit is ${REQUEST_KEY_IP_DAILY_LIMIT} per 24h.` });
      return;
    }
    throw error;
  }

  const body = parsed.data;
  const apiKey = generateApiKey();
  const keyHash = hashApiKey(apiKey);
  await db.collection('entrants').doc().set({
    entrant_name: body.agent_name,
    email: body.email,
    api_key_hash: keyHash,
    key_hash: keyHash,
    created_at: FieldValue.serverTimestamp(),
    ip,
    user_agent: userAgent,
    daily_run_count: 0,
    total_run_count: 0,
    daily_run_date: day
  });

  reply.code(200).send({
    api_key: apiKey,
    entrant_name: body.agent_name,
    daily_limit: RUN_DAILY_LIMIT,
    total_limit: RUN_TOTAL_LIMIT
  });
});

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

  const day = currentUtcDay();
  const reservedToday = await getReservedDailyTokens(day);
  if (reservedToday >= JUDGE_DAILY_TOKEN_CAP) {
    reply.code(429).send({ error: 'Judge daily capacity reached. Please retry after UTC midnight.' });
    return;
  }

  const entrantDay = String(entrant.daily_run_date ?? '');
  const entrantDailyCount = entrantDay === day ? Number(entrant.daily_run_count ?? 0) : 0;
  const entrantTotalCount = Number(entrant.total_run_count ?? 0);
  if (entrantDailyCount >= RUN_DAILY_LIMIT) {
    reply.code(429).send({ error: `Daily run limit reached (${RUN_DAILY_LIMIT}/day). Try again after UTC midnight.` });
    return;
  }
  if (entrantTotalCount >= RUN_TOTAL_LIMIT) {
    reply.code(429).send({ error: `Total run limit reached (${RUN_TOTAL_LIMIT} lifetime).` });
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

  const workItems = Array.from(questions.entries()).map(([questionId, data]) => ({
    questionId,
    prompt: String(data.prompt ?? ''),
    reference: String(data.reference_answer ?? ''),
    answer: uniqueByQuestion.get(questionId) ?? ''
  }));

  const totalEstimatedTokens = workItems.reduce((sum, item) => sum + estimateTokens(item.prompt, item.reference, item.answer), 0);
  if (totalEstimatedTokens > JUDGE_DAILY_TOKEN_CAP) {
    reply.code(429).send({ error: 'Submission token estimate exceeds daily judge cap.' });
    return;
  }

  try {
    await reserveDailyTokens(totalEstimatedTokens);
  } catch {
    reply.code(429).send({ error: 'Judge daily capacity reached. Please retry after UTC midnight.' });
    return;
  }

  for (const group of chunked(workItems, JUDGE_CONCURRENCY)) {
    const judged = await Promise.all(group.map(async (item) => {
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
    token_estimate: totalEstimatedTokens
  }, { merge: true });

  const entrantRef = db.collection('entrants').doc(entrant.id);
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(entrantRef);
    const dayNow = currentUtcDay();
    const storedDay = String(snap.get('daily_run_date') ?? '');
    const dailyCount = storedDay === dayNow ? Number(snap.get('daily_run_count') ?? 0) : 0;
    const totalCount = Number(snap.get('total_run_count') ?? 0);
    tx.set(entrantRef, {
      daily_run_date: dayNow,
      daily_run_count: dailyCount + 1,
      total_run_count: totalCount + 1,
      updated_at: FieldValue.serverTimestamp()
    }, { merge: true });
  });

  reply.code(200).send({
    run_id: runRef.id,
    entrant_name: body.entrant_name,
    question_count: judgedCount,
    total_score: averageScore,
    status: 'completed'
  });
});

fastify.post('/v1/feedback', async (request, reply) => {
  const parsed = feedbackSchema.safeParse(request.body);
  if (!parsed.success) {
    reply.code(400).send({ error: 'Invalid request body', details: parsed.error.issues });
    return;
  }

  const body = parsed.data;
  const ip = clientIp(request as unknown as { headers: Record<string, unknown>; ip?: string });
  const userAgent = String(request.headers['user-agent'] ?? '');
  const docRef = db.collection('feedback').doc();
  await docRef.set({
    name: body.name || null,
    email: body.email || null,
    message: body.message,
    ip,
    user_agent: userAgent,
    created_at: FieldValue.serverTimestamp()
  });

  let emailSent = false;
  let emailError: string | null = null;
  try {
    const sent = await sendFeedbackEmail({
      name: body.name || undefined,
      email: body.email || undefined,
      message: body.message,
      ip,
      userAgent
    });
    emailSent = sent.sent;
  } catch (error) {
    emailError = String((error as Error).message || error);
  }

  reply.code(200).send({
    ok: true,
    feedback_id: docRef.id,
    email_notified: emailSent,
    email_error: emailError
  });
});

fastify.setNotFoundHandler((_request, reply) => {
  reply.code(404).send({
    error: 'Not found',
    endpoints: [
      'GET /v1/eval/questions',
      'POST /v1/eval/request-key',
      'POST /v1/eval/submit',
      'GET /v1/eval/leaderboard',
      'POST /v1/feedback'
    ]
  });
});

try {
  await fastify.listen({ port: PORT, host: HOST });
} catch (error) {
  fastify.log.error(error);
  process.exit(1);
}
