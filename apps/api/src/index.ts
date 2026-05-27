import 'dotenv/config';
import crypto from 'node:crypto';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import formbody from '@fastify/formbody';
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
const SIBLING_RAGMAP_URL = 'https://ragmap-api.web.app';
const SIBLING_ROOTFETCH_URL = 'https://rootfetch.com';
const SIBLING_AGENTABILITY_URL = 'https://agentability.org';
const SIBLING_RELAYORB_URL = 'https://relayorb.com';
const SIBLING_AISTATUSDASHBOARD_URL = 'https://aistatusdashboard.com';
const BASELINE_ENTRANT_NAMES = new Set(
  (process.env.BASELINE_ENTRANTS ?? 'claude-haiku-4-5,gemini-2-0-flash,gemini-2-5-flash')
    .split(',')
    .map((name) => name.trim().toLowerCase())
    .filter(Boolean)
);
const PUBLIC_CACHE_SECONDS = 60;

type SiblingStatsLink = {
  name: string;
  url: string;
  stats_url: string;
  stats_json_url: string;
  agent_card_url: string;
};

function siblingLinksForStats(): Record<string, SiblingStatsLink> {
  return {
    ragmap: {
      name: 'Ragmap',
      url: SIBLING_RAGMAP_URL,
      stats_url: `${SIBLING_RAGMAP_URL}/stats`,
      stats_json_url: `${SIBLING_RAGMAP_URL}/stats.json`,
      agent_card_url: `${SIBLING_RAGMAP_URL}/.well-known/agent.json`
    },
    rootfetch: {
      name: 'Rootfetch',
      url: SIBLING_ROOTFETCH_URL,
      stats_url: `${SIBLING_ROOTFETCH_URL}/stats`,
      stats_json_url: `${SIBLING_ROOTFETCH_URL}/stats.json`,
      agent_card_url: `${SIBLING_ROOTFETCH_URL}/.well-known/agent.json`
    },
    agentability: {
      name: 'Agentability',
      url: SIBLING_AGENTABILITY_URL,
      stats_url: `${SIBLING_AGENTABILITY_URL}/stats`,
      stats_json_url: `${SIBLING_AGENTABILITY_URL}/stats.json`,
      agent_card_url: `${SIBLING_AGENTABILITY_URL}/.well-known/agent.json`
    },
    relayorb: {
      name: 'RelayOrb',
      url: SIBLING_RELAYORB_URL,
      stats_url: `${SIBLING_RELAYORB_URL}/stats`,
      stats_json_url: `${SIBLING_RELAYORB_URL}/stats.json`,
      agent_card_url: `${SIBLING_RELAYORB_URL}/.well-known/agent.json`
    },
    aistatusdashboard: {
      name: 'AIStatusDashboard',
      url: SIBLING_AISTATUSDASHBOARD_URL,
      stats_url: `${SIBLING_AISTATUSDASHBOARD_URL}/stats`,
      stats_json_url: `${SIBLING_AISTATUSDASHBOARD_URL}/stats.json`,
      agent_card_url: `${SIBLING_AISTATUSDASHBOARD_URL}/.well-known/agent.json`
    }
  };
}

function relatedProjectsForAgentCard() {
  return [
    {
      name: 'Ragmap',
      url: SIBLING_RAGMAP_URL,
      agent_card_url: `${SIBLING_RAGMAP_URL}/.well-known/agent.json`,
      description: 'MCP search and RAG-focused server discovery.'
    },
    {
      name: 'Rootfetch',
      url: SIBLING_ROOTFETCH_URL,
      agent_card_url: `${SIBLING_ROOTFETCH_URL}/.well-known/agent.json`,
      description: 'DNS delegation intelligence with MCP telemetry.'
    },
    {
      name: 'Agentability',
      url: SIBLING_AGENTABILITY_URL,
      agent_card_url: `${SIBLING_AGENTABILITY_URL}/.well-known/agent.json`,
      description: 'Agent-readiness audit and evidence-backed report publishing.'
    },
    {
      name: 'RelayOrb',
      url: SIBLING_RELAYORB_URL,
      agent_card_url: `${SIBLING_RELAYORB_URL}/.well-known/agent.json`,
      description: 'Tool control plane for AI agents with contract-first routing.'
    },
    {
      name: 'AIStatusDashboard',
      url: SIBLING_AISTATUSDASHBOARD_URL,
      agent_card_url: `${SIBLING_AISTATUSDASHBOARD_URL}/.well-known/agent.json`,
      description: 'Real-time AI provider status monitoring with evidence-backed metrics.'
    }
  ];
}

function crossProjectFooterHtml() {
  return `<footer data-cross-project-footer style="margin-top:28px;padding-top:14px;border-top:1px solid #d8d8d2;color:#555;font-size:13px">Cross-project: <a href="${SIBLING_RAGMAP_URL}/stats">Ragmap</a> · <a href="${SIBLING_ROOTFETCH_URL}/stats">Rootfetch</a> · <a href="${SIBLING_AGENTABILITY_URL}/stats">Agentability</a> · <a href="${SIBLING_RELAYORB_URL}/stats">RelayOrb</a> · <a href="${SIBLING_AISTATUSDASHBOARD_URL}/stats">AIStatusDashboard</a> — MCP search · DNS delegation · agent-readiness audit · tool control plane · status monitoring</footer>`;
}

function attachCrossProjectFooter(html: string) {
  if (!html.toLowerCase().includes('</body>')) return html;
  if (html.includes('data-cross-project-footer')) return html;
  return html.replace(/<\/body>/i, `${crossProjectFooterHtml()}\n</body>`);
}

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

type LeaderboardRow = {
  rank: number;
  run_id: string;
  entrant_name: string;
  score: number;
  question_count: number;
  date: string | null;
};

type PublicStatsPayload = {
  as_of_utc: string;
  generated_at: string;
  submissions: number;
  entrants: number;
  keys_issued: number;
  feedback_count: number;
  baseline_runs: number;
  total_completed_runs: number;
  last_submission_ts: string | null;
  top10: LeaderboardRow[];
};

type PublicStatsCache = {
  expiresAtMs: number;
  payload: PublicStatsPayload;
};

type AgentabilityReportSummary = {
  score: number | null;
  grade: string | null;
};

let publicStatsCache: PublicStatsCache | null = null;

async function fetchAgentabilityReportSummary(domain: string): Promise<AgentabilityReportSummary> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 1500);
  try {
    const url = `${SIBLING_AGENTABILITY_URL}/v1/evaluations/${encodeURIComponent(domain)}/latest.json`;
    const response = await fetch(url, { method: 'GET', signal: controller.signal });
    if (!response.ok) {
      return { score: null, grade: null };
    }
    const payload = (await response.json()) as Record<string, unknown>;
    return {
      score: typeof payload.score === 'number' ? payload.score : null,
      grade: typeof payload.grade === 'string' ? payload.grade : null
    };
  } catch {
    return { score: null, grade: null };
  } finally {
    clearTimeout(timer);
  }
}

function isBaselineEntrant(name: string): boolean {
  return BASELINE_ENTRANT_NAMES.has(name.trim().toLowerCase());
}

function escapeHtml(value: string): string {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function formatInt(value: number): string {
  return new Intl.NumberFormat('en-US').format(Math.trunc(value));
}

function cacheControlHeader() {
  return `public, max-age=${PUBLIC_CACHE_SECONDS}`;
}

function requestIp(headers: Record<string, string | string[] | undefined>, fallback: string | undefined): string {
  const fwd = headers['x-forwarded-for'];
  const raw = Array.isArray(fwd) ? String(fwd[0] ?? '') : String(fwd ?? '');
  const first = raw.split(',')[0]?.trim();
  if (first) return first;
  const realIp = headers['x-real-ip'];
  if (typeof realIp === 'string' && realIp.trim()) return realIp.trim();
  return String(fallback ?? 'unknown');
}

function asFormBody(value: unknown): Record<string, string> {
  if (!value || typeof value !== 'object') return {};
  const out: Record<string, string> = {};
  for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
    if (typeof raw === 'string') out[key] = raw.trim();
  }
  return out;
}

function issueApiKeyPlaintext(): string {
  return `a2ab_${crypto.randomBytes(24).toString('hex')}`;
}

async function enforceRequestKeyLimit(ip: string): Promise<void> {
  const day = new Date().toISOString().slice(0, 10);
  const ipHash = crypto.createHash('sha256').update(ip).digest('hex').slice(0, 20);
  const docRef = db.collection('request_key_limits').doc(`${day}_${ipHash}`);
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(docRef);
    const current = Number(snap.get('count') ?? 0);
    if (current >= REQUEST_KEY_IP_DAILY_LIMIT) {
      throw new Error('Daily request-key limit reached for this IP.');
    }
    tx.set(docRef, {
      day,
      ip_hash: ipHash,
      count: current + 1,
      updated_at: FieldValue.serverTimestamp()
    }, { merge: true });
  });
}

async function issueEntrantApiKey(input: {
  entrantName: string;
  email: string;
  organization: string;
  ip: string;
  userAgent: string;
}): Promise<{ apiKey: string; entrantId: string }> {
  const entrantLc = input.entrantName.toLowerCase();
  const existing = await db.collection('entrants').where('entrant_name_lc', '==', entrantLc).limit(1).get();
  const apiKey = issueApiKeyPlaintext();
  const payload = {
    entrant_name: input.entrantName,
    entrant_name_lc: entrantLc,
    contact_email: input.email,
    organization: input.organization,
    api_key_hash: hashApiKey(apiKey),
    api_key_prefix: apiKey.slice(0, 12),
    status: 'active',
    last_issued_at: FieldValue.serverTimestamp(),
    last_issued_ip: input.ip,
    last_issued_user_agent: input.userAgent
  };

  if (!existing.empty) {
    const doc = existing.docs[0];
    await doc.ref.set(payload, { merge: true });
    return { apiKey, entrantId: doc.id };
  }

  const created = await db.collection('entrants').add({
    ...payload,
    created_at: FieldValue.serverTimestamp()
  });
  return { apiKey, entrantId: created.id };
}

async function createFeedbackIssue(input: {
  title: string;
  body: string;
}): Promise<{ issueNumber: number; issueUrl: string }> {
  if (!FEEDBACK_GITHUB_TOKEN) {
    throw new Error('FEEDBACK_GITHUB_TOKEN is not configured');
  }
  const response = await fetch(`https://api.github.com/repos/${FEEDBACK_GITHUB_REPO}/issues`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${FEEDBACK_GITHUB_TOKEN}`,
      'content-type': 'application/json',
      accept: 'application/vnd.github+json',
      'user-agent': 'a2abench-feedback-bot'
    },
    body: JSON.stringify({
      title: input.title,
      body: input.body
    })
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`GitHub issue create failed (${response.status}): ${text.slice(0, 180)}`);
  }
  const parsed = await response.json() as { number?: unknown; html_url?: unknown };
  return {
    issueNumber: Number(parsed.number ?? 0),
    issueUrl: String(parsed.html_url ?? '')
  };
}

async function countCollection(name: string): Promise<number> {
  const snapshot = await db.collection(name).count().get();
  return Number(snapshot.data().count ?? 0);
}

async function loadLeaderboardRows(limit: number): Promise<LeaderboardRow[]> {
  const scanLimit = Math.max(limit * 5, 500);
  const snap = await db.collection('runs').orderBy('completed_at', 'desc').limit(scanLimit).get();
  const completed = snap.docs
    .map((doc) => ({ doc, data: doc.data() }))
    .filter(({ data }) => String(data.status ?? '') === 'completed')
    .sort((a, b) => {
      const scoreA = Number(a.data.total_score ?? 0);
      const scoreB = Number(b.data.total_score ?? 0);
      if (scoreA !== scoreB) return scoreB - scoreA;
      return String(a.doc.id).localeCompare(String(b.doc.id));
    });

  const ranked = completed.slice(0, limit);
  const topEntrants = new Set(
    ranked.map(({ data }) => String(data.entrant_name ?? '').trim().toLowerCase()).filter(Boolean)
  );

  for (const baselineName of BASELINE_ENTRANT_NAMES) {
    if (topEntrants.has(baselineName)) continue;
    const bestBaseline = completed.find(
      ({ data }) => String(data.entrant_name ?? '').trim().toLowerCase() === baselineName
    );
    if (bestBaseline) {
      ranked.push(bestBaseline);
      topEntrants.add(baselineName);
    }
  }

  return ranked.map(({ doc, data }, idx) => ({
    rank: idx + 1,
    run_id: doc.id,
    entrant_name: String(data.entrant_name ?? 'unknown'),
    score: Number(data.total_score ?? 0),
    question_count: Number(data.question_count ?? 0),
    date: toIso(data.completed_at)
  }));
}

async function loadDistinctEntrantsAndBaselineRuns(): Promise<{
  entrants: number;
  baselineRuns: number;
  totalCompletedRuns: number;
}> {
  const entrants = new Set<string>();
  let baselineRuns = 0;
  let totalCompletedRuns = 0;
  let cursor: FirebaseFirestore.QueryDocumentSnapshot | undefined;

  for (;;) {
    let query: FirebaseFirestore.Query = db
      .collection('runs')
      .orderBy('completed_at', 'desc')
      .limit(500);
    if (cursor) query = query.startAfter(cursor);
    const page = await query.get();
    if (page.empty) break;

    cursor = page.docs[page.docs.length - 1];
    for (const doc of page.docs) {
      const data = doc.data();
      if (String(data.status ?? '') !== 'completed') continue;
      totalCompletedRuns += 1;
      const entrant = String(data.entrant_name ?? '').trim();
      if (!entrant) continue;
      if (isBaselineEntrant(entrant)) {
        baselineRuns += 1;
      } else {
        entrants.add(entrant.toLowerCase());
      }
    }
  }

  return {
    entrants: entrants.size,
    baselineRuns,
    totalCompletedRuns
  };
}

async function loadLastSubmissionTimestamp(): Promise<string | null> {
  const submissionsSnap = await db.collection('submissions').orderBy('submitted_at', 'desc').limit(1).get();
  if (!submissionsSnap.empty) {
    const row = submissionsSnap.docs[0];
    return toIso(row.get('submitted_at')) || toIso(row.get('created_at'));
  }
  const runsSnap = await db.collection('runs').orderBy('completed_at', 'desc').limit(1).get();
  if (!runsSnap.empty) {
    return toIso(runsSnap.docs[0].get('completed_at'));
  }
  return null;
}

async function loadPublicStats(force = false): Promise<PublicStatsPayload> {
  const now = Date.now();
  if (!force && publicStatsCache && publicStatsCache.expiresAtMs > now) {
    return publicStatsCache.payload;
  }

  const [submissions, keysIssued, feedbackCount, top10, entrantStats, lastSubmissionTs] = await Promise.all([
    countCollection('submissions'),
    countCollection('entrants'),
    countCollection('feedback'),
    loadLeaderboardRows(10),
    loadDistinctEntrantsAndBaselineRuns(),
    loadLastSubmissionTimestamp()
  ]);

  const generatedAt = new Date().toISOString();
  const payload: PublicStatsPayload = {
    as_of_utc: generatedAt,
    generated_at: generatedAt,
    submissions,
    entrants: entrantStats.entrants,
    keys_issued: keysIssued,
    feedback_count: feedbackCount,
    baseline_runs: entrantStats.baselineRuns,
    total_completed_runs: entrantStats.totalCompletedRuns,
    last_submission_ts: lastSubmissionTs,
    top10
  };

  publicStatsCache = {
    expiresAtMs: now + PUBLIC_CACHE_SECONDS * 1000,
    payload
  };
  return payload;
}

function renderLeaderboardRows(rows: LeaderboardRow[]): string {
  if (!rows.length) return '<tr><td colspan="5">No runs yet.</td></tr>';
  return rows
    .map((row) => {
      const date = row.date ? row.date.slice(0, 10) : '-';
      const runIdSafe = escapeHtml(row.run_id);
      const entrantSafe = escapeHtml(row.entrant_name);
      return [
        '<tr>',
        `<td class="num">${row.rank}</td>`,
        `<td>${entrantSafe}</td>`,
        `<td class="num">${row.score.toFixed(2)}</td>`,
        `<td>${date}</td>`,
        `<td><a href="/v1/eval/leaderboard?run=${encodeURIComponent(row.run_id)}">${runIdSafe.slice(0, 12)}</a></td>`,
        '</tr>'
      ].join('');
    })
    .join('');
}

function renderHomeHtml(stats: PublicStatsPayload, audit: AgentabilityReportSummary): string {
  const leaderboardRows = renderLeaderboardRows(stats.top10);
  const auditLabel =
    typeof audit.score === 'number'
      ? `Audited by Agentability - score ${audit.score.toFixed(1)}/100${audit.grade ? ` (${escapeHtml(audit.grade)})` : ''} (full report ->)`
      : 'Audited by Agentability (full report ->)';
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>A2ABench - Agent Q&A Benchmark</title>
    <meta name="description" content="A2ABench public benchmark. ${formatInt(stats.submissions)} submissions, ${formatInt(stats.entrants)} external entrants, ${formatInt(stats.keys_issued)} keys issued." />
    <meta property="og:title" content="A2ABench - Agent Q&A Benchmark" />
    <meta property="og:description" content="Live counters: ${formatInt(stats.submissions)} submissions, ${formatInt(stats.entrants)} external entrants, ${formatInt(stats.keys_issued)} keys issued." />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="A2ABench - Agent Q&A Benchmark" />
    <meta name="twitter:description" content="Live counters: ${formatInt(stats.submissions)} submissions, ${formatInt(stats.entrants)} external entrants, ${formatInt(stats.keys_issued)} keys issued." />
    <style>
      :root {
        --bg: #f6f6f4;
        --panel: #ffffff;
        --ink: #101010;
        --muted: #545454;
        --line: #d8d8d2;
        --accent: #005fd1;
      }

      * { box-sizing: border-box; }
      body {
        margin: 0;
        background: radial-gradient(circle at 10% 10%, #ffffff 0%, #f0f0eb 32%, var(--bg) 100%);
        color: var(--ink);
        font-family: "IBM Plex Mono", "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
      }
      .wrap {
        max-width: 1024px;
        margin: 0 auto;
        padding: 40px 20px 64px;
      }
      h1 {
        margin: 0;
        font-size: clamp(2rem, 6vw, 3rem);
        letter-spacing: -0.03em;
      }
      p {
        line-height: 1.6;
        color: var(--muted);
      }
      .hero {
        border: 1px solid var(--line);
        background: var(--panel);
        padding: 20px;
      }
      .small {
        font-size: 0.88rem;
      }
      .table-wrap {
        margin-top: 24px;
        overflow-x: auto;
        border: 1px solid var(--line);
        background: var(--panel);
      }
      table {
        width: 100%;
        border-collapse: collapse;
        min-width: 760px;
      }
      th, td {
        text-align: left;
        padding: 10px 12px;
        border-bottom: 1px solid var(--line);
        white-space: nowrap;
      }
      th { color: var(--muted); font-weight: 500; }
      td.num { text-align: right; font-variant-numeric: tabular-nums; }
      a { color: var(--accent); text-decoration: none; }
      a:hover { text-decoration: underline; }
      .submit {
        margin-top: 24px;
        border: 1px solid var(--line);
        background: var(--panel);
        padding: 20px;
      }
      .stats-grid {
        margin-top: 14px;
        display: grid;
        gap: 10px;
        grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      }
      .stat-card {
        border: 1px solid var(--line);
        background: var(--panel);
        padding: 12px;
      }
      .stat-card .k { color: var(--muted); font-size: 0.8rem; }
      .stat-card .v { margin-top: 5px; font-size: 1.25rem; }
      .link-grid {
        margin-top: 10px;
        display: flex;
        flex-wrap: wrap;
        gap: 8px 14px;
      }
      .footer {
        margin-top: 28px;
        color: var(--muted);
      }
    </style>
  </head>
  <body>
    <main class="wrap">
      <section class="hero">
        <h1>A2ABench - Agent Q&A Benchmark</h1>
        <p>
          A2ABench is a public benchmark for agent question-answering performance. Submit your agent's
          answers to a curated set of Stack Overflow developer questions with accepted-answer references and
          get a public score on the leaderboard.
        </p>
        <p class="small">API: <a href="/v1/eval/questions"><code>/v1/eval/questions</code></a>, <code>POST /v1/eval/submit</code>, <a href="/v1/eval/leaderboard"><code>/v1/eval/leaderboard</code></a></p>
        <div class="link-grid small">
          <a href="/stats">Public stats (HTML)</a>
          <a href="/stats.json">Public stats (JSON)</a>
          <a href="/llms.txt">LLMs.txt</a>
          <a href="/.well-known/agent.json">Agent card</a>
          <a href="https://ragmap-api.web.app/stats">Ragmap stats</a>
          <a href="https://rootfetch.com/stats">Rootfetch stats</a>
          <a href="https://agentability.org/stats">Agentability stats</a>
        </div>
      </section>

      <section class="stats-grid" aria-label="Public counters">
        <div class="stat-card"><div class="k">Total submissions</div><div class="v">${formatInt(stats.submissions)}</div></div>
        <div class="stat-card"><div class="k">Distinct external entrants</div><div class="v">${formatInt(stats.entrants)}</div></div>
        <div class="stat-card"><div class="k">API keys issued</div><div class="v">${formatInt(stats.keys_issued)}</div></div>
        <div class="stat-card"><div class="k">Feedback issues opened</div><div class="v">${formatInt(stats.feedback_count)}</div></div>
      </section>

      <section class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Rank</th>
              <th>Entrant</th>
              <th>Score</th>
              <th>Date</th>
              <th>Run</th>
            </tr>
          </thead>
          <tbody id="rows">${leaderboardRows}</tbody>
        </table>
      </section>

      <section class="submit">
        <h2 style="margin-top:0">Submit your agent</h2>
        <p>Read benchmark format and scoring in <a href="https://github.com/khalidsaidi/a2abench/blob/main/BENCHMARK.md">BENCHMARK.md</a>.</p>
        <p><a href="/request-key">Get a benchmark API key</a></p>
        <p><a href="/feedback">Send feedback or report an issue</a></p>
        <p><a href="https://agentability.org/reports/a2abench-api.web.app" aria-label="Agentability report for A2ABench">${auditLabel}</a></p>
      </section>

      <p class="footer small"><a href="https://ragmap-api.web.app/">Related: Ragmap</a> - search engine for MCP servers.</p>
      <p class="footer small"><a href="https://rootfetch.com/">Related: Rootfetch</a> - delegation intelligence for DNS-visible evidence.</p>
      <p class="footer small"><a href="https://agentability.org/">Related: Agentability</a> - agent-readiness audit and report engine.</p>
      <p class="footer small">Server-rendered at ${escapeHtml(stats.as_of_utc)} (cache ${PUBLIC_CACHE_SECONDS}s).</p>
    </main>
  </body>
</html>`;
}

function renderStatsHtml(stats: PublicStatsPayload): string {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench stats</title>
    <meta name="description" content="A2ABench public counters and baseline/external split." />
    <style>
      body { margin: 32px auto; max-width: 860px; padding: 0 16px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; color: #121212; }
      h1 { margin-bottom: 4px; }
      .muted { color: #666; }
      table { width: 100%; border-collapse: collapse; margin-top: 16px; }
      th, td { border: 1px solid #d9d9d9; padding: 8px 10px; text-align: left; }
      th { background: #f4f4f4; }
      .num { text-align: right; font-variant-numeric: tabular-nums; }
      a { color: #0b57d0; }
    </style>
  </head>
  <body>
    <h1>A2ABench stats</h1>
    <p class="muted">Server-rendered ${escapeHtml(stats.as_of_utc)} · JSON: <a href="/stats.json">/stats.json</a></p>
    <table>
      <tbody>
        <tr><th>Submissions</th><td class="num">${formatInt(stats.submissions)}</td></tr>
        <tr><th>Distinct external entrants</th><td class="num">${formatInt(stats.entrants)}</td></tr>
        <tr><th>API keys issued</th><td class="num">${formatInt(stats.keys_issued)}</td></tr>
        <tr><th>Feedback issues opened</th><td class="num">${formatInt(stats.feedback_count)}</td></tr>
        <tr><th>Baseline runs</th><td class="num">${formatInt(stats.baseline_runs)}</td></tr>
        <tr><th>Total completed runs</th><td class="num">${formatInt(stats.total_completed_runs)}</td></tr>
      </tbody>
    </table>
    <p><a href="/">Back to homepage</a> · <a href="https://ragmap-api.web.app/stats">Ragmap stats</a> · <a href="https://rootfetch.com/stats">Rootfetch stats</a> · <a href="https://agentability.org/stats">Agentability stats</a></p>
  </body>
</html>`;
}

function renderRequestKeyHtml(input: { error?: string; apiKey?: string; entrantName?: string; email?: string; organization?: string }) {
  const error = input.error ? `<p class="err">${escapeHtml(input.error)}</p>` : '';
  const issued = input.apiKey
    ? `<section class="ok"><h2>Key issued</h2><p>Entrant: <strong>${escapeHtml(input.entrantName ?? '')}</strong></p><p>Your API key (shown once):</p><pre>${escapeHtml(input.apiKey)}</pre><p>Use as <code>Authorization: Bearer &lt;key&gt;</code> or <code>X-API-Key</code>.</p></section>`
    : '';
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench request key</title>
    <style>
      body { margin: 32px auto; max-width: 760px; padding: 0 16px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; color: #111; }
      form { border: 1px solid #d9d9d9; padding: 14px; border-radius: 10px; background: #fff; }
      label { display: block; margin-bottom: 10px; }
      input, textarea, button { width: 100%; padding: 8px 10px; font: inherit; }
      button { cursor: pointer; }
      .ok { margin-top: 14px; border: 1px solid #cce7d0; background: #f4fff5; padding: 12px; border-radius: 10px; }
      .err { margin-top: 12px; border: 1px solid #efb7b7; background: #fff4f4; padding: 10px; border-radius: 8px; }
      pre { overflow-x: auto; white-space: pre-wrap; word-break: break-all; background: #f7f7f7; padding: 8px; border-radius: 8px; }
      a { color: #0b57d0; }
    </style>
  </head>
  <body>
    <h1>Request A2ABench API key</h1>
    <p>Auto-issued immediately. No manual approval.</p>
    ${error}
    <form method="post" action="/request-key">
      <label>Entrant name
        <input name="entrant_name" required maxlength="80" value="${escapeHtml(input.entrantName ?? '')}" />
      </label>
      <label>Email
        <input type="email" name="email" required maxlength="180" value="${escapeHtml(input.email ?? '')}" />
      </label>
      <label>Organization (optional)
        <input name="organization" maxlength="120" value="${escapeHtml(input.organization ?? '')}" />
      </label>
      <button type="submit">Issue key</button>
    </form>
    ${issued}
    <p><a href="/">Back to homepage</a> · <a href="/stats">Public stats</a></p>
  </body>
</html>`;
}

function renderFeedbackHtml(input: { error?: string; ok?: string; title?: string; email?: string; message?: string; issueUrl?: string }) {
  const error = input.error ? `<p class="err">${escapeHtml(input.error)}</p>` : '';
  const ok = input.ok
    ? `<p class="ok">${escapeHtml(input.ok)}${input.issueUrl ? ` <a href="${escapeHtml(input.issueUrl)}" target="_blank" rel="noreferrer">Open issue</a>` : ''}</p>`
    : '';
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench feedback</title>
    <style>
      body { margin: 32px auto; max-width: 760px; padding: 0 16px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; color: #111; }
      form { border: 1px solid #d9d9d9; padding: 14px; border-radius: 10px; background: #fff; }
      label { display: block; margin-bottom: 10px; }
      input, textarea, button { width: 100%; padding: 8px 10px; font: inherit; }
      textarea { min-height: 140px; }
      button { cursor: pointer; }
      .ok { margin-top: 12px; border: 1px solid #cce7d0; background: #f4fff5; padding: 10px; border-radius: 8px; }
      .err { margin-top: 12px; border: 1px solid #efb7b7; background: #fff4f4; padding: 10px; border-radius: 8px; }
      a { color: #0b57d0; }
    </style>
  </head>
  <body>
    <h1>Feedback</h1>
    <p>Submits directly to the GitHub issue tracker.</p>
    ${error}
    ${ok}
    <form method="post" action="/feedback">
      <label>Title
        <input name="title" required maxlength="140" value="${escapeHtml(input.title ?? '')}" />
      </label>
      <label>Contact email (optional)
        <input type="email" name="email" maxlength="180" value="${escapeHtml(input.email ?? '')}" />
      </label>
      <label>Details
        <textarea name="message" required maxlength="5000">${escapeHtml(input.message ?? '')}</textarea>
      </label>
      <button type="submit">Open feedback issue</button>
    </form>
    <p><a href="/">Back to homepage</a> · <a href="/stats">Public stats</a></p>
  </body>
</html>`;
}

function sitemapXml(baseUrl: string): string {
  const now = new Date().toISOString();
  const urls = [
    '/',
    '/stats',
    '/stats.json',
    '/v1/eval/leaderboard',
    '/.well-known/agent.json',
    '/request-key',
    '/feedback',
    '/BENCHMARK.md'
  ];
  const body = urls
    .map((url) => `  <url><loc>${escapeHtml(`${baseUrl}${url}`)}</loc><lastmod>${now}</lastmod></url>`)
    .join('\n');
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${body}\n</urlset>\n`;
}

const fastify = Fastify({ logger: true });
await fastify.register(cors, { origin: true });
await fastify.register(formbody);
fastify.addHook('onSend', async (request, reply, payload) => {
  const contentType = String(reply.getHeader('content-type') ?? '').toLowerCase();
  if (!contentType.includes('text/html')) return payload;
  if (typeof payload !== 'string') return payload;
  return attachCrossProjectFooter(payload);
});

fastify.get('/', async (_request, reply) => {
  const [stats, audit] = await Promise.all([
    loadPublicStats(),
    fetchAgentabilityReportSummary('a2abench-api.web.app')
  ]);
  reply.header('Cache-Control', cacheControlHeader());
  reply.type('text/html').send(renderHomeHtml(stats, audit));
});

fastify.get('/stats.json', async (_request, reply) => {
  const stats = await loadPublicStats();
  reply.header('Cache-Control', cacheControlHeader());
  reply.code(200).send({
    submissions: stats.submissions,
    entrants_external: stats.entrants,
    keys_issued: stats.keys_issued,
    feedback_count: stats.feedback_count,
    baseline_runs: stats.baseline_runs,
    last_submission_ts: stats.last_submission_ts,
    generated_at: stats.generated_at,
    siblings: siblingLinksForStats()
  });
});

fastify.get('/stats', async (_request, reply) => {
  const stats = await loadPublicStats();
  reply.header('Cache-Control', cacheControlHeader());
  reply.type('text/html').send(renderStatsHtml(stats));
});

fastify.get('/request-key', async (_request, reply) => {
  reply.header('Cache-Control', 'no-store');
  reply.type('text/html').send(renderRequestKeyHtml({}));
});

fastify.post('/request-key', async (request, reply) => {
  const form = asFormBody(request.body);
  const entrantName = (form.entrant_name ?? '').trim();
  const email = (form.email ?? '').trim();
  const organization = (form.organization ?? '').trim();
  if (!entrantName || entrantName.length > 80) {
    reply.code(400).header('Cache-Control', 'no-store').type('text/html').send(
      renderRequestKeyHtml({ error: 'Entrant name is required (max 80 chars).', entrantName, email, organization })
    );
    return;
  }
  if (!email || email.length > 180 || !email.includes('@')) {
    reply.code(400).header('Cache-Control', 'no-store').type('text/html').send(
      renderRequestKeyHtml({ error: 'Valid email is required.', entrantName, email, organization })
    );
    return;
  }
  const ip = requestIp(request.headers, request.ip);
  const userAgent = String(request.headers['user-agent'] ?? '');
  try {
    await enforceRequestKeyLimit(ip);
    const issued = await issueEntrantApiKey({ entrantName, email, organization, ip, userAgent });
    reply.header('Cache-Control', 'no-store');
    reply.type('text/html').send(
      renderRequestKeyHtml({ apiKey: issued.apiKey, entrantName, email, organization })
    );
  } catch (error) {
    reply.code(429).header('Cache-Control', 'no-store').type('text/html').send(
      renderRequestKeyHtml({
        error: error instanceof Error ? error.message : 'Failed to issue API key.',
        entrantName,
        email,
        organization
      })
    );
  }
});

fastify.get('/feedback', async (_request, reply) => {
  reply.header('Cache-Control', 'no-store');
  reply.type('text/html').send(renderFeedbackHtml({}));
});

fastify.post('/feedback', async (request, reply) => {
  const form = asFormBody(request.body);
  const title = (form.title ?? '').trim();
  const email = (form.email ?? '').trim();
  const message = (form.message ?? '').trim();
  if (!title || title.length > 140) {
    reply.code(400).header('Cache-Control', 'no-store').type('text/html').send(
      renderFeedbackHtml({ error: 'Title is required (max 140 chars).', title, email, message })
    );
    return;
  }
  if (!message || message.length > 5000) {
    reply.code(400).header('Cache-Control', 'no-store').type('text/html').send(
      renderFeedbackHtml({ error: 'Message is required (max 5000 chars).', title, email, message })
    );
    return;
  }

  const ip = requestIp(request.headers, request.ip);
  const userAgent = String(request.headers['user-agent'] ?? '');
  const issueTitle = `[feedback] ${title}`;
  const issueBody = [
    `Reporter: ${email || 'anonymous'}`,
    `IP: ${ip}`,
    `User-Agent: ${userAgent || 'unknown'}`,
    '',
    message,
    '',
    FEEDBACK_GITHUB_MENTION ? `cc ${FEEDBACK_GITHUB_MENTION}` : ''
  ].join('\n');

  try {
    const issue = await createFeedbackIssue({ title: issueTitle, body: issueBody });
    await db.collection('feedback').add({
      title,
      email: email || null,
      message,
      issue_number: issue.issueNumber,
      issue_url: issue.issueUrl,
      created_at: FieldValue.serverTimestamp(),
      ip,
      user_agent: userAgent
    });
    reply.header('Cache-Control', 'no-store');
    reply.type('text/html').send(
      renderFeedbackHtml({
        ok: `Issue #${issue.issueNumber} created.`,
        issueUrl: issue.issueUrl
      })
    );
  } catch (error) {
    reply.code(500).header('Cache-Control', 'no-store').type('text/html').send(
      renderFeedbackHtml({
        error: error instanceof Error ? error.message : 'Failed to create feedback issue.',
        title,
        email,
        message
      })
    );
  }
});

fastify.get('/robots.txt', async (_request, reply) => {
  reply.header('Cache-Control', cacheControlHeader());
  reply.type('text/plain').send(
    `User-agent: *\n` +
      `Allow: /\n` +
      `Allow: /stats\n` +
      `Allow: /stats.json\n` +
      `Sitemap: ${PUBLIC_BASE_URL}/sitemap.xml\n`
  );
});

fastify.get('/sitemap.xml', async (_request, reply) => {
  reply.header('Cache-Control', cacheControlHeader());
  reply.type('application/xml').send(sitemapXml(PUBLIC_BASE_URL));
});

fastify.get('/llms.txt', async (_request, reply) => {
  reply.header('Cache-Control', cacheControlHeader());
  reply.type('text/plain').send(
    `# A2ABench\n\n` +
      `A2ABench is a public Q&A benchmark for agent submissions.\n\n` +
      `## Public endpoints\n` +
      `- ${PUBLIC_BASE_URL}/stats\n` +
      `- ${PUBLIC_BASE_URL}/stats.json\n` +
      `- ${PUBLIC_BASE_URL}/v1/eval/questions\n` +
      `- ${PUBLIC_BASE_URL}/v1/eval/leaderboard\n` +
      `- ${PUBLIC_BASE_URL}/request-key\n` +
      `- ${PUBLIC_BASE_URL}/feedback\n\n` +
      `## Related projects\n` +
      `- Ragmap: ${SIBLING_RAGMAP_URL} (stats: ${SIBLING_RAGMAP_URL}/stats)\n` +
      `- Rootfetch: ${SIBLING_ROOTFETCH_URL} (stats: ${SIBLING_ROOTFETCH_URL}/stats)\n` +
      `- Agentability: ${SIBLING_AGENTABILITY_URL} (stats: ${SIBLING_AGENTABILITY_URL}/stats)\n` +
      `- RelayOrb: ${SIBLING_RELAYORB_URL} (stats: ${SIBLING_RELAYORB_URL}/stats)\n` +
      `- AIStatusDashboard: ${SIBLING_AISTATUSDASHBOARD_URL} (stats: ${SIBLING_AISTATUSDASHBOARD_URL}/stats)\n`
  );
});

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
  ],
  related: relatedProjectsForAgentCard()
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
  ],
  related: relatedProjectsForAgentCard()
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
  const results = await loadLeaderboardRows(limit);
  reply.header('Cache-Control', cacheControlHeader());
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
