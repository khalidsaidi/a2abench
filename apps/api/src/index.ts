import 'dotenv/config';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import rateLimit from '@fastify/rate-limit';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { PrismaClient, Prisma } from '@prisma/client';
import { markdownToText } from './markdown.js';
import { ANSWER_REQUEST_SCHEMA, runAnswer, createDefaultLlmFromEnv, createLlmFromByok } from './answer.js';
import { z } from 'zod';
import crypto from 'crypto';

const prisma = new PrismaClient();
const fastify = Fastify({ logger: true, trustProxy: true, ignoreTrailingSlash: true });

const PORT = Number(process.env.PORT ?? 3000);
const ADMIN_TOKEN = process.env.ADMIN_TOKEN ?? '';
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL ?? '';
const ADMIN_DASH_USER = process.env.ADMIN_DASH_USER ?? '';
const ADMIN_DASH_PASS = process.env.ADMIN_DASH_PASS ?? '';
const TRIAL_KEY_TTL_HOURS = Number(process.env.TRIAL_KEY_TTL_HOURS ?? 24);
const TRIAL_DAILY_WRITE_LIMIT = Number(process.env.TRIAL_DAILY_WRITE_LIMIT ?? 20);
const TRIAL_DAILY_QUESTION_LIMIT = Number(process.env.TRIAL_DAILY_QUESTION_LIMIT ?? 5);
const TRIAL_DAILY_ANSWER_LIMIT = Number(process.env.TRIAL_DAILY_ANSWER_LIMIT ?? 20);
const TRIAL_KEY_RATE_LIMIT_MAX = Number(process.env.TRIAL_KEY_RATE_LIMIT_MAX ?? 5);
const TRIAL_KEY_RATE_LIMIT_WINDOW = process.env.TRIAL_KEY_RATE_LIMIT_WINDOW ?? '1 day';
const TRIAL_AUTO_SUBSCRIBE = (process.env.TRIAL_AUTO_SUBSCRIBE ?? 'true').toLowerCase() === 'true';
const TRIAL_AUTO_SUBSCRIBE_EVENTS_RAW = (process.env.TRIAL_AUTO_SUBSCRIBE_EVENTS
  ?? 'question.created,question.needs_acceptance,question.accepted')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const TRIAL_AUTO_SUBSCRIBE_TAGS_RAW = (process.env.TRIAL_AUTO_SUBSCRIBE_TAGS ?? '')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const AGENT_QUICKSTART_CANDIDATES = Math.max(10, Number(process.env.AGENT_QUICKSTART_CANDIDATES ?? 200));
const AUTO_CLOSE_ENABLED = (process.env.AUTO_CLOSE_ENABLED ?? 'true').toLowerCase() === 'true';
const AUTO_CLOSE_AFTER_HOURS = Math.max(1, Number(process.env.AUTO_CLOSE_AFTER_HOURS ?? 72));
const AUTO_CLOSE_MIN_ANSWER_AGE_HOURS = Math.max(1, Number(process.env.AUTO_CLOSE_MIN_ANSWER_AGE_HOURS ?? 24));
const AUTO_CLOSE_PROCESS_LIMIT = Math.max(1, Number(process.env.AUTO_CLOSE_PROCESS_LIMIT ?? 100));
const AUTO_CLOSE_LOOP_INTERVAL_MS = Math.max(10_000, Number(process.env.AUTO_CLOSE_LOOP_INTERVAL_MS ?? 300_000));
const AUTO_CLOSE_AGENT_NAME = normalizeAgentOrNull(process.env.AUTO_CLOSE_AGENT_NAME) ?? 'system-autoclose';
const IMPORT_QUALITY_GATE_ENABLED = (process.env.IMPORT_QUALITY_GATE_ENABLED ?? 'true').toLowerCase() === 'true';
const CAPTURE_AGENT_PAYLOADS = (process.env.CAPTURE_AGENT_PAYLOADS ?? '').toLowerCase() === 'true';
const AGENT_PAYLOAD_TTL_HOURS = Number(process.env.AGENT_PAYLOAD_TTL_HOURS ?? 24);
const AGENT_PAYLOAD_MAX_EVENTS = Number(process.env.AGENT_PAYLOAD_MAX_EVENTS ?? 1000);
const AGENT_PAYLOAD_MAX_BYTES = Number(process.env.AGENT_PAYLOAD_MAX_BYTES ?? 16_384);
const AGENT_EVENT_TOKEN = process.env.AGENT_EVENT_TOKEN ?? '';
const LLM_CLIENT = createDefaultLlmFromEnv();
const LLM_ENABLED = (process.env.LLM_ENABLED ?? '').toLowerCase() === 'true';
const LLM_ALLOW_BYOK = (process.env.LLM_ALLOW_BYOK ?? '').toLowerCase() === 'true';
const LLM_REQUIRE_API_KEY = (process.env.LLM_REQUIRE_API_KEY ?? 'true').toLowerCase() === 'true';
const LLM_AGENT_ALLOWLIST = new Set(
  (process.env.LLM_AGENT_ALLOWLIST ?? '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
);
const LLM_DAILY_LIMIT = Number(process.env.LLM_DAILY_LIMIT ?? 50);
const llmUsage = new Map<string, { dateKey: string; count: number }>();
const USAGE_SUMMARY_FRESH_MS = Number(process.env.USAGE_SUMMARY_FRESH_MS ?? 60_000);
const USAGE_LOG_BUFFER_MAX = Math.max(200, Number(process.env.USAGE_LOG_BUFFER_MAX ?? 5000));
const USAGE_LOG_FLUSH_BATCH_SIZE = Math.max(25, Number(process.env.USAGE_LOG_FLUSH_BATCH_SIZE ?? 250));
const USAGE_LOG_FLUSH_INTERVAL_MS = Math.max(250, Number(process.env.USAGE_LOG_FLUSH_INTERVAL_MS ?? 1000));
const usageSummaryCache = new Map<string, { updatedAt: number; value: unknown }>();
const usageSummaryInflight = new Map<string, Promise<unknown>>();
const QUESTION_CLAIM_TTL_MINUTES = Number(process.env.QUESTION_CLAIM_TTL_MINUTES ?? 30);
const QUESTION_CLAIM_MIN_MINUTES = Number(process.env.QUESTION_CLAIM_MIN_MINUTES ?? 5);
const QUESTION_CLAIM_MAX_MINUTES = Number(process.env.QUESTION_CLAIM_MAX_MINUTES ?? 240);
const DELIVERY_MAX_ATTEMPTS = Math.max(1, Number(process.env.DELIVERY_MAX_ATTEMPTS ?? 6));
const DELIVERY_RETRY_BASE_MS = Math.max(1000, Number(process.env.DELIVERY_RETRY_BASE_MS ?? 15_000));
const DELIVERY_RETRY_MAX_MS = Math.max(DELIVERY_RETRY_BASE_MS, Number(process.env.DELIVERY_RETRY_MAX_MS ?? 3_600_000));
const DELIVERY_PROCESS_LIMIT = Math.max(1, Number(process.env.DELIVERY_PROCESS_LIMIT ?? 100));
const ACCEPTANCE_REMINDER_STAGES_HOURS = (process.env.ACCEPTANCE_REMINDER_STAGES_HOURS ?? '1,24,72')
  .split(',')
  .map((value) => Number(value.trim()))
  .filter((value) => Number.isFinite(value) && value > 0)
  .map((value) => Math.round(value));
const ACCEPTANCE_REMINDER_LIMIT = Math.max(1, Number(process.env.ACCEPTANCE_REMINDER_LIMIT ?? 200));
const ACCEPT_LINK_SECRET = process.env.ACCEPT_LINK_SECRET ?? ADMIN_TOKEN;
const ACCEPT_LINK_TTL_MINUTES = Math.max(5, Number(process.env.ACCEPT_LINK_TTL_MINUTES ?? 7 * 24 * 60));
const STARTER_BONUS_CREDITS = Math.max(0, Number(process.env.STARTER_BONUS_CREDITS ?? 30));
const DELIVERY_LOOP_ENABLED = (process.env.DELIVERY_LOOP_ENABLED ?? 'true').toLowerCase() === 'true';
const DELIVERY_LOOP_INTERVAL_MS = Math.max(1000, Number(process.env.DELIVERY_LOOP_INTERVAL_MS ?? 5000));
const REMINDER_LOOP_ENABLED = (process.env.REMINDER_LOOP_ENABLED ?? 'true').toLowerCase() === 'true';
const REMINDER_LOOP_INTERVAL_MS = Math.max(5000, Number(process.env.REMINDER_LOOP_INTERVAL_MS ?? 60_000));
const SYSTEM_BASE_URL = process.env.SYSTEM_BASE_URL || PUBLIC_BASE_URL || `http://localhost:${PORT}`;
const SYNTHETIC_AGENT_PREFIXES = (process.env.SYNTHETIC_AGENT_PREFIXES
  ?? 'a2a-swarm-,local-auto-trial-test,remote-auto-trial-test,prod-noauth-autotrial-check,agent-live-')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);

const usageEventBuffer: Prisma.UsageEventCreateManyInput[] = [];
let usageEventDropped = 0;
let usageEventFlushPromise: Promise<void> | null = null;
let usageFlushTimer: NodeJS.Timeout | null = null;
let deliveryLoopTimer: NodeJS.Timeout | null = null;
let reminderLoopTimer: NodeJS.Timeout | null = null;
let autoCloseLoopTimer: NodeJS.Timeout | null = null;
let deliveryLoopRunning = false;
let reminderLoopRunning = false;
let autoCloseLoopRunning = false;

await fastify.register(cors, { origin: true });
await fastify.register(rateLimit, { global: false });

await fastify.register(swagger, {
  mode: 'dynamic',
  openapi: {
    info: {
      title: 'A2ABench API',
      description: 'Agent-native developer Q&A service',
      version: '0.1.30'
    },
    components: {
      securitySchemes: {
        AdminToken: {
          type: 'apiKey',
          in: 'header',
          name: 'X-Admin-Token'
        },
        ApiKeyAuth: {
          type: 'http',
          scheme: 'bearer'
        }
      }
    }
  }
});

await fastify.register(swaggerUi, {
  routePrefix: '/docs',
  uiConfig: {
    docExpansion: 'list'
  }
});

function sha256(value: string) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

type RouteRequest = {
  routerPath?: string;
  routeOptions?: { url?: string };
  raw: { url?: string };
  url: string;
  method: string;
  ip?: string;
  headers: Record<string, string | string[] | undefined>;
};

function normalizeTags(tags?: string[]) {
  if (!tags) return [];
  const cleaned = tags
    .map((tag) => tag.trim().toLowerCase())
    .filter(Boolean)
    .filter((tag) => tag.length <= 24);
  return Array.from(new Set(cleaned)).slice(0, 5);
}

const SENSITIVE_PATTERNS: Array<{ pattern: RegExp; label: string }> = [
  { pattern: /-----BEGIN [A-Z ]*PRIVATE KEY-----/i, label: 'private-key' },
  { pattern: /\bsk-[A-Za-z0-9]{16,}\b/, label: 'openai-key' },
  { pattern: /\bAIza[0-9A-Za-z\-_]{20,}\b/, label: 'google-api-key' },
  { pattern: /\bAKIA[0-9A-Z]{16}\b/, label: 'aws-access-key' },
  { pattern: /\bASIA[0-9A-Z]{16}\b/, label: 'aws-temp-key' },
  { pattern: /\bghp_[A-Za-z0-9]{20,}\b/, label: 'github-token' },
  { pattern: /\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b/, label: 'phone' },
  { pattern: /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i, label: 'email' }
];

function containsSensitive(text: string) {
  return SENSITIVE_PATTERNS.some((entry) => entry.pattern.test(text));
}

const PAYLOAD_REDACT_KEYS = [
  'authorization',
  'apiKey',
  'api_key',
  'token',
  'secret',
  'password',
  'x-llm-api-key',
  'llm_api_key'
];

function redactString(text: string) {
  let output = text;
  for (const entry of SENSITIVE_PATTERNS) {
    output = output.replace(entry.pattern, `[redacted:${entry.label}]`);
  }
  output = output.replace(/Bearer\\s+[A-Za-z0-9._-]+/gi, 'Bearer [redacted]');
  return output;
}

function redactPayload(value: unknown): unknown {
  if (typeof value === 'string') return redactString(value);
  if (typeof value === 'number' || typeof value === 'boolean' || value == null) return value;
  if (Array.isArray(value)) return value.map((item) => redactPayload(item));
  if (value instanceof Buffer) return redactString(value.toString('utf8'));
  if (typeof value === 'object') {
    const output: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
      const lower = key.toLowerCase();
      if (PAYLOAD_REDACT_KEYS.some((needle) => lower.includes(needle))) {
        output[key] = '[redacted]';
      } else {
        output[key] = redactPayload(val);
      }
    }
    return output;
  }
  return value;
}

function stringifyPayload(value: unknown) {
  const safeValue = redactPayload(value);
  const text = typeof safeValue === 'string' ? safeValue : JSON.stringify(safeValue);
  const redacted = redactString(text);
  if (Buffer.byteLength(redacted, 'utf8') <= AGENT_PAYLOAD_MAX_BYTES) return redacted;
  return `${redacted.slice(0, AGENT_PAYLOAD_MAX_BYTES)}...<truncated>`;
}

function buildRequestPayload(request: { body?: unknown; query?: unknown; params?: unknown }) {
  const payload: Record<string, unknown> = {};
  if (request.body !== undefined) payload.body = request.body;
  if (request.query !== undefined && Object.keys(request.query as Record<string, unknown>).length > 0) {
    payload.query = request.query;
  }
  if (request.params !== undefined && Object.keys(request.params as Record<string, unknown>).length > 0) {
    payload.params = request.params;
  }
  return payload;
}

function startOfUtcDay(now = new Date()) {
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
}

function startOfUtcWeek(now = new Date()) {
  const dayStart = startOfUtcDay(now);
  const day = dayStart.getUTCDay();
  const diffToMonday = (day + 6) % 7;
  dayStart.setUTCDate(dayStart.getUTCDate() - diffToMonday);
  return dayStart;
}

function getBaseUrl(request: { headers: Record<string, string | string[] | undefined>; protocol?: string }) {
  if (PUBLIC_BASE_URL) return PUBLIC_BASE_URL;
  const forwardedProto = request.headers['x-forwarded-proto'];
  const proto = Array.isArray(forwardedProto)
    ? forwardedProto[0]
    : forwardedProto ?? request.protocol ?? 'http';
  const forwardedHost = request.headers['x-forwarded-host'];
  const host = Array.isArray(forwardedHost) ? forwardedHost[0] : forwardedHost ?? request.headers.host ?? 'localhost';
  return `${proto}://${host}`;
}

function normalizeHeader(value: string | string[] | undefined) {
  if (!value) return '';
  return Array.isArray(value) ? value[0] : value;
}

function normalizeAgentName(value: string | null | undefined) {
  return (value ?? '').trim().toLowerCase();
}

function isSyntheticAgentName(value: string | null | undefined) {
  const normalized = normalizeAgentName(value);
  if (!normalized) return false;
  return SYNTHETIC_AGENT_PREFIXES.some((prefix) => normalized.startsWith(prefix));
}

function toNumber(value: bigint | number | string | null | undefined) {
  if (value == null) return 0;
  if (typeof value === 'number') return value;
  if (typeof value === 'bigint') return Number(value);
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toNullableNumber(value: number | string | null | undefined) {
  if (value == null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function ratio(numerator: number, denominator: number) {
  if (denominator <= 0) return 0;
  return numerator / denominator;
}

function growthRate(current: number, previous: number) {
  if (previous <= 0) return null;
  return (current - previous) / previous;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isPrismaPoolTimeoutError(err: unknown) {
  return err instanceof Prisma.PrismaClientKnownRequestError && err.code === 'P2024';
}

async function withPrismaPoolRetry<T>(label: string, work: () => Promise<T>, maxAttempts = 3): Promise<T> {
  let attempt = 0;
  while (true) {
    attempt += 1;
    try {
      return await work();
    } catch (err) {
      if (!isPrismaPoolTimeoutError(err) || attempt >= maxAttempts) throw err;
      const delayMs = Math.min(5000, 250 * Math.pow(2, attempt - 1));
      fastify.log.warn({ err, label, attempt, delayMs }, 'prisma pool timeout, retrying');
      await sleep(delayMs);
    }
  }
}

function enqueueUsageEvent(row: Prisma.UsageEventCreateManyInput) {
  if (usageEventBuffer.length >= USAGE_LOG_BUFFER_MAX) {
    usageEventBuffer.shift();
    usageEventDropped += 1;
  }
  usageEventBuffer.push(row);
}

async function flushUsageEventBuffer(limit = USAGE_LOG_FLUSH_BATCH_SIZE) {
  const take = Math.max(1, Math.min(limit, USAGE_LOG_FLUSH_BATCH_SIZE));
  if (take <= 0 || usageEventBuffer.length === 0) return;
  if (usageEventFlushPromise) return usageEventFlushPromise;

  const batch = usageEventBuffer.splice(0, Math.min(take, usageEventBuffer.length));
  usageEventFlushPromise = prisma.usageEvent.createMany({ data: batch })
    .then(() => {
      if (usageEventDropped > 0) {
        fastify.log.warn({ dropped: usageEventDropped }, 'usage event buffer dropped rows');
        usageEventDropped = 0;
      }
    })
    .catch((err) => {
      usageEventBuffer.unshift(...batch);
      while (usageEventBuffer.length > USAGE_LOG_BUFFER_MAX) {
        usageEventBuffer.pop();
        usageEventDropped += 1;
      }
      throw err;
    })
    .finally(() => {
      usageEventFlushPromise = null;
    });
  return usageEventFlushPromise;
}

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

function isPlaceholderId(id: string) {
  const trimmed = id.trim();
  if (!trimmed) return true;
  const lower = trimmed.toLowerCase();
  if (lower === ':id' || lower === '{id}' || lower === '<id>' || lower === 'id') return true;
  if (trimmed.includes(':')) return true;
  if (trimmed.includes('{') || trimmed.includes('}')) return true;
  if (trimmed.includes('<') || trimmed.includes('>')) return true;
  return false;
}

function stripQuery(value: string) {
  const index = value.indexOf('?');
  return index === -1 ? value : value.slice(0, index);
}

const WELL_KNOWN_AGENT_PATHS = ['/.well-known/agent.json', '/.well-known/agent-card.json'] as const;

function getCanonicalWellKnownPath(rawUrl: string) {
  const path = stripQuery(rawUrl);
  for (const canonical of WELL_KNOWN_AGENT_PATHS) {
    if (path === canonical || path.endsWith(canonical)) return canonical;
  }
  return null;
}

function resolveRoute(request: RouteRequest) {
  return (
    request.routerPath ??
    request.routeOptions?.url ??
    stripQuery(request.raw.url ?? request.url)
  );
}

function isNoiseEvent(entry: { method: string; route: string; status: number }) {
  const method = entry.method.toUpperCase();
  if (method !== 'GET' && method !== 'HEAD') return false;

  if (entry.status === 405) {
    if (entry.route === '/api/v1/auth/trial-key') return true;
    if (entry.route === '/api/v1/questions/:id/answers') return true;
  }

  if (entry.status === 400) {
    if (entry.route === '/q/:id') return true;
    if (entry.route === '/api/v1/questions/:id') return true;
  }

  if (entry.status === 404) {
    if (entry.route === '/') return true;
    if (entry.route === '/api/v1/fetch') return true;
    if (entry.route === '/docs/.well-known/agent.json') return true;
  }

  if (entry.status === 401 || entry.status === 403) {
    if (entry.route === '/api/v1/usage/summary') return true;
    if (entry.route === '/admin/usage') return true;
    if (entry.route === '/admin/usage/data') return true;
    if (entry.route === '/admin/agent-events') return true;
    if (entry.route === '/admin/agent-events/data') return true;
  }

  return false;
}

function extractApiKeyPrefix(headers: Record<string, string | string[] | undefined>) {
  const auth = normalizeHeader(headers.authorization);
  if (!auth) return null;
  const [scheme, ...rest] = auth.split(' ');
  if (!scheme || scheme.toLowerCase() !== 'bearer') return null;
  const token = rest.join(' ').trim();
  if (!token) return null;
  return token.slice(0, 8);
}

function getAgentName(headers: Record<string, string | string[] | undefined>) {
  const name = normalizeHeader(
    headers['x-agent-name'] ??
      headers['x-mcp-client-name'] ??
      headers['mcp-client-name'] ??
      headers['x-client-name']
  );
  if (!name) return null;
  return name.slice(0, 128);
}

function getHeaderValue(headers: Record<string, string | string[] | undefined>, key: string) {
  return normalizeHeader(headers[key as keyof typeof headers]);
}

function firstHeaderIp(value: string) {
  return value.split(',')[0]?.trim();
}

function getClientIp(request: RouteRequest & { ip?: string; socket?: { remoteAddress?: string } }) {
  const forwarded = normalizeHeader(request.headers['x-forwarded-for']);
  if (forwarded) return firstHeaderIp(forwarded);
  const realIp = normalizeHeader(request.headers['x-real-ip']);
  if (realIp) return realIp;
  const cfIp = normalizeHeader(request.headers['cf-connecting-ip']);
  if (cfIp) return cfIp;
  const appEngineIp = normalizeHeader(request.headers['x-appengine-user-ip']);
  if (appEngineIp) return appEngineIp;
  return request.ip ?? request.socket?.remoteAddress ?? null;
}

function parseBasicAuth(headers: Record<string, string | string[] | undefined>) {
  const header = normalizeHeader(headers.authorization);
  if (!header || !header.toLowerCase().startsWith('basic ')) return null;
  const encoded = header.slice(6).trim();
  if (!encoded) return null;
  try {
    const decoded = Buffer.from(encoded, 'base64').toString('utf8');
    const [user, pass] = decoded.split(':');
    if (!user || !pass) return null;
    return { user, pass };
  } catch {
    return null;
  }
}

async function requireAdminDashboard(request: { headers: Record<string, string | string[] | undefined> }, reply: any) {
  if (!ADMIN_DASH_USER || !ADMIN_DASH_PASS) {
    reply.code(500).send('Admin dashboard credentials are not configured');
    return false;
  }
  const creds = parseBasicAuth(request.headers);
  if (!creds || creds.user !== ADMIN_DASH_USER || creds.pass !== ADMIN_DASH_PASS) {
    reply.header('WWW-Authenticate', 'Basic realm="A2ABench Admin"');
    reply.code(401).send('Unauthorized');
    return false;
  }
  return true;
}

function agentCard(baseUrl: string) {
  return {
    name: 'A2ABench',
    description: 'Agent-native developer Q&A with REST + MCP + A2A discovery. Read-only endpoints do not require auth.',
    url: baseUrl,
    version: '0.1.30',
    protocolVersion: '0.1',
    skills: [
      {
        id: 'search',
        name: 'Search',
        description: 'Search questions by keyword or tag.'
      },
      {
        id: 'fetch',
        name: 'Fetch',
        description: 'Fetch a question thread by id.'
      },
      {
        id: 'create_question',
        name: 'Create Question',
        description: 'Create a new question thread (requires API key).'
      },
      {
        id: 'create_answer',
        name: 'Create Answer',
        description: 'Create an answer for a question (requires API key).'
      },
      {
        id: 'answer_job',
        name: 'Answer Job',
        description: 'One-step flow: claim question + submit answer + mark job progress.'
      },
      {
        id: 'claim_question',
        name: 'Claim Question',
        description: 'Claim a question before answering to establish job ownership and verification eligibility.'
      },
      {
        id: 'release_claim',
        name: 'Release Claim',
        description: 'Release a previously claimed question so another agent can take it.'
      },
      {
        id: 'pending_acceptance',
        name: 'Pending Acceptance',
        description: 'List open questions with answers that still need acceptance confirmation.'
      },
      {
        id: 'questions_unanswered',
        name: 'Unanswered Queue',
        description: 'Discover unanswered questions, prioritized by bounty.'
      },
      {
        id: 'agent_quickstart',
        name: 'Agent Quickstart',
        description: 'Return the highest-priority open question and one-call actions to answer it.'
      },
      {
        id: 'next_best_job',
        name: 'Next Best Job',
        description: 'Return a scored, personalized next question to answer with one-call action paths.'
      },
      {
        id: 'vote_answer',
        name: 'Vote Answer',
        description: 'Cast a +1/-1 vote on an answer to improve ranking signals.'
      },
      {
        id: 'accept_answer',
        name: 'Accept Answer',
        description: 'Mark the accepted answer for a question and settle bounty.'
      },
      {
        id: 'leaderboard',
        name: 'Agent Leaderboard',
        description: 'List top agents by reputation and accepted answers.'
      },
      {
        id: 'top_solved_weekly',
        name: 'Weekly Solved Leaderboard',
        description: 'List agents with most accepted solutions by week.'
      },
      {
        id: 'answer',
        name: 'Answer',
        description: 'Synthesize a grounded answer from A2ABench threads with citations.',
        input_schema: {
          query: { type: 'string' },
          top_k: { type: 'integer' },
          include_evidence: { type: 'boolean' },
          mode: { type: 'string', enum: ['balanced', 'strict'] },
          max_chars_per_evidence: { type: 'integer' }
        }
      }
    ],
    auth: {
      type: 'apiKey',
      description: 'Read-only endpoints and MCP tools are public. Bearer API key for write endpoints. X-Admin-Token for admin endpoints.'
    }
  };
}


function parse<T>(schema: z.ZodSchema<T>, input: unknown, reply: { code: (code: number) => { send: (payload: unknown) => void } }) {
  const result = schema.safeParse(input);
  if (!result.success) {
    reply.code(400).send({ error: 'Invalid request', issues: result.error.flatten() });
    return null;
  }
  return result.data;
}

async function requireAdmin(request: { headers: Record<string, string | string[] | undefined> }, reply: any) {
  if (!ADMIN_TOKEN) {
    reply.code(500).send({ error: 'ADMIN_TOKEN is not configured' });
    return false;
  }
  const token = request.headers['x-admin-token'];
  const value = Array.isArray(token) ? token[0] : token;
  if (!value || value !== ADMIN_TOKEN) {
    reply.code(401).send({ error: 'Unauthorized' });
    return false;
  }
  return true;
}

async function requireAgentEventToken(request: { headers: Record<string, string | string[] | undefined> }, reply: any) {
  if (!AGENT_EVENT_TOKEN) {
    reply.code(500).send({ error: 'AGENT_EVENT_TOKEN is not configured' });
    return false;
  }
  const token = request.headers['x-agent-event-token'];
  const value = Array.isArray(token) ? token[0] : token;
  if (!value || value !== AGENT_EVENT_TOKEN) {
    reply.code(401).send({ error: 'Unauthorized' });
    return false;
  }
  return true;
}

async function requireApiKey(request: { headers: Record<string, string | string[] | undefined> }, reply: any, scope?: string) {
  const header = request.headers.authorization;
  const auth = Array.isArray(header) ? header[0] : header;
  if (!auth) {
    reply.code(401).send({ error: 'Missing API key' });
    return null;
  }
  const [scheme, ...rest] = auth.split(' ');
  if (!scheme || scheme.toLowerCase() !== 'bearer' || rest.length === 0) {
    reply.code(401).send({ error: 'Missing API key' });
    return null;
  }
  const key = rest.join(' ').trim();
  if (!key) {
    reply.code(401).send({ error: 'Invalid API key' });
    return null;
  }
  const keyPrefix = key.slice(0, 8);
  const keyHash = sha256(key);
  const apiKey = await prisma.apiKey.findFirst({
    where: { keyPrefix, keyHash, revokedAt: null },
    include: { user: true }
  });
  if (!apiKey) {
    reply.code(401).send({ error: 'Invalid API key' });
    return null;
  }
  if (apiKey.expiresAt && apiKey.expiresAt.getTime() < Date.now()) {
    reply.code(401).send({ error: 'API key expired' });
    return null;
  }
  if (scope && apiKey.scopes.length > 0) {
    const aliases: Record<string, string[]> = {
      'write:questions': ['write:questions', 'write:question'],
      'write:answers': ['write:answers', 'write:answer']
    };
    const allowed = aliases[scope] ?? [scope];
    const hasScope = apiKey.scopes.some((value) => allowed.includes(value));
    if (!hasScope) {
      reply.code(403).send({ error: 'Insufficient scope' });
      return null;
    }
  }
  return apiKey;
}

async function validateApiKey(request: { headers: Record<string, string | string[] | undefined> }) {
  const header = request.headers.authorization;
  const auth = Array.isArray(header) ? header[0] : header;
  if (!auth) return { ok: false, reason: 'Missing API key' };
  const [scheme, ...rest] = auth.split(' ');
  if (!scheme || scheme.toLowerCase() !== 'bearer' || rest.length === 0) {
    return { ok: false, reason: 'Missing API key' };
  }
  const key = rest.join(' ').trim();
  if (!key) return { ok: false, reason: 'Invalid API key' };
  const keyPrefix = key.slice(0, 8);
  const keyHash = sha256(key);
  const apiKey = await prisma.apiKey.findFirst({
    where: { keyPrefix, keyHash, revokedAt: null }
  });
  if (!apiKey) return { ok: false, reason: 'Invalid API key' };
  if (apiKey.expiresAt && apiKey.expiresAt.getTime() < Date.now()) {
    return { ok: false, reason: 'API key expired' };
  }
  return { ok: true, keyPrefix };
}

function getLlmQuotaKey(request: RouteRequest, agentName: string | null) {
  const keyPrefix = extractApiKeyPrefix(request.headers);
  if (keyPrefix) return `key:${keyPrefix}`;
  if (agentName) return `agent:${normalizeAgentName(agentName)}`;
  return `ip:${request.ip ?? 'unknown'}`;
}

function getUtcDateKey(now = new Date()) {
  return now.toISOString().slice(0, 10);
}

function allowLlmForRequest(request: RouteRequest, agentName: string | null) {
  if (!LLM_ENABLED) {
    return {
      allowed: false,
      message: 'LLM disabled; returning retrieved evidence only.',
      warnings: ['LLM disabled by policy.']
    };
  }
  if (LLM_AGENT_ALLOWLIST.size > 0) {
    const normalized = normalizeAgentName(agentName);
    if (!normalized || !LLM_AGENT_ALLOWLIST.has(normalized)) {
      return {
        allowed: false,
        message: 'LLM disabled for this agent; returning retrieved evidence only.',
        warnings: ['LLM disabled for this agent.']
      };
    }
  }
  return { allowed: true, message: '', warnings: [] };
}

function allowLlmByQuota(request: RouteRequest, agentName: string | null) {
  if (LLM_DAILY_LIMIT <= 0) return { allowed: true };
  const key = getLlmQuotaKey(request, agentName);
  const today = getUtcDateKey();
  const entry = llmUsage.get(key);
  if (!entry || entry.dateKey !== today) {
    llmUsage.set(key, { dateKey: today, count: 1 });
    return { allowed: true };
  }
  if (entry.count >= LLM_DAILY_LIMIT) {
    return { allowed: false };
  }
  entry.count += 1;
  return { allowed: true };
}

const CAPTURED_ROUTES = new Set([
  '/api/v1/auth/trial-key',
  '/api/v1/questions',
  '/api/v1/questions/:id',
  '/api/v1/questions/pending-acceptance',
  '/api/v1/questions/:id/answer-job',
  '/api/v1/questions/:id/claim',
  '/api/v1/questions/:id/claims',
  '/api/v1/questions/:id/claims/:claimId/release',
  '/api/v1/questions/:id/answers',
  '/api/v1/questions/:id/bounty',
  '/api/v1/questions/:id/accept/:answerId',
  '/api/v1/questions/:id/accept/:answerId/link',
  '/api/v1/accept-links/:token',
  '/api/v1/accept-links',
  '/api/v1/answers/:id/vote',
  '/api/v1/agent/inbox',
  '/api/v1/subscriptions',
  '/api/v1/subscriptions/:id/disable',
  '/api/v1/agent/next-best-job',
  '/api/v1/search',
  '/api/v1/questions/unanswered',
  '/api/v1/feed/unanswered',
  '/api/v1/agent/quickstart',
  '/api/v1/agents/leaderboard',
  '/api/v1/agents/top-solved-weekly',
  '/api/v1/agents/:agentName/credits',
  '/api/v1/incentives/rules',
  '/api/v1/incentives/payouts/history',
  '/api/v1/incentives/seasons/monthly',
  '/api/v1/admin/retention/weekly',
  '/api/v1/admin/delivery/process',
  '/api/v1/admin/delivery/queue',
  '/api/v1/admin/reminders/process',
  '/api/v1/admin/autoclose/process',
  '/api/v1/admin/import/questions',
  '/api/v1/admin/partners/teams',
  '/api/v1/admin/partners/teams/:id/members',
  '/api/v1/admin/partners/teams/:id/metrics/weekly',
  '/api/v1/bounties'
]);

function isAgentTraffic(agentName: string | null, userAgent: string | null) {
  if (agentName) return true;
  if (!userAgent) return false;
  return /(chatgpt|claude|agent|mcp|bot)/i.test(userAgent);
}

async function pruneAgentPayloadEvents() {
  const ttlMs = AGENT_PAYLOAD_TTL_HOURS * 60 * 60 * 1000;
  if (ttlMs > 0) {
    const cutoff = new Date(Date.now() - ttlMs);
    await prisma.agentPayloadEvent.deleteMany({ where: { createdAt: { lt: cutoff } } });
  }
  if (AGENT_PAYLOAD_MAX_EVENTS > 0) {
    const total = await prisma.agentPayloadEvent.count();
    if (total > AGENT_PAYLOAD_MAX_EVENTS) {
      const removeCount = total - AGENT_PAYLOAD_MAX_EVENTS;
      const oldest: Array<{ id: string }> = await prisma.agentPayloadEvent.findMany({
        select: { id: true },
        orderBy: { createdAt: 'asc' },
        take: removeCount
      });
      if (oldest.length > 0) {
        await prisma.agentPayloadEvent.deleteMany({ where: { id: { in: oldest.map((row) => row.id) } } });
      }
    }
  }
}

async function storeAgentPayloadEvent(entry: {
  source: string;
  kind: string;
  method?: string | null;
  route?: string | null;
  status?: number | null;
  durationMs?: number | null;
  tool?: string | null;
  requestId?: string | null;
  agentName?: string | null;
  userAgent?: string | null;
  ip?: string | null;
  apiKeyPrefix?: string | null;
  requestBody?: unknown;
  responseBody?: unknown;
}) {
  if (!CAPTURE_AGENT_PAYLOADS) return;
  const requestBody = entry.requestBody !== undefined ? stringifyPayload(entry.requestBody) : null;
  const responseBody = entry.responseBody !== undefined ? stringifyPayload(entry.responseBody) : null;

  await prisma.agentPayloadEvent.create({
    data: {
      source: entry.source,
      kind: entry.kind,
      method: entry.method ?? null,
      route: entry.route ?? null,
      status: entry.status ?? null,
      durationMs: entry.durationMs ?? null,
      tool: entry.tool ?? null,
      requestId: entry.requestId ?? null,
      agentName: entry.agentName ?? null,
      userAgent: entry.userAgent ?? null,
      ip: entry.ip ?? null,
      apiKeyPrefix: entry.apiKeyPrefix ?? null,
      requestBody,
      responseBody
    }
  });

  void pruneAgentPayloadEvents().catch(() => undefined);
}

async function enforceWriteLimits(
  apiKey: { id: string; dailyWriteLimit: number | null; dailyQuestionLimit: number | null; dailyAnswerLimit: number | null },
  kind: 'question' | 'answer',
  reply: any
) {
  const limits = {
    dailyWrites: apiKey.dailyWriteLimit ?? null,
    dailyQuestions: apiKey.dailyQuestionLimit ?? null,
    dailyAnswers: apiKey.dailyAnswerLimit ?? null
  };
  if (!limits.dailyWrites && !limits.dailyQuestions && !limits.dailyAnswers) return true;
  const bucket = startOfUtcDay();
  const existing = await prisma.apiKeyUsage.findUnique({
    where: { apiKeyId_date: { apiKeyId: apiKey.id, date: bucket } }
  });
  const writeCount = existing?.writeCount ?? 0;
  const questionCount = existing?.questionCount ?? 0;
  const answerCount = existing?.answerCount ?? 0;
  const wouldWrite = writeCount + 1;
  const wouldQuestion = questionCount + (kind === 'question' ? 1 : 0);
  const wouldAnswer = answerCount + (kind === 'answer' ? 1 : 0);

  if (limits.dailyWrites !== null && wouldWrite > limits.dailyWrites) {
    reply.code(429).send({ error: 'Daily write limit reached', limits, resetAt: bucket.toISOString() });
    return false;
  }
  if (limits.dailyQuestions !== null && wouldQuestion > limits.dailyQuestions) {
    reply.code(429).send({ error: 'Daily question limit reached', limits, resetAt: bucket.toISOString() });
    return false;
  }
  if (limits.dailyAnswers !== null && wouldAnswer > limits.dailyAnswers) {
    reply.code(429).send({ error: 'Daily answer limit reached', limits, resetAt: bucket.toISOString() });
    return false;
  }

  await prisma.apiKeyUsage.upsert({
    where: { apiKeyId_date: { apiKeyId: apiKey.id, date: bucket } },
    update: {
      writeCount: { increment: 1 },
      questionCount: { increment: kind === 'question' ? 1 : 0 },
      answerCount: { increment: kind === 'answer' ? 1 : 0 }
    },
    create: {
      apiKeyId: apiKey.id,
      date: bucket,
      writeCount: 1,
      questionCount: kind === 'question' ? 1 : 0,
      answerCount: kind === 'answer' ? 1 : 0
    }
  });

  return true;
}

const ACCEPT_REPUTATION_REWARD = 15;

function normalizeAgentOrNull(value: string | null | undefined) {
  const normalized = normalizeAgentName(value);
  return normalized || null;
}

async function ensureAgentProfile(agentName: string | null | undefined) {
  const normalized = normalizeAgentOrNull(agentName);
  if (!normalized) return null;
  return prisma.agentProfile.upsert({
    where: { name: normalized },
    update: {},
    create: { name: normalized }
  });
}

async function addAgentReputation(agentName: string | null | undefined, delta: number, voteDelta = 0) {
  const normalized = normalizeAgentOrNull(agentName);
  if (!normalized || (!delta && !voteDelta)) return;
  await prisma.agentProfile.upsert({
    where: { name: normalized },
    update: {
      reputation: { increment: delta },
      voteScore: { increment: voteDelta }
    },
    create: {
      name: normalized,
      reputation: delta,
      voteScore: voteDelta
    }
  });
}

async function addAgentCredits(
  agentName: string | null | undefined,
  delta: number,
  reason: string,
  refs?: { questionId?: string | null; answerId?: string | null }
) {
  const normalized = normalizeAgentOrNull(agentName);
  if (!normalized || delta === 0) return;
  await prisma.$transaction([
    prisma.agentProfile.upsert({
      where: { name: normalized },
      update: { credits: { increment: delta } },
      create: { name: normalized, credits: delta }
    }),
    prisma.agentCreditLedger.create({
      data: {
        agentName: normalized,
        delta,
        reason,
        questionId: refs?.questionId ?? null,
        answerId: refs?.answerId ?? null
      }
    })
  ]);
}

async function incrementAgentAnswerCount(agentName: string | null | undefined) {
  const normalized = normalizeAgentOrNull(agentName);
  if (!normalized) return;
  await prisma.agentProfile.upsert({
    where: { name: normalized },
    update: { answersCount: { increment: 1 } },
    create: { name: normalized, answersCount: 1 }
  });
}

async function incrementAcceptedCount(agentName: string | null | undefined, delta: 1 | -1) {
  const normalized = normalizeAgentOrNull(agentName);
  if (!normalized) return;
  await prisma.agentProfile.upsert({
    where: { name: normalized },
    update: { acceptedCount: { increment: delta } },
    create: { name: normalized, acceptedCount: delta }
  });
}

async function getAnswerVoteMap(answerIds: string[]) {
  if (!answerIds.length) return new Map<string, number>();
  const rows = await prisma.answerVote.groupBy({
    by: ['answerId'],
    where: { answerId: { in: answerIds } },
    _sum: { value: true }
  });
  const map = new Map<string, number>();
  for (const row of rows) {
    map.set(row.answerId, row._sum.value ?? 0);
  }
  return map;
}

function getActiveBountyAmount(entry: { active: boolean; expiresAt: Date | null; amount: number } | null) {
  if (!entry || !entry.active) return 0;
  if (entry.expiresAt && entry.expiresAt.getTime() < Date.now()) return 0;
  return Math.max(0, entry.amount);
}

function subscriptionMatches(tags: string[], questionTags: string[]) {
  if (tags.length === 0) return true;
  const qset = new Set(questionTags.map((tag) => tag.toLowerCase()));
  return tags.some((tag) => qset.has(tag.toLowerCase()));
}

const SUBSCRIPTION_EVENT_TYPES = [
  'question.created',
  'question.accepted',
  'question.needs_acceptance',
  'question.acceptance_reminder'
] as const;
const SUBSCRIPTION_DEFAULT_EVENTS = [...SUBSCRIPTION_EVENT_TYPES] as const;

function subscriptionWantsEvent(events: string[] | null | undefined, eventName: string) {
  const normalized = (events ?? [])
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  const effective = normalized.length ? normalized : [...SUBSCRIPTION_DEFAULT_EVENTS];
  return effective.includes(eventName.toLowerCase());
}

type QuestionWebhookEventName = typeof SUBSCRIPTION_EVENT_TYPES[number];

type QuestionWebhookInput = {
  event: QuestionWebhookEventName;
  question: {
    id: string;
    title: string;
    bodyText: string;
    createdAt: Date;
    tags: string[];
    url: string;
    source?: {
      type: string | null;
      url: string | null;
      externalId: string | null;
      title: string | null;
    };
  };
  answer?: {
    id: string;
    agentName: string | null;
    createdAt: Date;
  };
  acceptance?: {
    acceptedAt: Date;
    acceptedAnswerId: string;
    acceptedAgentName: string | null;
    acceptedByAgentName: string | null;
    bountyPaid: number;
    starterBonusPaid: number;
  };
  prompt?: {
    message: string;
    suggestedAction: string;
    acceptUrl?: string | null;
    reminderStageHours?: number | null;
  };
};

function computeDeliveryBackoffMs(attemptCount: number) {
  const exponent = Math.max(0, attemptCount - 1);
  const ms = DELIVERY_RETRY_BASE_MS * Math.pow(2, exponent);
  return Math.min(DELIVERY_RETRY_MAX_MS, Math.max(DELIVERY_RETRY_BASE_MS, ms));
}

async function processDeliveryQueue(limit = DELIVERY_PROCESS_LIMIT) {
  const now = new Date();
  const take = Math.max(1, Math.min(500, limit));
  const due = await prisma.deliveryQueue.findMany({
    where: {
      deliveredAt: null,
      webhookUrl: { not: null },
      nextAttemptAt: { lte: now },
      attemptCount: { lt: DELIVERY_MAX_ATTEMPTS }
    },
    orderBy: [
      { nextAttemptAt: 'asc' },
      { createdAt: 'asc' }
    ],
    take
  });
  if (due.length === 0) return { processed: 0, delivered: 0, failed: 0, pending: 0 };

  let delivered = 0;
  let failed = 0;
  let pending = 0;

  for (const job of due) {
    const payloadText = JSON.stringify(job.payload);
    const headers: Record<string, string> = {
      'content-type': 'application/json',
      'x-a2abench-event': job.event
    };
    if (job.webhookSecret) {
      const signature = crypto.createHmac('sha256', job.webhookSecret).update(payloadText).digest('hex');
      headers['x-a2abench-signature'] = `sha256=${signature}`;
    }

    const attempts = job.attemptCount + 1;
    const maxAttempts = Math.max(1, job.maxAttempts || DELIVERY_MAX_ATTEMPTS);
    const attemptAt = new Date();
    try {
      const response = await fetch(job.webhookUrl!, {
        method: 'POST',
        headers,
        body: payloadText
      });
      if (response.ok) {
        await prisma.deliveryQueue.update({
          where: { id: job.id },
          data: {
            attemptCount: attempts,
            lastAttemptAt: attemptAt,
            lastStatus: response.status,
            lastError: null,
            deliveredAt: new Date()
          }
        });
        delivered += 1;
        continue;
      }

      const bodyText = (await response.text()).slice(0, 1000);
      await prisma.deliveryQueue.update({
        where: { id: job.id },
        data: {
          attemptCount: attempts,
          lastAttemptAt: attemptAt,
          lastStatus: response.status,
          lastError: bodyText || `HTTP ${response.status}`,
          nextAttemptAt: attempts >= maxAttempts
            ? new Date(Date.now() + DELIVERY_RETRY_MAX_MS)
            : new Date(Date.now() + computeDeliveryBackoffMs(attempts))
        }
      });
      if (attempts >= maxAttempts) failed += 1;
      else pending += 1;
    } catch (err) {
      await prisma.deliveryQueue.update({
        where: { id: job.id },
        data: {
          attemptCount: attempts,
          lastAttemptAt: attemptAt,
          lastStatus: null,
          lastError: err instanceof Error ? err.message.slice(0, 1000) : 'delivery_failed',
          nextAttemptAt: attempts >= maxAttempts
            ? new Date(Date.now() + DELIVERY_RETRY_MAX_MS)
            : new Date(Date.now() + computeDeliveryBackoffMs(attempts))
        }
      });
      if (attempts >= maxAttempts) failed += 1;
      else pending += 1;
    }
  }

  return { processed: due.length, delivered, failed, pending };
}

async function dispatchQuestionWebhookEvent(input: QuestionWebhookInput) {
  const subscriptions = await prisma.questionSubscription.findMany({
    where: { active: true }
  });
  if (subscriptions.length === 0) return;

  const payload = {
    event: input.event,
    question: {
      id: input.question.id,
      title: input.question.title,
      bodyText: input.question.bodyText,
      createdAt: input.question.createdAt.toISOString(),
      tags: input.question.tags,
      url: input.question.url,
      source: input.question.source
        ? {
            type: input.question.source.type ?? null,
            url: input.question.source.url ?? null,
            externalId: input.question.source.externalId ?? null,
            title: input.question.source.title ?? null
          }
        : null
    },
    answer: input.answer
      ? {
          id: input.answer.id,
          agentName: input.answer.agentName,
          createdAt: input.answer.createdAt.toISOString()
        }
      : undefined,
    acceptance: input.acceptance
      ? {
          acceptedAt: input.acceptance.acceptedAt.toISOString(),
          acceptedAnswerId: input.acceptance.acceptedAnswerId,
          acceptedAgentName: input.acceptance.acceptedAgentName,
          acceptedByAgentName: input.acceptance.acceptedByAgentName,
          bountyPaid: input.acceptance.bountyPaid,
          starterBonusPaid: input.acceptance.starterBonusPaid
        }
      : undefined,
    prompt: input.prompt
      ? {
          message: input.prompt.message,
          suggestedAction: input.prompt.suggestedAction,
          acceptUrl: input.prompt.acceptUrl ?? null,
          reminderStageHours: input.prompt.reminderStageHours ?? null
        }
      : undefined
  };

  const queued = subscriptions
    .filter((sub) => subscriptionMatches(sub.tags ?? [], input.question.tags))
    .filter((sub) => subscriptionWantsEvent(sub.events, input.event))
    .map((sub) => ({
      subscriptionId: sub.id,
      agentName: sub.agentName,
      event: input.event,
      payload,
      questionId: input.question.id,
      answerId: input.answer?.id ?? input.acceptance?.acceptedAnswerId ?? null,
      webhookUrl: sub.webhookUrl ?? null,
      webhookSecret: sub.webhookSecret ?? null,
      maxAttempts: DELIVERY_MAX_ATTEMPTS,
      nextAttemptAt: new Date()
    }));

  if (queued.length === 0) return;
  await prisma.deliveryQueue.createMany({ data: queued });
  void processDeliveryQueue(Math.min(DELIVERY_PROCESS_LIMIT, queued.length)).catch(() => undefined);
}

async function dispatchQuestionCreatedEvent(input: {
  id: string;
  title: string;
  bodyText: string;
  createdAt: Date;
  tags: string[];
  url: string;
  source?: {
    type: string | null;
    url: string | null;
    externalId: string | null;
    title: string | null;
  };
}) {
  await dispatchQuestionWebhookEvent({
    event: 'question.created',
    question: {
      id: input.id,
      title: input.title,
      bodyText: input.bodyText,
      createdAt: input.createdAt,
      tags: input.tags,
      url: input.url,
      source: input.source
    }
  });
}

async function dispatchQuestionAcceptedEvent(input: {
  id: string;
  title: string;
  bodyText: string;
  createdAt: Date;
  tags: string[];
  url: string;
  acceptedAt: Date;
  acceptedAnswerId: string;
  acceptedAgentName: string | null;
  acceptedByAgentName: string | null;
  bountyPaid: number;
  starterBonusPaid: number;
  source?: {
    type: string | null;
    url: string | null;
    externalId: string | null;
    title: string | null;
  };
}) {
  await dispatchQuestionWebhookEvent({
    event: 'question.accepted',
    question: {
      id: input.id,
      title: input.title,
      bodyText: input.bodyText,
      createdAt: input.createdAt,
      tags: input.tags,
      url: input.url,
      source: input.source
    },
    acceptance: {
      acceptedAt: input.acceptedAt,
      acceptedAnswerId: input.acceptedAnswerId,
      acceptedAgentName: input.acceptedAgentName,
      acceptedByAgentName: input.acceptedByAgentName,
      bountyPaid: input.bountyPaid,
      starterBonusPaid: input.starterBonusPaid
    }
  });
}

async function dispatchNeedsAcceptanceEvent(input: {
  id: string;
  title: string;
  bodyText: string;
  createdAt: Date;
  tags: string[];
  url: string;
  answerId: string;
  answerAgentName: string | null;
  answerCreatedAt: Date;
  acceptUrl?: string | null;
  reminderStageHours?: number | null;
  source?: {
    type: string | null;
    url: string | null;
    externalId: string | null;
    title: string | null;
  };
}) {
  await dispatchQuestionWebhookEvent({
    event: 'question.needs_acceptance',
    question: {
      id: input.id,
      title: input.title,
      bodyText: input.bodyText,
      createdAt: input.createdAt,
      tags: input.tags,
      url: input.url,
      source: input.source
    },
    answer: {
      id: input.answerId,
      agentName: input.answerAgentName,
      createdAt: input.answerCreatedAt
    },
    prompt: {
      message: 'New answer posted. Please verify and accept the best answer to close the loop.',
      suggestedAction: `POST /api/v1/questions/${input.id}/accept/${input.answerId}`,
      acceptUrl: input.acceptUrl ?? null,
      reminderStageHours: input.reminderStageHours ?? null
    }
  });
}

async function dispatchAcceptanceReminderEvent(input: {
  id: string;
  title: string;
  bodyText: string;
  createdAt: Date;
  tags: string[];
  url: string;
  answerId: string;
  answerAgentName: string | null;
  answerCreatedAt: Date;
  reminderStageHours: number;
  acceptUrl?: string | null;
  source?: {
    type: string | null;
    url: string | null;
    externalId: string | null;
    title: string | null;
  };
}) {
  await dispatchQuestionWebhookEvent({
    event: 'question.acceptance_reminder',
    question: {
      id: input.id,
      title: input.title,
      bodyText: input.bodyText,
      createdAt: input.createdAt,
      tags: input.tags,
      url: input.url,
      source: input.source
    },
    answer: {
      id: input.answerId,
      agentName: input.answerAgentName,
      createdAt: input.answerCreatedAt
    },
    prompt: {
      message: `Acceptance reminder (${input.reminderStageHours}h): please accept the best answer to close this question.`,
      suggestedAction: `POST /api/v1/questions/${input.id}/accept/${input.answerId}`,
      acceptUrl: input.acceptUrl ?? null,
      reminderStageHours: input.reminderStageHours
    }
  });
}

function clampClaimTtlMinutes(value: number | null | undefined) {
  if (!Number.isFinite(value)) return QUESTION_CLAIM_TTL_MINUTES;
  const rounded = Math.round(Number(value));
  return Math.min(QUESTION_CLAIM_MAX_MINUTES, Math.max(QUESTION_CLAIM_MIN_MINUTES, rounded));
}

function getClaimExpiry(ttlMinutes: number) {
  return new Date(Date.now() + ttlMinutes * 60 * 1000);
}

async function expireStaleClaims(questionId?: string) {
  const now = new Date();
  const where: Prisma.QuestionClaimWhereInput = {
    state: { in: ['claimed', 'answered'] },
    expiresAt: { lt: now }
  };
  if (questionId) where.questionId = questionId;
  await prisma.questionClaim.updateMany({
    where,
    data: {
      state: 'expired',
      releasedAt: now,
      verifyReason: 'claim_ttl_elapsed'
    }
  });
}

function getReminderStagesHours() {
  const deduped = Array.from(new Set(ACCEPTANCE_REMINDER_STAGES_HOURS))
    .filter((value) => Number.isFinite(value) && value > 0)
    .map((value) => Math.round(value))
    .sort((a, b) => a - b);
  return deduped.length > 0 ? deduped : [1, 24, 72];
}

function normalizeSourceType(value: string | null | undefined) {
  const normalized = (value ?? '').trim().toLowerCase();
  if (!normalized) return null;
  if (normalized === 'dev-support') return 'support';
  if (normalized === 'github' || normalized === 'discord' || normalized === 'support' || normalized === 'other') {
    return normalized;
  }
  return 'other';
}

function getQuestionSource(question: {
  sourceType: string | null;
  sourceUrl: string | null;
  sourceExternalId: string | null;
  sourceTitle: string | null;
  sourceImportedAt: Date | null;
  sourceImportedBy: string | null;
}) {
  if (!question.sourceType && !question.sourceUrl && !question.sourceExternalId && !question.sourceTitle) return undefined;
  return {
    type: question.sourceType ?? null,
    url: question.sourceUrl ?? null,
    externalId: question.sourceExternalId ?? null,
    title: question.sourceTitle ?? null,
    importedAt: question.sourceImportedAt ?? null,
    importedBy: question.sourceImportedBy ?? null
  };
}

function sourcePriorityWeight(sourceType: string | null) {
  switch (sourceType) {
    case 'github':
      return 40;
    case 'support':
      return 30;
    case 'discord':
      return 22;
    case 'other':
      return 10;
    default:
      return 0;
  }
}

function normalizeTitleKey(value: string) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

type ImportQualityResult = {
  ok: boolean;
  score: number;
  reasons: string[];
  bodyTextLength: number;
  titleKey: string;
};

function assessImportQualityCandidate(input: { title: string; bodyMd: string; url?: string | null }) {
  const title = input.title.trim();
  const bodyText = markdownToText(input.bodyMd);
  const bodyTextLength = bodyText.length;
  const titleKey = normalizeTitleKey(title);
  const reasons: string[] = [];
  let score = 0;

  if ((input.url ?? '').trim()) score += 1;
  else reasons.push('missing_source_url');

  if (title.length >= 16 && title.length <= 220) score += 1;
  else if (title.length < 16) reasons.push('title_too_short');
  else reasons.push('title_too_long');

  if (bodyTextLength >= 80) score += 1;
  else reasons.push('body_too_short');

  const technicalCue = /(error|exception|stack|trace|bug|how|why|what|when|cannot|failed|issue|\?)/i.test(`${title}\n${bodyText}`);
  if (technicalCue) score += 1;
  else reasons.push('not_actionable');

  const noisyTitle = /(help|urgent|pls|please help|any update|thanks|thank you)/i.test(title);
  if (noisyTitle && !technicalCue) reasons.push('noisy_title');

  const ok = score >= 3 && !reasons.includes('noisy_title');
  return { ok, score, reasons, bodyTextLength, titleKey } as ImportQualityResult;
}

type RecommendedQuestion = {
  id: string;
  title: string;
  tags: string[];
  source: ReturnType<typeof getQuestionSource>;
  answerCount: number;
  bounty: { amount: number; currency: string; expiresAt: Date | null } | null;
  createdAt: Date;
  score: number;
  reasons: string[];
  matchedTags: string[];
  activeClaim: {
    id: string;
    agentName: string;
    expiresAt: Date;
    state: string;
  } | null;
};

async function getAgentTagPreferences(agentName: string | null) {
  if (!agentName) return new Set<string>();
  const subs = await prisma.questionSubscription.findMany({
    where: {
      agentName,
      active: true
    },
    select: { tags: true }
  });
  const tags = subs.flatMap((row) => row.tags ?? []);
  return new Set(tags.map((tag) => tag.toLowerCase()).filter(Boolean));
}

async function getRecommendedQuestionForAgent(agentName?: string | null) {
  const normalizedAgent = normalizeAgentOrNull(agentName);
  const preferredTags = await getAgentTagPreferences(normalizedAgent);
  const now = new Date();
  const rows = await prisma.question.findMany({
    where: {
      resolution: null,
      ...(normalizedAgent
        ? { answers: { none: { agentName: normalizedAgent } } }
        : {})
    },
    include: {
      tags: { include: { tag: true } },
      _count: { select: { answers: true } },
      bounty: true,
      claims: {
        where: {
          state: { in: ['claimed', 'answered'] },
          expiresAt: { gte: now }
        },
        select: { id: true, agentName: true, state: true, expiresAt: true }
      }
    },
    orderBy: { createdAt: 'desc' },
    take: AGENT_QUICKSTART_CANDIDATES
  });

  const ranked = rows
    .map((row): RecommendedQuestion | null => {
      const activeClaim = normalizedAgent
        ? row.claims.find((claim) => claim.agentName === normalizedAgent) ?? null
        : null;
      const claimedByOther = normalizedAgent
        ? row.claims.find((claim) => claim.agentName !== normalizedAgent) ?? null
        : null;
      if (claimedByOther && !activeClaim) return null;

      const bountyAmount = getActiveBountyAmount(row.bounty);
      const ageHours = Math.max(0, (Date.now() - row.createdAt.getTime()) / (60 * 60 * 1000));
      const answerCount = row._count.answers;
      const tags = row.tags.map((link) => link.tag.name);
      const matchedTags = preferredTags.size > 0
        ? tags.filter((tag) => preferredTags.has(tag.toLowerCase()))
        : [];
      const unansweredBonus = answerCount === 0 ? 450 : 0;
      const sourceWeight = sourcePriorityWeight(row.sourceType);
      const claimBonus = activeClaim ? 100 : 0;
      const score =
        (bountyAmount * 1000) +
        unansweredBonus +
        (matchedTags.length * 140) +
        (Math.min(120, ageHours) * 1.5) +
        sourceWeight +
        claimBonus -
        (answerCount * 35);
      const reasons: string[] = [];
      if (bountyAmount > 0) reasons.push(`bounty_${bountyAmount}`);
      if (unansweredBonus > 0) reasons.push('unanswered');
      if (matchedTags.length > 0) reasons.push(`tag_match_${matchedTags.length}`);
      if (sourceWeight > 0) reasons.push(`source_${row.sourceType ?? 'none'}`);
      if (activeClaim) reasons.push('already_claimed_by_agent');

      return {
        id: row.id,
        title: row.title,
        tags,
        source: getQuestionSource(row),
        answerCount,
        bounty: bountyAmount > 0
          ? {
              amount: bountyAmount,
              currency: row.bounty?.currency ?? 'credits',
              expiresAt: row.bounty?.expiresAt ?? null
            }
          : null,
        createdAt: row.createdAt,
        score,
        reasons,
        matchedTags,
        activeClaim: activeClaim
          ? {
              id: activeClaim.id,
              agentName: activeClaim.agentName,
              expiresAt: activeClaim.expiresAt,
              state: activeClaim.state
            }
          : null
      };
    })
    .filter((row): row is RecommendedQuestion => Boolean(row))
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return b.createdAt.getTime() - a.createdAt.getTime();
    });

  return ranked[0] ?? null;
}

function formatRecommendedQuestion(
  recommended: RecommendedQuestion,
  baseUrl: string
) {
  return {
    id: recommended.id,
    title: recommended.title,
    tags: recommended.tags,
    source: recommended.source,
    answerCount: recommended.answerCount,
    bounty: recommended.bounty,
    score: Number(recommended.score.toFixed(2)),
    reasons: recommended.reasons,
    matchedTags: recommended.matchedTags,
    activeClaim: recommended.activeClaim,
    url: `${baseUrl}/q/${recommended.id}`,
    actions: {
      answerJob: `POST /api/v1/questions/${recommended.id}/answer-job`,
      claim: `POST /api/v1/questions/${recommended.id}/claim`
    }
  };
}

async function getWeeklySolvedLeaderboard(weeks: number, take: number, includeSynthetic: boolean) {
  const startWeek = startOfUtcWeek(new Date());
  startWeek.setUTCDate(startWeek.getUTCDate() - ((weeks - 1) * 7));

  const rows = await prisma.$queryRaw<Array<{
    week: Date | string;
    agentName: string;
    solved: bigint | number | string;
  }>>`
    SELECT
      date_trunc('week', qr."createdAt") AS week,
      COALESCE(NULLIF(a."agentName", ''), CONCAT('user:', a."userId")) AS "agentName",
      COUNT(*) AS solved
    FROM "QuestionResolution" qr
    JOIN "Answer" a ON a."id" = qr."answerId"
    WHERE qr."createdAt" >= ${startWeek}
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
  `;

  const byWeek = new Map<string, Array<{ agentName: string; solved: number }>>();
  for (const row of rows) {
    const weekDate = row.week instanceof Date ? row.week : new Date(row.week);
    const weekStart = weekDate.toISOString().slice(0, 10);
    const agentName = normalizeAgentOrNull(row.agentName);
    if (!agentName) continue;
    if (!includeSynthetic && isSyntheticAgentName(agentName)) continue;
    const current = byWeek.get(weekStart) ?? [];
    current.push({
      agentName,
      solved: toNumber(row.solved)
    });
    byWeek.set(weekStart, current);
  }

  const timeline = Array.from(byWeek.entries())
    .sort((a, b) => b[0].localeCompare(a[0]))
    .map(([weekStart, values]) => ({
      weekStart,
      leaders: values
        .sort((a, b) => {
          if (b.solved !== a.solved) return b.solved - a.solved;
          return a.agentName.localeCompare(b.agentName);
        })
        .slice(0, take)
    }));

  return {
    weeks,
    includeSynthetic,
    timeline
  };
}

async function ensureTrialAutoSubscription(agentName: string) {
  if (!TRIAL_AUTO_SUBSCRIBE) {
    return { enabled: false, created: false, id: null, events: [] as string[], tags: [] as string[] };
  }

  const validEvents = new Set<string>(SUBSCRIPTION_EVENT_TYPES);
  const events = Array.from(
    new Set(
      TRIAL_AUTO_SUBSCRIBE_EVENTS_RAW.filter((value) => validEvents.has(value))
    )
  );
  const effectiveEvents = events.length > 0
    ? events
    : [...SUBSCRIPTION_DEFAULT_EVENTS];
  const tags = normalizeTags(TRIAL_AUTO_SUBSCRIBE_TAGS_RAW);

  const existing = await prisma.questionSubscription.findFirst({
    where: {
      agentName,
      active: true,
      webhookUrl: null,
      webhookSecret: null,
      tags: { equals: tags },
      events: { equals: effectiveEvents }
    },
    orderBy: { createdAt: 'desc' }
  });
  if (existing) {
    return {
      enabled: true,
      created: false,
      id: existing.id,
      events: effectiveEvents,
      tags
    };
  }

  const created = await prisma.questionSubscription.create({
    data: {
      agentName,
      tags,
      events: effectiveEvents,
      active: true
    }
  });
  return {
    enabled: true,
    created: true,
    id: created.id,
    events: effectiveEvents,
    tags
  };
}

function base64UrlEncode(value: string) {
  return Buffer.from(value, 'utf8').toString('base64url');
}

function base64UrlDecode(value: string) {
  return Buffer.from(value, 'base64url').toString('utf8');
}

type AcceptLinkClaims = {
  q: string;
  a: string;
  u: string;
  e: number;
};

function createAcceptLinkToken(claims: AcceptLinkClaims) {
  if (!ACCEPT_LINK_SECRET) return null;
  const payload = base64UrlEncode(JSON.stringify(claims));
  const signature = crypto.createHmac('sha256', ACCEPT_LINK_SECRET).update(payload).digest('base64url');
  // Envelope payload+signature so token remains strictly base64url (route-safe).
  return base64UrlEncode(JSON.stringify({ p: payload, s: signature }));
}

function parseAcceptLinkToken(token: string) {
  if (!ACCEPT_LINK_SECRET) return null;
  const normalized = decodeURIComponent(token);
  let payload = '';
  let signature = '';
  try {
    const envelope = JSON.parse(base64UrlDecode(normalized)) as { p?: unknown; s?: unknown };
    if (typeof envelope?.p === 'string' && typeof envelope?.s === 'string') {
      payload = envelope.p;
      signature = envelope.s;
    }
  } catch {
    // Backward compatible: accept legacy "." and "~" token formats.
    const separator = normalized.includes('~') ? '~' : '.';
    [payload, signature] = normalized.split(separator);
  }
  if (!payload || !signature) return null;
  const expected = crypto.createHmac('sha256', ACCEPT_LINK_SECRET).update(payload).digest('base64url');
  if (expected.length !== signature.length) return null;
  if (!crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(signature))) return null;
  try {
    const parsed = JSON.parse(base64UrlDecode(payload)) as Partial<AcceptLinkClaims>;
    if (!parsed || typeof parsed.q !== 'string' || typeof parsed.a !== 'string' || typeof parsed.u !== 'string') return null;
    const expiresAtMs = Number(parsed.e);
    if (!Number.isFinite(expiresAtMs) || expiresAtMs < Date.now()) return null;
    return {
      questionId: parsed.q,
      answerId: parsed.a,
      ownerUserId: parsed.u,
      expiresAtMs
    };
  } catch {
    return null;
  }
}

function extractAcceptToken(input: string) {
  const raw = input.trim();
  if (!raw) return '';
  if (!/^https?:\/\//i.test(raw)) return raw;
  try {
    const parsed = new URL(raw);
    const queryToken = parsed.searchParams.get('token');
    if (queryToken) return queryToken;
    const segments = parsed.pathname.split('/').filter(Boolean);
    const idx = segments.findIndex((segment) => segment === 'accept-links');
    if (idx >= 0 && idx + 1 < segments.length) return segments[idx + 1];
  } catch {
    return raw;
  }
  return raw;
}

async function acceptAnswerFromToken(token: string, acceptedByAgentName: string | null, baseUrl: string) {
  const claims = parseAcceptLinkToken(token);
  if (!claims) {
    return {
      status: 401,
      payload: { error: 'Invalid or expired accept link.' }
    };
  }
  const result = await acceptAnswerForQuestion({
    questionId: claims.questionId,
    answerId: claims.answerId,
    ownerUserId: claims.ownerUserId,
    acceptedByAgentName,
    baseUrl
  });
  return {
    status: result.status,
    payload: {
      ...result.payload,
      acceptedVia: 'accept_link',
      linkExpiresAt: new Date(claims.expiresAtMs).toISOString()
    }
  };
}

function buildAcceptLink(baseUrl: string, questionId: string, answerId: string, ownerUserId: string, ttlMinutes = ACCEPT_LINK_TTL_MINUTES) {
  const expiresAtMs = Date.now() + Math.max(5, ttlMinutes) * 60 * 1000;
  const token = createAcceptLinkToken({
    q: questionId,
    a: answerId,
    u: ownerUserId,
    e: expiresAtMs
  });
  if (!token) return null;
  return {
    token,
    url: `${baseUrl}/api/v1/accept-links?token=${encodeURIComponent(token)}`,
    expiresAt: new Date(expiresAtMs).toISOString()
  };
}

async function ensureUserHandle(handle: string) {
  const normalized = handle.trim().toLowerCase();
  return prisma.user.upsert({
    where: { handle: normalized },
    update: {},
    create: { handle: normalized }
  });
}

async function processAcceptanceReminders(baseUrl: string, limit = ACCEPTANCE_REMINDER_LIMIT) {
  const now = new Date();
  const stages = getReminderStagesHours();
  const take = Math.max(20, Math.min(1000, Math.max(1, limit) * 4));
  const candidates = await prisma.question.findMany({
    where: {
      resolution: null,
      answers: { some: {} }
    },
    include: {
      tags: { include: { tag: true } },
      answers: { orderBy: { createdAt: 'desc' }, take: 1 },
      reminders: {
        where: { sentAt: { not: null } },
        select: { stageHours: true }
      }
    },
    orderBy: { updatedAt: 'desc' },
    take
  });

  const results: Array<{
    questionId: string;
    answerId: string;
    stageHours: number;
  }> = [];

  for (const question of candidates) {
    if (results.length >= limit) break;
    const latestAnswer = question.answers[0];
    if (!latestAnswer) continue;
    const ageHours = (now.getTime() - latestAnswer.createdAt.getTime()) / (60 * 60 * 1000);
    const seenStages = new Set(question.reminders.map((row) => row.stageHours));
    const stageHours = stages.find((stage) => ageHours >= stage && !seenStages.has(stage));
    if (!stageHours) continue;

    const dueAt = new Date(latestAnswer.createdAt.getTime() + stageHours * 60 * 60 * 1000);
    try {
      await prisma.acceptanceReminder.create({
        data: {
          questionId: question.id,
          answerId: latestAnswer.id,
          stageHours,
          dueAt,
          sentAt: now
        }
      });
    } catch {
      continue;
    }

    const acceptLink = buildAcceptLink(baseUrl, question.id, latestAnswer.id, question.userId);
    void dispatchAcceptanceReminderEvent({
      id: question.id,
      title: question.title,
      bodyText: question.bodyText,
      createdAt: question.createdAt,
      tags: question.tags.map((link) => link.tag.name),
      url: `${baseUrl}/q/${question.id}`,
      answerId: latestAnswer.id,
      answerAgentName: latestAnswer.agentName ?? null,
      answerCreatedAt: latestAnswer.createdAt,
      reminderStageHours: stageHours,
      acceptUrl: acceptLink?.url ?? null,
      source: getQuestionSource(question)
    }).catch(() => undefined);

    results.push({
      questionId: question.id,
      answerId: latestAnswer.id,
      stageHours
    });
  }

  return {
    stages,
    processed: candidates.length,
    queued: results.length,
    reminders: results
  };
}

async function processAutoCloseQuestions(baseUrl: string, limit = AUTO_CLOSE_PROCESS_LIMIT) {
  const now = new Date();
  const questionCutoff = new Date(now.getTime() - (AUTO_CLOSE_AFTER_HOURS * 60 * 60 * 1000));
  const answerCutoff = new Date(now.getTime() - (AUTO_CLOSE_MIN_ANSWER_AGE_HOURS * 60 * 60 * 1000));
  const take = Math.max(20, Math.min(1000, Math.max(1, limit) * 4));
  const candidates = await prisma.question.findMany({
    where: {
      resolution: null,
      createdAt: { lte: questionCutoff },
      answers: { some: { createdAt: { lte: answerCutoff } } }
    },
    include: {
      answers: {
        orderBy: { createdAt: 'asc' },
        select: { id: true, createdAt: true }
      }
    },
    orderBy: { createdAt: 'asc' },
    take
  });

  const results: Array<{
    questionId: string;
    answerId: string;
    changed: boolean;
  }> = [];
  let failed = 0;

  for (const question of candidates) {
    if (results.length >= limit) break;
    const answerIds = question.answers.map((row) => row.id);
    if (answerIds.length === 0) continue;
    const scoreRows = await prisma.answerVote.groupBy({
      by: ['answerId'],
      where: { answerId: { in: answerIds } },
      _sum: { value: true }
    });
    const scoreMap = new Map<string, number>(
      scoreRows.map((row) => [row.answerId, row._sum.value ?? 0])
    );
    const best = question.answers
      .map((row) => ({ ...row, score: scoreMap.get(row.id) ?? 0 }))
      .sort((a, b) => {
        if (b.score !== a.score) return b.score - a.score;
        return a.createdAt.getTime() - b.createdAt.getTime();
      })[0];
    if (!best) continue;

    try {
      const accepted = await acceptAnswerForQuestion({
        questionId: question.id,
        answerId: best.id,
        ownerUserId: question.userId,
        acceptedByAgentName: AUTO_CLOSE_AGENT_NAME,
        baseUrl
      });
      if (accepted.status === 200 && accepted.payload.ok) {
        results.push({
          questionId: question.id,
          answerId: best.id,
          changed: Boolean(accepted.payload.changed)
        });
      } else {
        failed += 1;
      }
    } catch {
      failed += 1;
    }
  }

  return {
    processed: candidates.length,
    closed: results.length,
    failed,
    policy: {
      enabled: AUTO_CLOSE_ENABLED,
      afterHours: AUTO_CLOSE_AFTER_HOURS,
      minAnswerAgeHours: AUTO_CLOSE_MIN_ANSWER_AGE_HOURS,
      acceptedBy: AUTO_CLOSE_AGENT_NAME
    },
    results
  };
}

function startBackgroundWorkers() {
  if (!usageFlushTimer) {
    usageFlushTimer = setInterval(() => {
      if (usageEventBuffer.length === 0) return;
      void withPrismaPoolRetry('usage_event_flush', () => flushUsageEventBuffer(), 3).catch((err) => {
        fastify.log.warn({ err }, 'usage event flush failed');
      });
    }, USAGE_LOG_FLUSH_INTERVAL_MS);
    usageFlushTimer.unref?.();
  }

  if (DELIVERY_LOOP_ENABLED && !deliveryLoopTimer) {
    deliveryLoopTimer = setInterval(() => {
      if (deliveryLoopRunning) return;
      deliveryLoopRunning = true;
      void withPrismaPoolRetry('delivery_queue_loop', () => processDeliveryQueue(DELIVERY_PROCESS_LIMIT), 3)
        .catch((err) => {
          fastify.log.warn({ err }, 'delivery queue loop failed');
        })
        .finally(() => {
          deliveryLoopRunning = false;
        });
    }, DELIVERY_LOOP_INTERVAL_MS);
    deliveryLoopTimer.unref?.();
  }

  if (REMINDER_LOOP_ENABLED && !reminderLoopTimer) {
    reminderLoopTimer = setInterval(() => {
      if (reminderLoopRunning) return;
      reminderLoopRunning = true;
      void withPrismaPoolRetry('acceptance_reminder_loop', async () => {
        const summary = await processAcceptanceReminders(SYSTEM_BASE_URL, ACCEPTANCE_REMINDER_LIMIT);
        if (summary.queued > 0) {
          await processDeliveryQueue(Math.min(DELIVERY_PROCESS_LIMIT, summary.queued * 5));
        }
      }, 3)
        .catch((err) => {
          fastify.log.warn({ err }, 'acceptance reminder loop failed');
        })
        .finally(() => {
          reminderLoopRunning = false;
        });
    }, REMINDER_LOOP_INTERVAL_MS);
    reminderLoopTimer.unref?.();
  }

  if (AUTO_CLOSE_ENABLED && !autoCloseLoopTimer) {
    autoCloseLoopTimer = setInterval(() => {
      if (autoCloseLoopRunning) return;
      autoCloseLoopRunning = true;
      void withPrismaPoolRetry('autoclose_loop', async () => {
        const summary = await processAutoCloseQuestions(SYSTEM_BASE_URL, AUTO_CLOSE_PROCESS_LIMIT);
        if (summary.closed > 0) {
          await processDeliveryQueue(Math.min(DELIVERY_PROCESS_LIMIT, summary.closed * 5));
        }
      }, 3)
        .catch((err) => {
          fastify.log.warn({ err }, 'autoclose loop failed');
        })
        .finally(() => {
          autoCloseLoopRunning = false;
        });
    }, AUTO_CLOSE_LOOP_INTERVAL_MS);
    autoCloseLoopTimer.unref?.();
  }

  fastify.log.info({
    usageLogFlushMs: USAGE_LOG_FLUSH_INTERVAL_MS,
    deliveryLoopEnabled: DELIVERY_LOOP_ENABLED,
    deliveryLoopMs: DELIVERY_LOOP_INTERVAL_MS,
    reminderLoopEnabled: REMINDER_LOOP_ENABLED,
    reminderLoopMs: REMINDER_LOOP_INTERVAL_MS,
    autoCloseEnabled: AUTO_CLOSE_ENABLED,
    autoCloseLoopMs: AUTO_CLOSE_LOOP_INTERVAL_MS,
    autoCloseAfterHours: AUTO_CLOSE_AFTER_HOURS,
    autoCloseMinAnswerAgeHours: AUTO_CLOSE_MIN_ANSWER_AGE_HOURS
  }, 'background workers started');
}

async function stopBackgroundWorkers() {
  if (usageFlushTimer) {
    clearInterval(usageFlushTimer);
    usageFlushTimer = null;
  }
  if (deliveryLoopTimer) {
    clearInterval(deliveryLoopTimer);
    deliveryLoopTimer = null;
  }
  if (reminderLoopTimer) {
    clearInterval(reminderLoopTimer);
    reminderLoopTimer = null;
  }
  if (autoCloseLoopTimer) {
    clearInterval(autoCloseLoopTimer);
    autoCloseLoopTimer = null;
  }

  if (usageEventFlushPromise) {
    try {
      await usageEventFlushPromise;
    } catch {
      // swallow during shutdown
    }
  }
  if (usageEventBuffer.length > 0) {
    try {
      await withPrismaPoolRetry('usage_event_flush_shutdown', () => flushUsageEventBuffer(usageEventBuffer.length), 2);
    } catch {
      // swallow during shutdown
    }
  }
}

async function acceptAnswerForQuestion(input: {
  questionId: string;
  answerId: string;
  ownerUserId: string;
  acceptedByAgentName: string | null;
  baseUrl: string;
}) {
  const question = await prisma.question.findUnique({
    where: { id: input.questionId },
    include: {
      resolution: true,
      bounty: true,
      tags: { include: { tag: true } }
    }
  });
  if (!question) return { status: 404, payload: { error: 'Question not found' } };
  if (question.userId !== input.ownerUserId) {
    return { status: 403, payload: { error: 'Only the question owner can accept an answer.' } };
  }

  const target = await prisma.answer.findFirst({
    where: { id: input.answerId, questionId: input.questionId },
    select: { id: true, agentName: true, userId: true, createdAt: true }
  });
  if (!target) return { status: 404, payload: { error: 'Answer not found for this question.' } };

  if (question.resolution?.answerId === target.id) {
    return {
      status: 200,
      payload: {
        ok: true,
        questionId: input.questionId,
        acceptedAnswerId: target.id,
        changed: false
      }
    };
  }

  const prevAnswerId = question.resolution?.answerId ?? null;
  let previousAgentName: string | null = null;
  if (prevAnswerId) {
    const prevAnswer = await prisma.answer.findUnique({
      where: { id: prevAnswerId },
      select: { agentName: true }
    });
    previousAgentName = normalizeAgentOrNull(prevAnswer?.agentName ?? null);
  }
  const targetAgentName = normalizeAgentOrNull(target.agentName);
  const acceptedAt = new Date();
  const bountyAmount = getActiveBountyAmount(question.bounty);
  const isSelfAccept = target.userId === question.userId;
  const shouldPayoutBounty = bountyAmount > 0 && !isSelfAccept;
  const payoutReason = shouldPayoutBounty
    ? 'payout_applied'
    : (isSelfAccept ? 'self_accept_no_payout' : 'no_active_bounty');
  let starterBonusPaid = 0;

  await prisma.$transaction(async (tx) => {
    await tx.questionClaim.updateMany({
      where: {
        questionId: input.questionId,
        state: { in: ['claimed', 'answered'] },
        expiresAt: { lt: acceptedAt }
      },
      data: {
        state: 'expired',
        releasedAt: acceptedAt,
        verifyReason: 'claim_ttl_elapsed'
      }
    });

    await tx.questionResolution.upsert({
      where: { questionId: input.questionId },
      create: {
        questionId: input.questionId,
        answerId: target.id,
        acceptedByAgentName: input.acceptedByAgentName
      },
      update: {
        answerId: target.id,
        acceptedByAgentName: input.acceptedByAgentName
      }
    });

    if (previousAgentName && previousAgentName !== targetAgentName) {
      await tx.agentProfile.upsert({
        where: { name: previousAgentName },
        update: {
          reputation: { decrement: ACCEPT_REPUTATION_REWARD },
          acceptedCount: { decrement: 1 }
        },
        create: {
          name: previousAgentName,
          reputation: -ACCEPT_REPUTATION_REWARD,
          acceptedCount: -1
        }
      });
    }

    let targetAcceptedBefore = false;
    if (targetAgentName) {
      const profileBefore = await tx.agentProfile.findUnique({
        where: { name: targetAgentName },
        select: { acceptedCount: true }
      });
      targetAcceptedBefore = (profileBefore?.acceptedCount ?? 0) > 0;
      await tx.agentProfile.upsert({
        where: { name: targetAgentName },
        update: {
          reputation: { increment: ACCEPT_REPUTATION_REWARD },
          acceptedCount: { increment: 1 }
        },
        create: {
          name: targetAgentName,
          reputation: ACCEPT_REPUTATION_REWARD,
          acceptedCount: 1
        }
      });

      if (STARTER_BONUS_CREDITS > 0 && !targetAcceptedBefore) {
        const priorStarter = await tx.agentCreditLedger.findFirst({
          where: {
            agentName: targetAgentName,
            reason: 'starter_bonus_first_accepted'
          },
          select: { id: true }
        });
        if (!priorStarter) {
          starterBonusPaid = STARTER_BONUS_CREDITS;
          await tx.agentProfile.upsert({
            where: { name: targetAgentName },
            update: { credits: { increment: STARTER_BONUS_CREDITS } },
            create: { name: targetAgentName, credits: STARTER_BONUS_CREDITS }
          });
          await tx.agentCreditLedger.create({
            data: {
              agentName: targetAgentName,
              delta: STARTER_BONUS_CREDITS,
              reason: 'starter_bonus_first_accepted',
              questionId: input.questionId,
              answerId: target.id
            }
          });
        }
      }
    }

    if (shouldPayoutBounty) {
      if (targetAgentName) {
        await tx.agentProfile.upsert({
          where: { name: targetAgentName },
          update: { credits: { increment: bountyAmount } },
          create: { name: targetAgentName, credits: bountyAmount }
        });
        await tx.agentCreditLedger.create({
          data: {
            agentName: targetAgentName,
            delta: bountyAmount,
            reason: 'bounty_payout',
            questionId: input.questionId,
            answerId: target.id
          }
        });
      }
      if (question.bounty) {
        await tx.questionBounty.update({
          where: { questionId: input.questionId },
          data: { active: false }
        });
      }
    }

    if (targetAgentName) {
      const targetClaim = await tx.questionClaim.findFirst({
        where: {
          questionId: input.questionId,
          agentName: targetAgentName,
          state: { in: ['claimed', 'answered', 'verified'] },
          OR: [{ answerId: target.id }, { answerId: null }]
        },
        orderBy: { createdAt: 'desc' }
      });
      if (targetClaim) {
        await tx.questionClaim.update({
          where: { id: targetClaim.id },
          data: {
            state: 'verified',
            answerId: target.id,
            answeredAt: targetClaim.answeredAt ?? acceptedAt,
            verifiedAt: acceptedAt,
            expiresAt: acceptedAt,
            verifiedByAgent: input.acceptedByAgentName,
            verifyReason: shouldPayoutBounty ? 'accepted_with_bounty' : payoutReason
          }
        });
      } else {
        await tx.questionClaim.create({
          data: {
            questionId: input.questionId,
            agentName: targetAgentName,
            state: 'verified',
            expiresAt: acceptedAt,
            answerId: target.id,
            answeredAt: acceptedAt,
            verifiedAt: acceptedAt,
            verifiedByAgent: input.acceptedByAgentName,
            verifyReason: shouldPayoutBounty ? 'accepted_with_bounty' : payoutReason
          }
        });
      }
    }

    const competingClaimWhere: Prisma.QuestionClaimWhereInput = {
      questionId: input.questionId,
      state: { in: ['claimed', 'answered'] }
    };
    if (targetAgentName) {
      competingClaimWhere.agentName = { not: targetAgentName };
    }
    await tx.questionClaim.updateMany({
      where: competingClaimWhere,
      data: {
        state: 'released',
        releasedAt: acceptedAt,
        verifyReason: 'accepted_elsewhere'
      }
    });
  });

  void dispatchQuestionAcceptedEvent({
    id: question.id,
    title: question.title,
    bodyText: question.bodyText,
    createdAt: question.createdAt,
    tags: question.tags.map((link) => link.tag.name),
    url: `${input.baseUrl}/q/${question.id}`,
    acceptedAt,
    acceptedAnswerId: target.id,
    acceptedAgentName: targetAgentName,
    acceptedByAgentName: input.acceptedByAgentName,
    bountyPaid: shouldPayoutBounty ? bountyAmount : 0,
    starterBonusPaid,
    source: getQuestionSource(question)
  }).catch(() => undefined);

  return {
    status: 200,
    payload: {
      ok: true,
      changed: true,
      questionId: input.questionId,
      acceptedAnswerId: target.id,
      previousAnswerId: prevAnswerId,
      bountyPaid: shouldPayoutBounty ? bountyAmount : 0,
      payout: {
        eligible: bountyAmount > 0,
        applied: shouldPayoutBounty,
        amount: shouldPayoutBounty ? bountyAmount : 0,
        currency: question.bounty?.currency ?? 'credits',
        reason: payoutReason
      },
      starterBonus: {
        eligible: Boolean(targetAgentName) && STARTER_BONUS_CREDITS > 0,
        applied: starterBonusPaid > 0,
        amount: starterBonusPaid
      },
      completion: {
        verified: Boolean(targetAgentName),
        acceptedAt: acceptedAt.toISOString(),
        acceptedByAgentName: input.acceptedByAgentName
      }
    }
  };
}

fastify.addHook('onRequest', async (request, reply) => {
  (request as { startTimeNs?: bigint }).startTimeNs = process.hrtime.bigint();
  if (request.method === 'GET') {
    const rawUrl = request.raw.url ?? request.url;
    if (rawUrl) {
      const canonical = getCanonicalWellKnownPath(rawUrl);
      if (canonical && stripQuery(rawUrl) !== canonical) {
        const queryIndex = rawUrl.indexOf('?');
        const query = queryIndex === -1 ? '' : rawUrl.slice(queryIndex);
        reply.redirect(`${canonical}${query}`, 301);
        return;
      }
    }
  }
});

fastify.addHook('preHandler', async (request) => {
  if (!CAPTURE_AGENT_PAYLOADS) return;
  const route = resolveRoute(request as RouteRequest);
  if (!CAPTURED_ROUTES.has(route)) return;
  const userAgent = normalizeHeader(request.headers['user-agent']).slice(0, 256) || null;
  const agentName = getAgentName(request.headers);
  if (!isAgentTraffic(agentName, userAgent)) return;
  const payload = buildRequestPayload(request as { body?: unknown; query?: unknown; params?: unknown });
  (request as { payloadCapture?: { requestBody?: unknown; responseBody?: unknown; route?: string } }).payloadCapture = {
    requestBody: payload,
    route
  };
});

fastify.addHook('onSend', async (request, reply, payload) => {
  if (!CAPTURE_AGENT_PAYLOADS) return payload;
  const capture = (request as { payloadCapture?: { requestBody?: unknown; responseBody?: unknown } }).payloadCapture;
  if (capture) {
    capture.responseBody = payload;
  }
  return payload;
});

fastify.addHook('onResponse', async (request, reply) => {
  if (request.method === 'OPTIONS') return;
  const startNs = (request as { startTimeNs?: bigint }).startTimeNs;
  const durationMs = startNs ? Math.max(0, Number(process.hrtime.bigint() - startNs) / 1_000_000) : 0;
  const route = resolveRoute(request as RouteRequest);
  const logNoise = normalizeHeader(process.env.LOG_NOISE) === 'true';
  if (!logNoise && isNoiseEvent({ method: request.method, route, status: reply.statusCode })) {
    return;
  }
  const apiKeyPrefix = extractApiKeyPrefix(request.headers);
  const userAgent = normalizeHeader(request.headers['user-agent']).slice(0, 256) || null;
  const ip = getClientIp(request as RouteRequest & { ip?: string; socket?: { remoteAddress?: string } });
  const referer = normalizeHeader(request.headers.referer).slice(0, 512) || null;
  const agentName = getAgentName(request.headers);

  enqueueUsageEvent({
    method: request.method,
    route,
    status: reply.statusCode,
    durationMs: Math.round(durationMs),
    apiKeyPrefix,
    userAgent,
    ip,
    referer,
    agentName
  });
  if (usageEventBuffer.length >= USAGE_LOG_FLUSH_BATCH_SIZE) {
    void flushUsageEventBuffer().catch((err) => {
      request.log.warn({ err }, 'usage event flush failed');
    });
  }

  const capture = (request as { payloadCapture?: { requestBody?: unknown; responseBody?: unknown; route?: string } }).payloadCapture;
  if (capture) {
    try {
      await storeAgentPayloadEvent({
        source: 'api',
        kind: request.method === 'GET' ? 'rest_read' : 'rest_write',
        method: request.method,
        route: capture.route ?? route,
        status: reply.statusCode,
        durationMs: Math.round(durationMs),
        requestId: request.id,
        agentName,
        userAgent,
        ip,
        apiKeyPrefix,
        requestBody: capture.requestBody,
        responseBody: capture.responseBody
      });
    } catch (err) {
      request.log.warn({ err }, 'agent payload logging failed');
    }
  }
});

fastify.get('/api/openapi.json', async () => {
  return fastify.swagger();
});

fastify.get('/.well-known/agent.json', async (request) => {
  return agentCard(getBaseUrl(request));
});

fastify.get('/.well-known/agent-card.json', async (request) => {
  return agentCard(getBaseUrl(request));
});

fastify.get('/api/v1/health', {
  schema: {
    tags: ['health'],
    response: {
      200: {
        type: 'object',
        properties: {
          ok: { type: 'boolean' }
        }
      }
    }
  }
}, async () => ({ ok: true }));

fastify.get('/robots.txt', async (request, reply) => {
  const baseUrl = PUBLIC_BASE_URL || (process.env.NODE_ENV === 'production'
    ? 'https://a2abench-api.web.app'
    : getBaseUrl(request));
  const lines = [
    'User-agent: *',
    'Disallow: /admin/',
    'Disallow: /api/v1/admin',
    'Disallow: /api/v1/usage',
    'Disallow: /docs/',
    'Allow: /q/',
    'Allow: /.well-known/',
    `Sitemap: ${baseUrl}/sitemap.xml`
  ];
  reply.type('text/plain').send(lines.join('\n'));
});

fastify.get('/sitemap.xml', {
  schema: {
    tags: ['meta'],
    response: {
      200: { type: 'string' }
    }
  }
}, async (request, reply) => {
  const baseUrl = PUBLIC_BASE_URL || (process.env.NODE_ENV === 'production'
    ? 'https://a2abench-api.web.app'
    : getBaseUrl(request));
  try {
    const seedIds = ['seed_v2_01', 'seed_v2_02', 'seed_v2_03', 'seed_v2_04', 'seed_v2_05', 'seed_v2_06'];
    const urls = [
      `${baseUrl}/.well-known/agent.json`,
      `${baseUrl}/api/openapi.json`,
      `${baseUrl}/leaderboard/weekly`,
      `${baseUrl}/q/demo_q1`,
      ...seedIds.map((id) => `${baseUrl}/q/${id}`)
    ];
    const body = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.map((loc) => `  <url><loc>${loc}</loc></url>`).join('\n')}
</urlset>`;
    reply.type('application/xml').send(body);
  } catch (err) {
    request.log.warn({ err }, 'sitemap generation failed');
    const fallback = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>${baseUrl}/.well-known/agent.json</loc></url>
  <url><loc>${baseUrl}/api/openapi.json</loc></url>
  <url><loc>${baseUrl}/leaderboard/weekly</loc></url>
</urlset>`;
    reply.type('application/xml').send(fallback);
  }
});

fastify.get('/api/v1/usage/summary', {
  schema: {
    tags: ['usage', 'admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        days: { type: 'integer', minimum: 1, maximum: 90 },
        includeNoise: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { days?: number; includeNoise?: boolean };
  const days = Math.min(90, Math.max(1, Number(query.days ?? 7)));
  return getUsageSummaryCached(days, Boolean(query.includeNoise));
});

async function getUsageSummary(days: number, includeNoise: boolean) {
  const now = new Date();
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  const previousSince = new Date(since.getTime() - days * 24 * 60 * 60 * 1000);
  const last24h = new Date(Date.now() - 24 * 60 * 60 * 1000);
  const noiseWhere: Prisma.UsageEventWhereInput = {
    OR: [
      { route: '/api/v1/auth/trial-key', method: { in: ['GET', 'HEAD'] }, status: 405 },
      { route: '/api/v1/questions/:id/answers', method: { in: ['GET', 'HEAD'] }, status: 405 },
      { route: '/q/:id', method: { in: ['GET', 'HEAD'] }, status: 400 },
      { route: '/api/v1/questions/:id', method: { in: ['GET', 'HEAD'] }, status: 400 },
      { route: '/', method: { in: ['GET', 'HEAD'] }, status: 404 },
      { route: '/api/v1/fetch', method: { in: ['GET', 'HEAD'] }, status: 404 },
      { route: '/docs/.well-known/agent.json', method: { in: ['GET', 'HEAD'] }, status: 404 },
      {
        AND: [
          { route: { endsWith: '/.well-known/agent.json' } },
          { route: { not: '/.well-known/agent.json' } },
          { method: { in: ['GET', 'HEAD'] } },
          { status: { in: [301, 308, 404] } }
        ]
      },
      {
        AND: [
          { route: { endsWith: '/.well-known/agent-card.json' } },
          { route: { not: '/.well-known/agent-card.json' } },
          { method: { in: ['GET', 'HEAD'] } },
          { status: { in: [301, 308, 404] } }
        ]
      },
      { route: '/api/v1/usage/summary', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } },
      { route: '/admin/usage', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } },
      { route: '/admin/usage/data', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } },
      { route: '/admin/agent-events', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } },
      { route: '/admin/agent-events/data', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } }
    ]
  };
  const usageWhere = includeNoise
    ? { createdAt: { gte: since } }
    : { AND: [{ createdAt: { gte: since } }, { NOT: noiseWhere }] };
  const last24hWhere = includeNoise
    ? { createdAt: { gte: last24h } }
    : { AND: [{ createdAt: { gte: last24h } }, { NOT: noiseWhere }] };
  const noiseSql = includeNoise
    ? Prisma.empty
    : Prisma.sql`
      AND NOT (
        ("route" = '/api/v1/auth/trial-key' AND "method" IN ('GET','HEAD') AND "status" = 405)
        OR ("route" = '/api/v1/questions/:id/answers' AND "method" IN ('GET','HEAD') AND "status" = 405)
        OR ("route" = '/q/:id' AND "method" IN ('GET','HEAD') AND "status" = 400)
        OR ("route" = '/api/v1/questions/:id' AND "method" IN ('GET','HEAD') AND "status" = 400)
        OR ("route" = '/' AND "method" IN ('GET','HEAD') AND "status" = 404)
        OR ("route" = '/api/v1/fetch' AND "method" IN ('GET','HEAD') AND "status" = 404)
        OR ("route" = '/docs/.well-known/agent.json' AND "method" IN ('GET','HEAD') AND "status" = 404)
        OR ("route" LIKE '%/.well-known/agent.json' AND "route" <> '/.well-known/agent.json' AND "method" IN ('GET','HEAD') AND "status" IN (301,308,404))
        OR ("route" LIKE '%/.well-known/agent-card.json' AND "route" <> '/.well-known/agent-card.json' AND "method" IN ('GET','HEAD') AND "status" IN (301,308,404))
        OR ("route" = '/api/v1/usage/summary' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
        OR ("route" = '/admin/usage' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
        OR ("route" = '/admin/usage/data' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
        OR ("route" = '/admin/agent-events' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
        OR ("route" = '/admin/agent-events/data' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
      )
    `;

  const total = await prisma.usageEvent.count({ where: usageWhere });
  const lastDay = await prisma.usageEvent.count({ where: last24hWhere });
  const byRoute = await prisma.usageEvent.groupBy({
    by: ['route'],
    where: usageWhere,
    _count: { route: true },
    orderBy: { _count: { route: 'desc' } },
    take: 10
  });
  const byStatus = await prisma.usageEvent.groupBy({
    by: ['status'],
    where: usageWhere,
    _count: { status: true },
    orderBy: { _count: { status: 'desc' } }
  });
  const dailyRows = await prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
    SELECT date_trunc('day', "createdAt") AS day, COUNT(*) AS count
    FROM "UsageEvent"
    WHERE "createdAt" >= ${since}
    ${noiseSql}
    GROUP BY 1
    ORDER BY day ASC
  `;
  const totalQuestions = await prisma.question.count();
  const totalAnswers = await prisma.answer.count();
  const questionsInRange = await prisma.question.count({ where: { createdAt: { gte: since } } });
  const answersInRange = await prisma.answer.count({ where: { createdAt: { gte: since } } });
  const previousQuestionsInRange = await prisma.question.count({ where: { createdAt: { gte: previousSince, lt: since } } });
  const previousAnswersInRange = await prisma.answer.count({ where: { createdAt: { gte: previousSince, lt: since } } });
  const questionsAnsweredInRange = await prisma.question.count({ where: { createdAt: { gte: since }, answers: { some: {} } } });
  const questionsAcceptedInRange = await prisma.questionResolution.count({ where: { question: { createdAt: { gte: since } } } });
  const unansweredTotal = await prisma.question.count({ where: { answers: { none: {} } } });
  const uniqueAskersInRangeRows = await prisma.$queryRaw<Array<{ count: bigint | number | string }>>`
    SELECT COUNT(DISTINCT "userId") AS count
    FROM "Question"
    WHERE "createdAt" >= ${since}
  `;
  const uniqueAskersPreviousRangeRows = await prisma.$queryRaw<Array<{ count: bigint | number | string }>>`
    SELECT COUNT(DISTINCT "userId") AS count
    FROM "Question"
    WHERE "createdAt" >= ${previousSince} AND "createdAt" < ${since}
  `;
  const uniqueAnswerersInRangeRows = await prisma.$queryRaw<Array<{ count: bigint | number | string }>>`
    SELECT COUNT(DISTINCT COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId"))) AS count
    FROM "Answer"
    WHERE "createdAt" >= ${since}
  `;
  const uniqueAnswerersPreviousRangeRows = await prisma.$queryRaw<Array<{ count: bigint | number | string }>>`
    SELECT COUNT(DISTINCT COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId"))) AS count
    FROM "Answer"
    WHERE "createdAt" >= ${previousSince} AND "createdAt" < ${since}
  `;
  const tractionAskerDailyRows = await prisma.$queryRaw<Array<{ day: Date | string; askers: bigint | number | string }>>`
    SELECT date_trunc('day', "createdAt") AS day, COUNT(DISTINCT "userId") AS askers
    FROM "Question"
    WHERE "createdAt" >= ${since}
    GROUP BY 1
    ORDER BY day ASC
  `;
  const tractionAnswererDailyRows = await prisma.$queryRaw<Array<{ day: Date | string; answerers: bigint | number | string }>>`
    SELECT
      date_trunc('day', "createdAt") AS day,
      COUNT(DISTINCT COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId"))) AS answerers
    FROM "Answer"
    WHERE "createdAt" >= ${since}
    GROUP BY 1
    ORDER BY day ASC
  `;
  const questionDailyRows = await prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
    SELECT date_trunc('day', "createdAt") AS day, COUNT(*) AS count
    FROM "Question"
    WHERE "createdAt" >= ${since}
    GROUP BY 1
    ORDER BY day ASC
  `;
  const answerDailyRows = await prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
    SELECT date_trunc('day', "createdAt") AS day, COUNT(*) AS count
    FROM "Answer"
    WHERE "createdAt" >= ${since}
    GROUP BY 1
    ORDER BY day ASC
  `;

  const qaByDay = new Map<string, { day: string; questions: number; answers: number }>();
  for (const row of questionDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = qaByDay.get(day) ?? { day, questions: 0, answers: 0 };
    existing.questions = Number(row.count);
    qaByDay.set(day, existing);
  }
  for (const row of answerDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = qaByDay.get(day) ?? { day, questions: 0, answers: 0 };
    existing.answers = Number(row.count);
    qaByDay.set(day, existing);
  }

  const startDay = new Date(Date.UTC(since.getUTCFullYear(), since.getUTCMonth(), since.getUTCDate()));
  const endDay = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const cursor = new Date(startDay);
  while (cursor <= endDay) {
    const day = cursor.toISOString().slice(0, 10);
    if (!qaByDay.has(day)) {
      qaByDay.set(day, { day, questions: 0, answers: 0 });
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }

  const qaDaily = Array.from(qaByDay.values()).sort((a, b) => a.day.localeCompare(b.day));
  const uniqueAskersInRange = toNumber(uniqueAskersInRangeRows[0]?.count);
  const uniqueAskersPreviousRange = toNumber(uniqueAskersPreviousRangeRows[0]?.count);
  const uniqueAnswerersInRange = toNumber(uniqueAnswerersInRangeRows[0]?.count);
  const uniqueAnswerersPreviousRange = toNumber(uniqueAnswerersPreviousRangeRows[0]?.count);

  const firstAnswerSampleCount = 0;
  const firstAnswerWithin24hCount = 0;
  const firstAnswerAvgMinutes: number | null = null;
  const firstAnswerP50Minutes: number | null = null;
  const firstAnswerP95Minutes: number | null = null;

  const tractionByDay = new Map<string, {
    day: string;
    questions: number;
    answeredQuestions: number;
    acceptedQuestions: number;
    askers: number;
    answerers: number;
    avgFirstAnswerMinutes: number | null;
  }>();

  for (const row of questionDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    tractionByDay.set(day, {
      day,
      questions: toNumber(row.count),
      answeredQuestions: 0,
      acceptedQuestions: 0,
      askers: 0,
      answerers: 0,
      avgFirstAnswerMinutes: null
    });
  }

  for (const row of answerDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = tractionByDay.get(day) ?? {
      day,
      questions: 0,
      answeredQuestions: 0,
      acceptedQuestions: 0,
      askers: 0,
      answerers: 0,
      avgFirstAnswerMinutes: null
    };
    existing.answeredQuestions = toNumber(row.count);
    tractionByDay.set(day, existing);
  }

  for (const row of tractionAskerDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = tractionByDay.get(day) ?? {
      day,
      questions: 0,
      answeredQuestions: 0,
      acceptedQuestions: 0,
      askers: 0,
      answerers: 0,
      avgFirstAnswerMinutes: null
    };
    existing.askers = toNumber(row.askers);
    tractionByDay.set(day, existing);
  }

  for (const row of tractionAnswererDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = tractionByDay.get(day) ?? {
      day,
      questions: 0,
      answeredQuestions: 0,
      acceptedQuestions: 0,
      askers: 0,
      answerers: 0,
      avgFirstAnswerMinutes: null
    };
    existing.answerers = toNumber(row.answerers);
    tractionByDay.set(day, existing);
  }

  const tractionCursor = new Date(startDay);
  while (tractionCursor <= endDay) {
    const day = tractionCursor.toISOString().slice(0, 10);
    if (!tractionByDay.has(day)) {
      tractionByDay.set(day, {
        day,
        questions: 0,
        answeredQuestions: 0,
        acceptedQuestions: 0,
        askers: 0,
        answerers: 0,
        avgFirstAnswerMinutes: null
      });
    }
    tractionCursor.setUTCDate(tractionCursor.getUTCDate() + 1);
  }

  const tractionDaily = Array.from(tractionByDay.values())
    .sort((a, b) => a.day.localeCompare(b.day))
    .map((row) => ({
      ...row,
      answerCoverageRate: ratio(row.answeredQuestions, row.questions),
      acceptedRate: ratio(row.acceptedQuestions, row.questions)
    }));

  const growthQuestions = growthRate(questionsInRange, previousQuestionsInRange);
  const growthAnswers = growthRate(answersInRange, previousAnswersInRange);
  const growthAskers = growthRate(uniqueAskersInRange, uniqueAskersPreviousRange);
  const growthAnswerers = growthRate(uniqueAnswerersInRange, uniqueAnswerersPreviousRange);
  const rateAnswerCoverage = ratio(questionsAnsweredInRange, questionsInRange);
  const rateAcceptedOfQuestions = ratio(questionsAcceptedInRange, questionsInRange);
  const rateAcceptedOfAnswered = ratio(questionsAcceptedInRange, questionsAnsweredInRange);
  const rateAnswersPerQuestion = ratio(answersInRange, questionsInRange);
  const latency24hRate = firstAnswerSampleCount > 0
    ? ratio(firstAnswerWithin24hCount, firstAnswerSampleCount)
    : null;

  const scoreCoverage = clamp(rateAnswerCoverage, 0, 1);
  const scoreAcceptance = clamp(rateAcceptedOfAnswered, 0, 1);
  const scoreDepth = clamp(rateAnswersPerQuestion / 2, 0, 1);
  const scoreAskers = clamp(growthAskers == null ? 0.5 : 0.5 + growthAskers / 2, 0, 1);
  const scoreAnswerers = clamp(growthAnswerers == null ? 0.5 : 0.5 + growthAnswerers / 2, 0, 1);
  const scoreQuestionGrowth = clamp(growthQuestions == null ? 0.5 : 0.5 + growthQuestions / 2, 0, 1);
  const scoreAnswerGrowth = clamp(growthAnswers == null ? 0.5 : 0.5 + growthAnswers / 2, 0, 1);
  const scoreLatency = clamp(latency24hRate ?? 0.5, 0, 1);
  const weightedScoreRaw = (
    scoreCoverage * 0.24 +
    scoreAcceptance * 0.14 +
    scoreDepth * 0.08 +
    scoreAskers * 0.14 +
    scoreAnswerers * 0.14 +
    scoreQuestionGrowth * 0.12 +
    scoreAnswerGrowth * 0.08 +
    scoreLatency * 0.06
  );
  const tractionScore = Math.round(weightedScoreRaw * 1000) / 10;

  return {
    days,
    since: since.toISOString(),
    total,
    last24h: lastDay,
    byRoute: byRoute.map((row) => ({ route: row.route, count: row._count.route })),
    byStatus: byStatus.map((row) => ({ status: row.status, count: row._count.status })),
    byIp: [],
    byReferer: [],
    byUserAgent: [],
    byAgentName: [],
    recentErrors: [],
    daily: dailyRows.map((row) => {
      const date = row.day instanceof Date ? row.day : new Date(row.day);
      return {
        day: date.toISOString().slice(0, 10),
        count: Number(row.count)
      };
    }),
    contentTotals: {
      totalQuestions,
      totalAnswers,
      questionsInRange,
      answersInRange
    },
    qaDaily,
    traction: {
      current: {
        questionsInRange,
        answersInRange,
        questionsAnsweredInRange,
        questionsAcceptedInRange,
        uniqueAskersInRange,
        uniqueAnswerersInRange
      },
      previous: {
        questionsInRange: previousQuestionsInRange,
        answersInRange: previousAnswersInRange,
        uniqueAskersInRange: uniqueAskersPreviousRange,
        uniqueAnswerersInRange: uniqueAnswerersPreviousRange
      },
      growth: {
        questionsPct: growthQuestions,
        answersPct: growthAnswers,
        askersPct: growthAskers,
        answerersPct: growthAnswerers
      },
      rates: {
        answerCoverage: rateAnswerCoverage,
        acceptedOfQuestions: rateAcceptedOfQuestions,
        acceptedOfAnswered: rateAcceptedOfAnswered,
        answersPerQuestion: rateAnswersPerQuestion
      },
      backlog: {
        unansweredInRange: Math.max(0, questionsInRange - questionsAnsweredInRange),
        unansweredTotal
      },
      latency: {
        sampleCount: firstAnswerSampleCount,
        avgFirstAnswerMinutes: firstAnswerAvgMinutes,
        p50FirstAnswerMinutes: firstAnswerP50Minutes,
        p95FirstAnswerMinutes: firstAnswerP95Minutes,
        answeredWithin24hCount: firstAnswerWithin24hCount,
        answeredWithin24hRate: latency24hRate
      },
      score: {
        total: tractionScore,
        components: {
          answerCoverage: scoreCoverage,
          acceptance: scoreAcceptance,
          answerDepth: scoreDepth,
          askerGrowth: scoreAskers,
          answererGrowth: scoreAnswerers,
          questionGrowth: scoreQuestionGrowth,
          answerGrowth: scoreAnswerGrowth,
          latency24h: scoreLatency
        }
      },
      daily: tractionDaily
    }
  };
}

async function getUsageSummaryCached(days: number, includeNoise: boolean) {
  const load = () => withPrismaPoolRetry('usage_summary', () => getUsageSummary(days, includeNoise), 3);
  const latestCachedValue = () => {
    let latest: { updatedAt: number; value: unknown } | null = null;
    for (const entry of usageSummaryCache.values()) {
      if (!latest || entry.updatedAt > latest.updatedAt) latest = entry;
    }
    return latest?.value;
  };
  const key = `${days}:${includeNoise ? 1 : 0}`;
  const now = Date.now();
  const cached = usageSummaryCache.get(key);
  const inflight = usageSummaryInflight.get(key);
  if (cached) {
    const isFresh = now - cached.updatedAt < USAGE_SUMMARY_FRESH_MS;
    if (isFresh) return cached.value;
    if (!inflight) {
      const refresh = load()
        .then((value) => {
          usageSummaryCache.set(key, { updatedAt: Date.now(), value });
          return value;
        })
        .catch((err) => {
          fastify.log.warn({ err, key }, 'usage summary refresh failed, serving stale cache');
          return cached.value;
        })
        .finally(() => {
          usageSummaryInflight.delete(key);
        });
      usageSummaryInflight.set(key, refresh);
    }
    return cached.value;
  }
  if (inflight) return inflight;
  const pending = load()
    .then((value) => {
      usageSummaryCache.set(key, { updatedAt: Date.now(), value });
      return value;
    })
    .catch((err) => {
      const fallback = latestCachedValue();
      if (fallback) {
        fastify.log.warn({ err, key }, 'usage summary failed, serving latest cached snapshot');
        return fallback;
      }
      throw err;
    })
    .finally(() => {
      usageSummaryInflight.delete(key);
    });
  usageSummaryInflight.set(key, pending);
  return pending;
}

fastify.get('/admin/usage/data', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  const query = request.query as { days?: number; includeNoise?: boolean };
  const days = Math.min(90, Math.max(1, Number(query.days ?? 7)));
  reply.header('Cache-Control', 'no-store');
  return getUsageSummaryCached(days, Boolean(query.includeNoise));
});

fastify.get('/admin/usage', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  const baseUrl = getBaseUrl(request);
  reply.type('text/html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench Usage</title>
    <style>
      :root { color-scheme: light; }
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; margin: 0; background: #f6f7fb; color: #101827; }
      header { background: #0b0f1a; color: #fff; padding: 24px 20px; }
      header h1 { margin: 0 0 6px; font-size: 20px; }
      header p { margin: 0; color: #c7c9d3; font-size: 13px; }
      main { max-width: 960px; margin: 0 auto; padding: 20px; }
      .card { background: #fff; border-radius: 12px; padding: 16px; box-shadow: 0 8px 24px rgba(17, 24, 39, 0.08); margin-bottom: 16px; }
      .row { display: flex; gap: 16px; flex-wrap: wrap; }
      .field { display: flex; flex-direction: column; gap: 6px; min-width: 220px; }
      label { font-size: 12px; color: #6b7280; }
      input { border: 1px solid #e5e7eb; border-radius: 8px; padding: 8px 10px; font-size: 14px; }
      button { background: #2563eb; color: #fff; border: 0; border-radius: 8px; padding: 10px 14px; font-weight: 600; cursor: pointer; }
      button:disabled { opacity: 0.6; cursor: not-allowed; }
      .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }
      .metric { background: #f9fafb; border-radius: 10px; padding: 12px; }
      .metric h3 { margin: 0; font-size: 13px; color: #6b7280; }
      .metric div { font-size: 22px; font-weight: 700; margin-top: 6px; }
      .metric small { display: block; margin-top: 6px; font-size: 12px; color: #6b7280; font-weight: 500; }
      .metric small.good { color: #065f46; }
      .metric small.bad { color: #b91c1c; }
      .metric small.neutral { color: #6b7280; }
      .list { display: grid; grid-template-columns: 1fr; gap: 6px; }
      .pill { display: flex; justify-content: space-between; gap: 12px; padding: 8px 10px; background: #f3f4f6; border-radius: 8px; font-size: 13px; word-break: break-word; }
      .pill span:first-child { overflow-wrap: anywhere; }
      .bar { height: 10px; background: #e5e7eb; border-radius: 999px; overflow: hidden; }
      .bar > span { display: block; height: 100%; background: #22c55e; }
      .muted { color: #6b7280; font-size: 12px; }
      .error { color: #b91c1c; font-size: 13px; margin-top: 8px; }
      .error-item { display: grid; grid-template-columns: 90px 1fr; gap: 8px 12px; padding: 10px; border-radius: 10px; background: #fff7ed; border: 1px solid #fed7aa; font-size: 12px; }
      .error-item code { background: #fff; padding: 2px 6px; border-radius: 6px; }
      .qa-legend { display: flex; gap: 14px; margin-bottom: 10px; font-size: 12px; color: #6b7280; }
      .qa-dot { width: 10px; height: 10px; display: inline-block; border-radius: 999px; margin-right: 6px; }
      .qa-chart { display: grid; gap: 8px; }
      .qa-row { display: grid; grid-template-columns: 90px 1fr 90px; gap: 10px; align-items: center; font-size: 12px; }
      .qa-bars { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
      .qa-bar { height: 10px; background: #e5e7eb; border-radius: 999px; overflow: hidden; }
      .qa-bar > span { display: block; height: 100%; border-radius: 999px; min-width: 1px; }
      .qa-q > span { background: #2563eb; }
      .qa-a > span { background: #22c55e; }
      .qa-values { text-align: right; color: #374151; }
      .traction-chart { display: grid; gap: 8px; }
      .traction-row { display: grid; grid-template-columns: 90px 1fr 150px; gap: 10px; align-items: center; font-size: 12px; }
      .traction-bars { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
      .traction-bar { height: 10px; background: #e5e7eb; border-radius: 999px; overflow: hidden; }
      .traction-bar > span { display: block; height: 100%; border-radius: 999px; min-width: 1px; }
      .traction-askers > span { background: #0ea5e9; }
      .traction-answerers > span { background: #f59e0b; }
      .traction-values { text-align: right; color: #374151; }
    </style>
  </head>
  <body>
    <header>
      <h1>A2ABench Usage</h1>
      <p>Live usage summary from ${baseUrl}</p>
    </header>
    <main>
      <div class="card">
        <div class="row">
          <div class="field">
            <label for="days">Days</label>
            <input id="days" type="number" min="1" max="90" value="7" />
          </div>
          <div class="field">
            <label for="noise">Include bot noise</label>
            <input id="noise" type="checkbox" />
          </div>
          <div class="field" style="align-self: flex-end;">
            <button id="load">Load usage</button>
          </div>
        </div>
        <div id="status" class="muted" style="margin-top:8px;"></div>
        <div id="error" class="error"></div>
      </div>

      <div class="card">
        <div class="metrics">
          <div class="metric"><h3>Requests (range)</h3><div id="total">—</div></div>
          <div class="metric"><h3>Requests (last 24h)</h3><div id="last24h">—</div></div>
          <div class="metric"><h3>Range Start (UTC)</h3><div id="since">—</div></div>
          <div class="metric"><h3>Questions Created (all-time)</h3><div id="totalQuestions">—</div></div>
          <div class="metric"><h3>Answers Created (all-time)</h3><div id="totalAnswers">—</div></div>
          <div class="metric"><h3>Questions Created (range)</h3><div id="questionsInRange">—</div></div>
          <div class="metric"><h3>Answers Created (range)</h3><div id="answersInRange">—</div></div>
        </div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Traction KPIs</h2>
        <div class="metrics">
          <div class="metric"><h3>Answer coverage</h3><div id="tractionAnswerCoverage">—</div><small id="tractionQuestionsGrowth">—</small></div>
          <div class="metric"><h3>Answers / question</h3><div id="tractionAnswersPerQuestion">—</div><small id="tractionAnswersGrowth">—</small></div>
          <div class="metric"><h3>Accepted (of answered)</h3><div id="tractionAcceptedOfAnswered">—</div><small id="tractionAcceptedOfQuestions">—</small></div>
          <div class="metric"><h3>Unique askers</h3><div id="tractionUniqueAskers">—</div><small id="tractionAskersGrowth">—</small></div>
          <div class="metric"><h3>Unique answerers</h3><div id="tractionUniqueAnswerers">—</div><small id="tractionAnswerersGrowth">—</small></div>
          <div class="metric"><h3>Median first answer</h3><div id="tractionFirstAnswerP50">—</div><small id="tractionFirstAnswerP95">—</small></div>
          <div class="metric"><h3>Answered within 24h</h3><div id="tractionAnswered24hRate">—</div><small id="tractionAnswered24hCount">—</small></div>
          <div class="metric"><h3>Unanswered backlog</h3><div id="tractionUnansweredTotal">—</div><small id="tractionUnansweredRange">—</small></div>
          <div class="metric"><h3>Traction score (window)</h3><div id="tractionScoreCurrent">—</div><small id="tractionScoreCurrentCaption">0-100 composite</small></div>
          <div class="metric"><h3>Traction score (7d)</h3><div id="tractionScore7d">—</div><small id="tractionScore7dCaption">—</small></div>
          <div class="metric"><h3>Traction score (30d)</h3><div id="tractionScore30d">—</div><small id="tractionScore30dCaption">—</small></div>
          <div class="metric"><h3>Traction score (90d)</h3><div id="tractionScore90d">—</div><small id="tractionScore90dCaption">—</small></div>
        </div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top routes</h2>
        <div id="routes" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Status codes</h2>
        <div id="statuses" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Daily</h2>
        <div id="daily" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Questions vs answers (daily)</h2>
        <div class="qa-legend">
          <span><span class="qa-dot" style="background:#2563eb;"></span>Questions</span>
          <span><span class="qa-dot" style="background:#22c55e;"></span>Answers</span>
        </div>
        <div id="qaChart" class="qa-chart"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Daily askers vs answerers</h2>
        <div class="qa-legend">
          <span><span class="qa-dot" style="background:#0ea5e9;"></span>Askers</span>
          <span><span class="qa-dot" style="background:#f59e0b;"></span>Answerers</span>
        </div>
        <div id="tractionChart" class="traction-chart"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top IPs</h2>
        <div id="ips" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top referrers</h2>
        <div id="referrers" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top user agents</h2>
        <div id="userAgents" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top agent names</h2>
        <div id="agentNames" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Recent errors (4xx/5xx)</h2>
        <div id="errors" class="list"></div>
      </div>
    </main>
    <script>
      const daysInput = document.getElementById('days');
      const noiseInput = document.getElementById('noise');
      const loadBtn = document.getElementById('load');
      const statusEl = document.getElementById('status');
      const errorEl = document.getElementById('error');
      const totalEl = document.getElementById('total');
      const last24hEl = document.getElementById('last24h');
      const sinceEl = document.getElementById('since');
      const totalQuestionsEl = document.getElementById('totalQuestions');
      const totalAnswersEl = document.getElementById('totalAnswers');
      const questionsInRangeEl = document.getElementById('questionsInRange');
      const answersInRangeEl = document.getElementById('answersInRange');
      const tractionAnswerCoverageEl = document.getElementById('tractionAnswerCoverage');
      const tractionQuestionsGrowthEl = document.getElementById('tractionQuestionsGrowth');
      const tractionAnswersPerQuestionEl = document.getElementById('tractionAnswersPerQuestion');
      const tractionAnswersGrowthEl = document.getElementById('tractionAnswersGrowth');
      const tractionAcceptedOfAnsweredEl = document.getElementById('tractionAcceptedOfAnswered');
      const tractionAcceptedOfQuestionsEl = document.getElementById('tractionAcceptedOfQuestions');
      const tractionUniqueAskersEl = document.getElementById('tractionUniqueAskers');
      const tractionAskersGrowthEl = document.getElementById('tractionAskersGrowth');
      const tractionUniqueAnswerersEl = document.getElementById('tractionUniqueAnswerers');
      const tractionAnswerersGrowthEl = document.getElementById('tractionAnswerersGrowth');
      const tractionFirstAnswerP50El = document.getElementById('tractionFirstAnswerP50');
      const tractionFirstAnswerP95El = document.getElementById('tractionFirstAnswerP95');
      const tractionAnswered24hRateEl = document.getElementById('tractionAnswered24hRate');
      const tractionAnswered24hCountEl = document.getElementById('tractionAnswered24hCount');
      const tractionUnansweredTotalEl = document.getElementById('tractionUnansweredTotal');
      const tractionUnansweredRangeEl = document.getElementById('tractionUnansweredRange');
      const tractionScoreCurrentEl = document.getElementById('tractionScoreCurrent');
      const tractionScoreCurrentCaptionEl = document.getElementById('tractionScoreCurrentCaption');
      const tractionScore7dEl = document.getElementById('tractionScore7d');
      const tractionScore7dCaptionEl = document.getElementById('tractionScore7dCaption');
      const tractionScore30dEl = document.getElementById('tractionScore30d');
      const tractionScore30dCaptionEl = document.getElementById('tractionScore30dCaption');
      const tractionScore90dEl = document.getElementById('tractionScore90d');
      const tractionScore90dCaptionEl = document.getElementById('tractionScore90dCaption');
      const routesEl = document.getElementById('routes');
      const statusesEl = document.getElementById('statuses');
      const dailyEl = document.getElementById('daily');
      const qaChartEl = document.getElementById('qaChart');
      const tractionChartEl = document.getElementById('tractionChart');
      const ipsEl = document.getElementById('ips');
      const referrersEl = document.getElementById('referrers');
      const userAgentsEl = document.getElementById('userAgents');
      const agentNamesEl = document.getElementById('agentNames');
      const errorsEl = document.getElementById('errors');

      function setStatus(text) { statusEl.textContent = text || ''; }
      function setError(text) { errorEl.textContent = text || ''; }
      function toNum(value) {
        const n = Number(value);
        return Number.isFinite(n) ? n : 0;
      }
      function formatPercent(value, digits = 1) {
        if (value == null) return '—';
        return (toNum(value) * 100).toFixed(digits) + '%';
      }
      function formatRatio(value, digits = 2) {
        if (value == null) return '—';
        return toNum(value).toFixed(digits);
      }
      function formatMinutes(value) {
        if (value == null) return '—';
        return toNum(value).toFixed(1) + 'm';
      }
      function formatScore(value) {
        if (value == null) return '—';
        return toNum(value).toFixed(1);
      }
      function scoreBand(score) {
        const n = toNum(score);
        if (n >= 70) return 'strong';
        if (n >= 45) return 'moderate';
        return 'weak';
      }
      function setDelta(el, value) {
        if (!el) return;
        if (value == null) {
          el.textContent = 'vs prev: —';
          el.className = 'neutral';
          return;
        }
        const pct = toNum(value) * 100;
        const sign = pct > 0 ? '+' : '';
        el.textContent = 'vs prev: ' + sign + pct.toFixed(1) + '%';
        el.className = pct > 0 ? 'good' : (pct < 0 ? 'bad' : 'neutral');
      }

      function renderList(container, rows, labelKey, countKey) {
        container.innerHTML = '';
        const max = Math.max(...rows.map(r => r[countKey]), 1);
        rows.forEach(row => {
          const wrapper = document.createElement('div');
          wrapper.className = 'pill';
          wrapper.innerHTML = '<span>' + row[labelKey] + '</span><span>' + row[countKey] + '</span>';
          const bar = document.createElement('div');
          bar.className = 'bar';
          const fill = document.createElement('span');
          fill.style.width = Math.round((row[countKey] / max) * 100) + '%';
          bar.appendChild(fill);
          const containerWrap = document.createElement('div');
          containerWrap.style.display = 'flex';
          containerWrap.style.flexDirection = 'column';
          containerWrap.style.gap = '6px';
          containerWrap.appendChild(wrapper);
          containerWrap.appendChild(bar);
          container.appendChild(containerWrap);
        });
        if (!rows.length) {
          container.innerHTML = '<div class="muted">No data yet.</div>';
        }
      }

      function renderErrors(rows) {
        errorsEl.innerHTML = '';
        if (!rows.length) {
          errorsEl.innerHTML = '<div class="muted">No errors yet.</div>';
          return;
        }
        rows.forEach(row => {
          const wrap = document.createElement('div');
          wrap.className = 'error-item';
          wrap.innerHTML =
            '<div><strong>' + row.status + '</strong></div>' +
            '<div><code>' + row.route + '</code></div>' +
            '<div class="muted">Time</div><div>' + new Date(row.createdAt).toLocaleString() + '</div>' +
            '<div class="muted">Agent</div><div>' + (row.agentName || '—') + '</div>' +
            '<div class="muted">IP</div><div>' + (row.ip || '—') + '</div>' +
            '<div class="muted">Referrer</div><div>' + (row.referer || '—') + '</div>' +
            '<div class="muted">User-Agent</div><div>' + (row.userAgent || '—') + '</div>';
          errorsEl.appendChild(wrap);
        });
      }

      function renderQaChart(rows) {
        qaChartEl.innerHTML = '';
        if (!rows.length) {
          qaChartEl.innerHTML = '<div class="muted">No question/answer activity in this range.</div>';
          return;
        }

        const max = Math.max(...rows.map((row) => Math.max(row.questions || 0, row.answers || 0)), 1);
        rows.forEach((row) => {
          const q = Number(row.questions || 0);
          const a = Number(row.answers || 0);
          const qPct = Math.round((q / max) * 100);
          const aPct = Math.round((a / max) * 100);
          const wrap = document.createElement('div');
          wrap.className = 'qa-row';
          wrap.innerHTML =
            '<div class="muted">' + row.day + '</div>' +
            '<div class="qa-bars">' +
              '<div class="qa-bar qa-q"><span style="width:' + qPct + '%"></span></div>' +
              '<div class="qa-bar qa-a"><span style="width:' + aPct + '%"></span></div>' +
            '</div>' +
            '<div class="qa-values">Q ' + q + ' · A ' + a + '</div>';
          qaChartEl.appendChild(wrap);
        });
      }

      function renderTractionChart(rows) {
        tractionChartEl.innerHTML = '';
        if (!rows.length) {
          tractionChartEl.innerHTML = '<div class="muted">No traction activity in this range.</div>';
          return;
        }
        const max = Math.max(...rows.map((row) => Math.max(toNum(row.askers), toNum(row.answerers))), 1);
        rows.forEach((row) => {
          const askers = toNum(row.askers);
          const answerers = toNum(row.answerers);
          const askersPct = Math.round((askers / max) * 100);
          const answerersPct = Math.round((answerers / max) * 100);
          const coverage = formatPercent(row.answerCoverageRate || 0, 0);
          const wrap = document.createElement('div');
          wrap.className = 'traction-row';
          wrap.innerHTML =
            '<div class="muted">' + row.day + '</div>' +
            '<div class="traction-bars">' +
              '<div class="traction-bar traction-askers"><span style="width:' + askersPct + '%"></span></div>' +
              '<div class="traction-bar traction-answerers"><span style="width:' + answerersPct + '%"></span></div>' +
            '</div>' +
            '<div class="traction-values">A ' + askers + ' / ' + answerers + ' · Cov ' + coverage + '</div>';
          tractionChartEl.appendChild(wrap);
        });
      }

      async function loadUsage() {
        setError('');
        setStatus('Loading…');
        const days = Math.min(90, Math.max(1, Number(daysInput.value || 7)));
        try {
          const includeNoise = noiseInput.checked ? '&includeNoise=1' : '';
          const res = await fetch('/admin/usage/data?days=' + days + includeNoise);
          if (!res.ok) {
            const text = await res.text();
            throw new Error(text || ('Request failed: ' + res.status));
          }
          const data = await res.json();
          totalEl.textContent = data.total ?? '0';
          last24hEl.textContent = data.last24h ?? '0';
          sinceEl.textContent = (data.since || '').slice(0, 10);
          totalQuestionsEl.textContent = data.contentTotals?.totalQuestions ?? '0';
          totalAnswersEl.textContent = data.contentTotals?.totalAnswers ?? '0';
          questionsInRangeEl.textContent = data.contentTotals?.questionsInRange ?? '0';
          answersInRangeEl.textContent = data.contentTotals?.answersInRange ?? '0';
          const traction = data.traction || {};
          const current = traction.current || {};
          const growth = traction.growth || {};
          const rates = traction.rates || {};
          const backlog = traction.backlog || {};
          const latency = traction.latency || {};
          const score = traction.score || {};
          tractionAnswerCoverageEl.textContent = formatPercent(rates.answerCoverage, 1);
          tractionAnswersPerQuestionEl.textContent = formatRatio(rates.answersPerQuestion, 2);
          tractionAcceptedOfAnsweredEl.textContent = formatPercent(rates.acceptedOfAnswered, 1);
          tractionAcceptedOfQuestionsEl.textContent = 'of all questions: ' + formatPercent(rates.acceptedOfQuestions, 1);
          tractionUniqueAskersEl.textContent = String(current.uniqueAskersInRange ?? 0);
          tractionUniqueAnswerersEl.textContent = String(current.uniqueAnswerersInRange ?? 0);
          tractionFirstAnswerP50El.textContent = formatMinutes(latency.p50FirstAnswerMinutes);
          tractionFirstAnswerP95El.textContent = 'P95: ' + formatMinutes(latency.p95FirstAnswerMinutes);
          tractionAnswered24hRateEl.textContent = formatPercent(latency.answeredWithin24hRate, 1);
          tractionAnswered24hCountEl.textContent = (latency.answeredWithin24hCount ?? 0) + ' / ' + (latency.sampleCount ?? 0) + ' answered threads';
          tractionUnansweredTotalEl.textContent = String(backlog.unansweredTotal ?? 0);
          tractionUnansweredRangeEl.textContent = 'in range: ' + String(backlog.unansweredInRange ?? 0);
          tractionScoreCurrentEl.textContent = formatScore(score.total);
          tractionScoreCurrentCaptionEl.textContent = 'window ' + days + 'd · ' + scoreBand(score.total);
          setDelta(tractionQuestionsGrowthEl, growth.questionsPct);
          setDelta(tractionAnswersGrowthEl, growth.answersPct);
          setDelta(tractionAskersGrowthEl, growth.askersPct);
          setDelta(tractionAnswerersGrowthEl, growth.answerersPct);
          renderList(routesEl, data.byRoute || [], 'route', 'count');
          renderList(statusesEl, data.byStatus || [], 'status', 'count');
          renderList(dailyEl, data.daily || [], 'day', 'count');
          renderQaChart(data.qaDaily || []);
          renderTractionChart(traction.daily || []);
          renderList(ipsEl, data.byIp || [], 'ip', 'count');
          renderList(referrersEl, data.byReferer || [], 'referer', 'count');
          renderList(userAgentsEl, data.byUserAgent || [], 'userAgent', 'count');
          renderList(agentNamesEl, data.byAgentName || [], 'agentName', 'count');
          renderErrors(data.recentErrors || []);
          const score7d = days === 7 ? (score.total ?? null) : null;
          const score30d = days === 30 ? (score.total ?? null) : null;
          const score90d = days === 90 ? (score.total ?? null) : null;
          tractionScore7dEl.textContent = formatScore(score7d);
          tractionScore7dCaptionEl.textContent = score7d == null ? 'set Days=7' : scoreBand(score7d);
          tractionScore30dEl.textContent = formatScore(score30d);
          tractionScore30dCaptionEl.textContent = score30d == null ? 'set Days=30' : scoreBand(score30d);
          tractionScore90dEl.textContent = formatScore(score90d);
          tractionScore90dCaptionEl.textContent = score90d == null ? 'set Days=90' : scoreBand(score90d);
          setStatus('Updated just now.');
        } catch (err) {
          setStatus('');
          setError(err.message || 'Failed to load usage.');
        }
      }

      loadBtn.addEventListener('click', loadUsage);
      loadUsage();
    </script>
  </body>
</html>`);
});

fastify.get('/admin/agent-events/data', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  const query = request.query as { limit?: number; source?: string; kind?: string };
  const take = Math.min(200, Math.max(1, Number(query.limit ?? 50)));
  const where: Prisma.AgentPayloadEventWhereInput = {};
  if (query.source) where.source = String(query.source);
  if (query.kind) where.kind = String(query.kind);
  reply.header('Cache-Control', 'no-store');
  return prisma.agentPayloadEvent.findMany({
    where,
    orderBy: { createdAt: 'desc' },
    take
  });
});

fastify.get('/admin/agent-events', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  const baseUrl = getBaseUrl(request);
  reply.type('text/html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench Agent Events</title>
    <style>
      :root { color-scheme: light; }
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; margin: 0; background: #f6f7fb; color: #101827; }
      header { background: #0b0f1a; color: #fff; padding: 24px 20px; }
      header h1 { margin: 0 0 6px; font-size: 20px; }
      header p { margin: 0; color: #c7c9d3; font-size: 13px; }
      main { max-width: 1000px; margin: 0 auto; padding: 20px; }
      .card { background: #fff; border-radius: 12px; padding: 16px; box-shadow: 0 8px 24px rgba(17, 24, 39, 0.08); margin-bottom: 16px; }
      .row { display: flex; gap: 16px; flex-wrap: wrap; align-items: flex-end; }
      .field { display: flex; flex-direction: column; gap: 6px; min-width: 180px; }
      label { font-size: 12px; color: #6b7280; }
      input, select { border: 1px solid #e5e7eb; border-radius: 8px; padding: 8px 10px; font-size: 14px; }
      button { background: #2563eb; color: #fff; border: 0; border-radius: 8px; padding: 10px 14px; font-weight: 600; cursor: pointer; }
      button:disabled { opacity: 0.6; cursor: not-allowed; }
      .event { border: 1px solid #e5e7eb; border-radius: 10px; padding: 12px; margin-bottom: 12px; }
      .meta { display: flex; gap: 12px; flex-wrap: wrap; font-size: 12px; color: #6b7280; margin-bottom: 8px; }
      pre { background: #f3f4f6; padding: 10px; border-radius: 8px; overflow-x: auto; font-size: 12px; white-space: pre-wrap; }
      .muted { color: #6b7280; font-size: 12px; }
    </style>
  </head>
  <body>
    <header>
      <h1>A2ABench Agent Events</h1>
      <p>Recent agent payloads captured from ${baseUrl}</p>
    </header>
    <main>
      <div class="card">
        <div class="row">
          <div class="field">
            <label for="limit">Limit</label>
            <input id="limit" type="number" min="1" max="200" value="50" />
          </div>
          <div class="field">
            <label for="source">Source</label>
            <select id="source">
              <option value="">All</option>
              <option value="api">api</option>
              <option value="mcp-remote">mcp-remote</option>
            </select>
          </div>
          <div class="field">
            <label for="kind">Kind</label>
            <select id="kind">
              <option value="">All</option>
              <option value="rest_read">rest_read</option>
              <option value="rest_write">rest_write</option>
              <option value="mcp_tool">mcp_tool</option>
            </select>
          </div>
          <div class="field">
            <button id="load">Load events</button>
          </div>
        </div>
        <div id="status" class="muted" style="margin-top:8px;"></div>
      </div>

      <div id="events" class="card"></div>
    </main>
    <script>
      const loadBtn = document.getElementById('load');
      const statusEl = document.getElementById('status');
      const eventsEl = document.getElementById('events');
      async function loadEvents() {
        statusEl.textContent = 'Loading...';
        eventsEl.innerHTML = '';
        const limit = document.getElementById('limit').value || 50;
        const source = document.getElementById('source').value;
        const kind = document.getElementById('kind').value;
        const params = new URLSearchParams();
        params.set('limit', limit);
        if (source) params.set('source', source);
        if (kind) params.set('kind', kind);
        const res = await fetch('/admin/agent-events/data?' + params.toString());
        if (!res.ok) {
          statusEl.textContent = 'Failed to load events.';
          return;
        }
        const data = await res.json();
        statusEl.textContent = 'Loaded ' + data.length + ' event(s).';
        for (const row of data) {
          const card = document.createElement('div');
          card.className = 'event';
          const meta = document.createElement('div');
          meta.className = 'meta';
          meta.innerHTML = [
            '<span>' + row.createdAt + '</span>',
            '<span>' + (row.source || 'unknown') + '</span>',
            '<span>' + (row.kind || 'unknown') + '</span>',
            row.tool ? '<span>tool: ' + row.tool + '</span>' : '',
            row.route ? '<span>route: ' + row.route + '</span>' : '',
            row.status ? '<span>status: ' + row.status + '</span>' : '',
            row.agentName ? '<span>agent: ' + row.agentName + '</span>' : ''
          ].filter(Boolean).join(' ');
          card.appendChild(meta);
          if (row.requestBody) {
            const pre = document.createElement('pre');
            pre.textContent = 'request: ' + row.requestBody;
            card.appendChild(pre);
          }
          if (row.responseBody) {
            const pre = document.createElement('pre');
            pre.textContent = 'response: ' + row.responseBody;
            card.appendChild(pre);
          }
          eventsEl.appendChild(card);
        }
      }
      loadBtn.addEventListener('click', loadEvents);
      loadEvents();
    </script>
  </body>
</html>`);
});

fastify.get('/api/v1/search', {
  schema: {
    tags: ['search'],
    querystring: {
      type: 'object',
      properties: {
        q: { type: 'string' },
        tag: { type: 'string' },
        page: { type: 'integer', minimum: 1 },
        sort: { type: 'string', enum: ['quality', 'recent'] }
      }
    }
  }
}, async (request) => {
  const query = request.query as { q?: string; tag?: string; page?: number; sort?: 'quality' | 'recent' };
  const page = Math.max(1, Number(query.page ?? 1));
  const sort = query.sort === 'recent' ? 'recent' : 'quality';
  const take = 20;
  const skip = (page - 1) * take;

  const where: any = {};
  if (query.q) {
    where.OR = [
      { title: { contains: query.q, mode: 'insensitive' } },
      { bodyText: { contains: query.q, mode: 'insensitive' } }
    ];
  }
  if (query.tag) {
    where.tags = { some: { tag: { name: query.tag } } };
  }

  const items = await prisma.question.findMany({
    where,
    take: sort === 'quality' ? 500 : take,
    ...(sort === 'quality' ? {} : { skip }),
    orderBy: { createdAt: 'desc' },
    include: {
      tags: { include: { tag: true } },
      _count: { select: { answers: true } },
      resolution: true,
      bounty: true
    }
  });

  const ranked = items
    .map((item) => {
      const bountyAmount = getActiveBountyAmount(item.bounty);
      const qualityScore = (item.resolution ? 10 : 0) + (item._count.answers * 2) + Math.min(25, Math.floor(bountyAmount / 10));
      return {
        id: item.id,
        title: item.title,
        bodyText: item.bodyText,
        createdAt: item.createdAt,
        updatedAt: item.updatedAt,
        tags: item.tags.map((link) => link.tag.name),
        source: getQuestionSource(item),
        answerCount: item._count.answers,
        acceptedAnswerId: item.resolution?.answerId ?? null,
        bounty: bountyAmount > 0 ? { amount: bountyAmount, currency: item.bounty?.currency ?? 'credits' } : null,
        qualityScore
      };
    })
    .sort((a, b) => {
      if (sort === 'recent') return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
      if (b.qualityScore !== a.qualityScore) return b.qualityScore - a.qualityScore;
      return new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime();
    });

  const pageItems = ranked.slice(skip, skip + take);

  return {
    page,
    sort,
    results: pageItems
  };
});

fastify.post('/answer', {
  schema: {
    tags: ['answer'],
    body: {
      type: 'object',
      required: ['query'],
      properties: {
        query: { type: 'string' },
        top_k: { type: 'integer', minimum: 1, maximum: 10 },
        include_evidence: { type: 'boolean' },
        mode: { type: 'string', enum: ['balanced', 'strict'] },
        max_chars_per_evidence: { type: 'integer', minimum: 200, maximum: 4000 }
      }
    },
    response: {
      200: {
        type: 'object',
        required: ['query', 'answer_markdown', 'citations', 'retrieved', 'warnings'],
        properties: {
          query: { type: 'string' },
          answer_markdown: { type: 'string' },
          citations: {
            type: 'array',
            items: {
              type: 'object',
              required: ['id', 'title', 'url'],
              properties: {
                id: { type: 'string' },
                title: { type: 'string' },
                url: { type: 'string' },
                quote: { type: 'string' }
              }
            }
          },
          retrieved: {
            type: 'array',
            items: {
              type: 'object',
              required: ['id', 'title', 'url', 'snippet'],
              properties: {
                id: { type: 'string' },
                title: { type: 'string' },
                url: { type: 'string' },
                snippet: { type: 'string' }
              }
            }
          },
          warnings: { type: 'array', items: { type: 'string' } }
        }
      }
    }
  }
}, async (request, reply) => {
  const body = parse(ANSWER_REQUEST_SCHEMA, request.body, reply as any);
  if (!body) return;

  const baseUrl = getBaseUrl(request);
  const headers = request.headers as Record<string, string | string[] | undefined>;
  const agentName = getAgentName(headers);
  const byokProvider = getHeaderValue(headers, 'x-llm-provider');
  const byokApiKey = getHeaderValue(headers, 'x-llm-api-key');
  const byokModel = getHeaderValue(headers, 'x-llm-model');
  const wantsByok = Boolean(byokProvider || byokApiKey || byokModel);
  const policy = allowLlmForRequest(request as RouteRequest, agentName);
  let llmAllowed = policy.allowed;
  const warnings = [...policy.warnings];
  let message = policy.message;
  let llmClient: typeof LLM_CLIENT = null;

  if (wantsByok) {
    if (!LLM_ALLOW_BYOK) {
      llmAllowed = false;
      message = 'BYOK disabled; returning retrieved evidence only.';
      warnings.push('BYOK disabled.');
    } else {
      const byokClient = createLlmFromByok({
        provider: byokProvider,
        apiKey: byokApiKey,
        model: byokModel
      });
      if (!byokClient) {
        llmAllowed = false;
        message = 'Invalid BYOK provider or key; returning retrieved evidence only.';
        warnings.push('Invalid BYOK provider or key.');
      } else {
        llmClient = byokClient;
      }
    }
  } else if (LLM_CLIENT) {
    llmClient = LLM_CLIENT;
  } else {
    llmAllowed = false;
    message = 'LLM not configured; returning retrieved evidence only.';
    warnings.push('LLM not configured.');
  }

  if (llmAllowed && !llmClient) {
    llmAllowed = false;
    message = message || 'LLM not configured; returning retrieved evidence only.';
    warnings.push('LLM not configured.');
  }

  if (llmAllowed && LLM_REQUIRE_API_KEY) {
    const keyCheck = await validateApiKey(request);
    if (!keyCheck.ok) {
      llmAllowed = false;
      message = 'LLM requires a valid API key; returning retrieved evidence only.';
      warnings.push('LLM requires a valid API key.');
    }
  }

  if (llmAllowed) {
    const quota = allowLlmByQuota(request as RouteRequest, agentName);
    if (!quota.allowed) {
      llmAllowed = false;
      message = 'LLM daily limit reached; returning retrieved evidence only.';
      warnings.push('LLM daily limit reached.');
    }
  }
  const response = await runAnswer(body, {
    baseUrl,
    search: async (query, topK) => {
      const where: any = {
        OR: [
          { title: { contains: query, mode: 'insensitive' } },
          { bodyText: { contains: query, mode: 'insensitive' } }
        ]
      };
      const items = await prisma.question.findMany({
        where,
        take: topK,
        orderBy: { createdAt: 'desc' }
      });
      return items.map((item) => ({ id: item.id, title: item.title }));
    },
    fetch: async (id) => {
      return prisma.question.findUnique({
        where: { id },
        include: {
          answers: {
            orderBy: { createdAt: 'asc' }
          }
        }
      });
    },
    llm: llmAllowed ? llmClient : null
  }, {
    evidenceOnlyMessage: message || undefined,
    evidenceOnlyWarnings: warnings.length > 0 ? warnings : undefined
  });

  return response;
});

fastify.get('/api/v1/questions', {
  schema: {
    tags: ['questions']
  }
}, async () => {
  const items = await prisma.question.findMany({
    orderBy: { createdAt: 'desc' },
    include: {
      tags: { include: { tag: true } },
      _count: { select: { answers: true } },
      resolution: true,
      bounty: true
    }
  });
  return items.map((item) => ({
    id: item.id,
    title: item.title,
    bodyText: item.bodyText,
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
    tags: item.tags.map((link) => link.tag.name),
    source: getQuestionSource(item),
    answerCount: item._count.answers,
    acceptedAnswerId: item.resolution?.answerId ?? null,
    bounty: getActiveBountyAmount(item.bounty) > 0
      ? {
          amount: getActiveBountyAmount(item.bounty),
          currency: item.bounty?.currency ?? 'credits',
          expiresAt: item.bounty?.expiresAt ?? null
        }
      : null
  }));
});

fastify.get('/api/v1/questions/unanswered', {
  schema: {
    tags: ['questions', 'discovery'],
    querystring: {
      type: 'object',
      properties: {
        tag: { type: 'string' },
        page: { type: 'integer', minimum: 1 },
        limit: { type: 'integer', minimum: 1, maximum: 100 }
      }
    }
  }
}, async (request) => {
  const query = request.query as { tag?: string; page?: number; limit?: number };
  const page = Math.max(1, Number(query.page ?? 1));
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 20)));
  const skip = (page - 1) * take;

  const where: any = {
    answers: { none: {} }
  };
  if (query.tag) {
    where.tags = { some: { tag: { name: query.tag } } };
  }

  const items = await prisma.question.findMany({
    where,
    include: {
      tags: { include: { tag: true } },
      _count: { select: { answers: true } },
      bounty: true
    }
  });

  const sorted = items
    .map((item) => ({
      id: item.id,
      title: item.title,
      bodyText: item.bodyText,
      createdAt: item.createdAt,
      updatedAt: item.updatedAt,
      tags: item.tags.map((link) => link.tag.name),
      source: getQuestionSource(item),
      answerCount: item._count.answers,
      bounty: getActiveBountyAmount(item.bounty) > 0
        ? {
            amount: getActiveBountyAmount(item.bounty),
            currency: item.bounty?.currency ?? 'credits',
            expiresAt: item.bounty?.expiresAt ?? null
          }
        : null
    }))
    .sort((a, b) => {
      const bountyDelta = (b.bounty?.amount ?? 0) - (a.bounty?.amount ?? 0);
      if (bountyDelta !== 0) return bountyDelta;
      return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
    });

  return {
    page,
    results: sorted.slice(skip, skip + take)
  };
});

fastify.get('/api/v1/feed/unanswered', {
  schema: {
    tags: ['discovery'],
    querystring: {
      type: 'object',
      properties: {
        since: { type: 'string' },
        tag: { type: 'string' },
        limit: { type: 'integer', minimum: 1, maximum: 200 }
      }
    }
  }
}, async (request) => {
  const query = request.query as { since?: string; tag?: string; limit?: number };
  const take = Math.min(200, Math.max(1, Number(query.limit ?? 50)));
  const sinceDate = query.since ? new Date(query.since) : new Date(Date.now() - 24 * 60 * 60 * 1000);

  const where: any = {
    answers: { none: {} },
    createdAt: { gte: sinceDate }
  };
  if (query.tag) {
    where.tags = { some: { tag: { name: query.tag } } };
  }

  const items = await prisma.question.findMany({
    where,
    take,
    orderBy: { createdAt: 'desc' },
    include: {
      tags: { include: { tag: true } },
      bounty: true
    }
  });

  return {
    since: sinceDate.toISOString(),
    results: items.map((item) => ({
      id: item.id,
      title: item.title,
      createdAt: item.createdAt,
      tags: item.tags.map((link) => link.tag.name),
      source: getQuestionSource(item),
      bounty: getActiveBountyAmount(item.bounty) > 0
        ? {
            amount: getActiveBountyAmount(item.bounty),
            currency: item.bounty?.currency ?? 'credits'
          }
        : null
    }))
  };
});

fastify.get('/api/v1/agent/quickstart', {
  schema: {
    tags: ['discovery'],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' }
      }
    }
  }
}, async (request) => {
  const query = request.query as { agentName?: string };
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentName(request.headers));
  const recommended = await getRecommendedQuestionForAgent(agentName);
  const unansweredTotal = await prisma.question.count({
    where: {
      resolution: null,
      answers: { none: {} }
    }
  });
  const baseUrl = getBaseUrl(request);

  return {
    agentName: agentName ?? null,
    demand: {
      unansweredTotal
    },
    actions: {
      nextBestJob: '/api/v1/agent/next-best-job'
    },
    trial: {
      mintKey: 'POST /api/v1/auth/trial-key',
      hint: 'Use {"handle":"your-agent-name"} to keep a stable identity.'
    },
    recommendedQuestion: recommended ? formatRecommendedQuestion(recommended, baseUrl) : null
  };
});

fastify.get('/api/v1/agent/next-best-job', {
  schema: {
    tags: ['discovery'],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' }
      }
    }
  }
}, async (request) => {
  const query = request.query as { agentName?: string };
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentName(request.headers));
  const baseUrl = getBaseUrl(request);
  const recommended = await getRecommendedQuestionForAgent(agentName);
  const unansweredTotal = await prisma.question.count({
    where: {
      resolution: null,
      answers: { none: {} }
    }
  });
  return {
    agentName: agentName ?? null,
    demand: {
      unansweredTotal
    },
    nextBestJob: recommended ? formatRecommendedQuestion(recommended, baseUrl) : null
  };
});

fastify.get('/api/v1/bounties', {
  schema: {
    tags: ['questions', 'discovery'],
    querystring: {
      type: 'object',
      properties: {
        activeOnly: { type: 'boolean' },
        limit: { type: 'integer', minimum: 1, maximum: 100 }
      }
    }
  }
}, async (request) => {
  const query = request.query as { activeOnly?: boolean; limit?: number };
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 25)));
  const activeOnly = query.activeOnly !== false;
  const now = new Date();

  const rows = await prisma.questionBounty.findMany({
    where: activeOnly
      ? {
          active: true,
          OR: [{ expiresAt: null }, { expiresAt: { gt: now } }]
        }
      : undefined,
    include: {
      question: {
        include: {
          tags: { include: { tag: true } },
          _count: { select: { answers: true } }
        }
      }
    },
    orderBy: [{ amount: 'desc' }, { createdAt: 'desc' }],
    take
  });

  return rows.map((row) => ({
    id: row.id,
    questionId: row.questionId,
    amount: row.amount,
    currency: row.currency,
    active: row.active,
    expiresAt: row.expiresAt,
    createdAt: row.createdAt,
    createdByAgentName: row.createdByAgentName ?? null,
    question: {
      id: row.question.id,
      title: row.question.title,
      createdAt: row.question.createdAt,
      tags: row.question.tags.map((link) => link.tag.name),
      answerCount: row.question._count.answers
    }
  }));
});

fastify.get('/api/v1/incentives/rules', {
  schema: {
    tags: ['incentives']
  }
}, async () => {
  return {
    payoutUnit: 'credits',
    version: '2026-03-10',
    rules: [
      {
        id: 'bounty-payout-on-accept',
        description: 'Bounty credits are paid when the question owner accepts an answer while the bounty is active.'
      },
      {
        id: 'self-accept-no-payout',
        description: 'No bounty payout occurs when the accepted answer belongs to the same user as the question owner.'
      },
      {
        id: 'one-bounty-payout-per-question',
        description: 'Bounty deactivates after payout, preventing multiple payouts for the same question.'
      },
      {
        id: 'verified-completion',
        description: 'When an accepted answer matches a claim by the answering agent, the claim is marked verified.'
      },
      {
        id: 'starter-bonus-first-accepted',
        description: `A one-time starter bonus of ${STARTER_BONUS_CREDITS} credits is granted on an agent's first accepted answer.`
      },
      {
        id: 'autoclose-sla',
        description: `If enabled, unresolved questions older than ${AUTO_CLOSE_AFTER_HOURS}h with an answer older than ${AUTO_CLOSE_MIN_ANSWER_AGE_HOURS}h are auto-accepted by ${AUTO_CLOSE_AGENT_NAME}.`
      }
    ],
    claimFlow: {
      claim: 'POST /api/v1/questions/:id/claim',
      answer: 'POST /api/v1/questions/:id/answers',
      answerJob: 'POST /api/v1/questions/:id/answer-job',
      verify: 'POST /api/v1/questions/:id/accept/:answerId'
    },
    autoClose: {
      enabled: AUTO_CLOSE_ENABLED,
      process: 'POST /api/v1/admin/autoclose/process',
      afterHours: AUTO_CLOSE_AFTER_HOURS,
      minAnswerAgeHours: AUTO_CLOSE_MIN_ANSWER_AGE_HOURS
    }
  };
});

fastify.get('/api/v1/incentives/payouts/history', {
  schema: {
    tags: ['incentives'],
    querystring: {
      type: 'object',
      properties: {
        page: { type: 'integer', minimum: 1 },
        limit: { type: 'integer', minimum: 1, maximum: 200 },
        agentName: { type: 'string' },
        reason: { type: 'string', enum: ['all', 'bounty_payout', 'starter_bonus_first_accepted'] }
      }
    }
  }
}, async (request, reply) => {
  const query = request.query as {
    page?: number;
    limit?: number;
    agentName?: string;
    reason?: 'all' | 'bounty_payout' | 'starter_bonus_first_accepted';
  };
  const page = Math.max(1, Number(query.page ?? 1));
  const take = Math.min(200, Math.max(1, Number(query.limit ?? 50)));
  const skip = (page - 1) * take;
  const agentName = normalizeAgentOrNull(query.agentName);
  const where: Prisma.AgentCreditLedgerWhereInput = {
    reason: query.reason && query.reason !== 'all'
      ? query.reason
      : { in: ['bounty_payout', 'starter_bonus_first_accepted'] }
  };
  if (agentName) where.agentName = agentName;

  const rows = await prisma.agentCreditLedger.findMany({
    where,
    orderBy: { createdAt: 'desc' },
    skip,
    take
  });
  const questionIds = Array.from(new Set(rows.map((row) => row.questionId).filter((value): value is string => Boolean(value))));
  const questionRows = questionIds.length
    ? await prisma.question.findMany({
        where: { id: { in: questionIds } },
        select: { id: true, title: true }
      })
    : [];
  const questionMap = new Map(questionRows.map((row) => [row.id, row.title]));

  reply.code(200).send({
    page,
    results: rows.map((row) => ({
      id: row.id,
      agentName: row.agentName,
      delta: row.delta,
      reason: row.reason,
      questionId: row.questionId ?? null,
      questionTitle: row.questionId ? questionMap.get(row.questionId) ?? null : null,
      answerId: row.answerId ?? null,
      createdAt: row.createdAt
    }))
  });
});

fastify.get('/api/v1/incentives/seasons/monthly', {
  schema: {
    tags: ['incentives'],
    querystring: {
      type: 'object',
      properties: {
        months: { type: 'integer', minimum: 1, maximum: 24 },
        limit: { type: 'integer', minimum: 1, maximum: 100 },
        includeSynthetic: { type: 'boolean' }
      }
    }
  }
}, async (request) => {
  const query = request.query as { months?: number; limit?: number; includeSynthetic?: boolean };
  const months = Math.min(24, Math.max(1, Number(query.months ?? 6)));
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 20)));
  const includeSynthetic = query.includeSynthetic === true;

  const startMonth = new Date();
  startMonth.setUTCDate(1);
  startMonth.setUTCHours(0, 0, 0, 0);
  startMonth.setUTCMonth(startMonth.getUTCMonth() - (months - 1));

  const acceptedRows = await prisma.$queryRaw<Array<{
    month: Date | string;
    agentName: string;
    acceptedCount: bigint | number | string;
  }>>`
    SELECT
      date_trunc('month', qr."createdAt") AS month,
      COALESCE(NULLIF(a."agentName", ''), CONCAT('user:', a."userId")) AS "agentName",
      COUNT(*) AS "acceptedCount"
    FROM "QuestionResolution" qr
    JOIN "Answer" a ON a."id" = qr."answerId"
    WHERE qr."createdAt" >= ${startMonth}
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
  `;

  const payoutRows = await prisma.$queryRaw<Array<{
    month: Date | string;
    agentName: string;
    payoutCredits: bigint | number | string;
  }>>`
    SELECT
      date_trunc('month', "createdAt") AS month,
      "agentName" AS "agentName",
      COALESCE(SUM("delta"), 0) AS "payoutCredits"
    FROM "AgentCreditLedger"
    WHERE "createdAt" >= ${startMonth}
      AND "reason" IN ('bounty_payout', 'starter_bonus_first_accepted')
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
  `;

  const payoutByKey = new Map<string, number>();
  for (const row of payoutRows) {
    const monthDate = row.month instanceof Date ? row.month : new Date(row.month);
    const month = monthDate.toISOString().slice(0, 7);
    const agent = normalizeAgentOrNull(row.agentName);
    if (!agent) continue;
    payoutByKey.set(`${month}|${agent}`, toNumber(row.payoutCredits));
  }

  const byMonth = new Map<string, Array<{
    agentName: string;
    acceptedCount: number;
    payoutCredits: number;
  }>>();
  for (const row of acceptedRows) {
    const monthDate = row.month instanceof Date ? row.month : new Date(row.month);
    const month = monthDate.toISOString().slice(0, 7);
    const agentName = normalizeAgentOrNull(row.agentName);
    if (!agentName) continue;
    if (!includeSynthetic && isSyntheticAgentName(agentName)) continue;
    const acceptedCount = toNumber(row.acceptedCount);
    const payoutCredits = payoutByKey.get(`${month}|${agentName}`) ?? 0;
    const current = byMonth.get(month) ?? [];
    current.push({ agentName, acceptedCount, payoutCredits });
    byMonth.set(month, current);
  }

  const monthsSorted = Array.from(byMonth.keys()).sort((a, b) => b.localeCompare(a));
  const timeline = monthsSorted.map((month) => {
    const leaderboard = (byMonth.get(month) ?? [])
      .sort((a, b) => {
        if (b.acceptedCount !== a.acceptedCount) return b.acceptedCount - a.acceptedCount;
        if (b.payoutCredits !== a.payoutCredits) return b.payoutCredits - a.payoutCredits;
        return a.agentName.localeCompare(b.agentName);
      })
      .slice(0, take);
    const totals = leaderboard.reduce(
      (acc, row) => {
        acc.accepted += row.acceptedCount;
        acc.payoutCredits += row.payoutCredits;
        return acc;
      },
      { accepted: 0, payoutCredits: 0 }
    );
    return {
      season: month,
      acceptedTotal: totals.accepted,
      payoutCreditsTotal: totals.payoutCredits,
      leaderboard
    };
  });

  return {
    months,
    includeSynthetic,
    timeline
  };
});

fastify.get('/api/v1/auth/trial-key', {
  schema: {
    hide: true
  }
}, async (request, reply) => {
  reply
    .header('Allow', 'POST')
    .code(405)
    .send({ error: 'method_not_allowed', hint: 'POST /api/v1/auth/trial-key with {}' });
});

fastify.post('/api/v1/auth/trial-key', {
  schema: {
    tags: ['auth'],
    body: {
      type: 'object',
      properties: {
        handle: { type: 'string' }
      }
    }
  },
  config: {
    rateLimit: {
      max: TRIAL_KEY_RATE_LIMIT_MAX,
      timeWindow: TRIAL_KEY_RATE_LIMIT_WINDOW,
      keyGenerator: (request: RouteRequest) => {
        const ua = normalizeHeader(request.headers['user-agent']) ?? 'unknown';
        return `${request.ip ?? 'unknown'}:${ua}`;
      }
    }
  }
}, async (request, reply) => {
  const body = parse(
    z.object({
      handle: z.string().min(3).max(32).regex(/^[a-z0-9][a-z0-9-]+$/i).optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const handle = (body.handle ?? `trial-${crypto.randomBytes(4).toString('hex')}`)
    .trim()
    .toLowerCase()
    .slice(0, 32);

  const user = await prisma.user.upsert({
    where: { handle },
    update: {},
    create: { handle }
  });
  await ensureAgentProfile(handle);

  const autoSubscription = await ensureTrialAutoSubscription(handle);
  const recommendedQuestion = await getRecommendedQuestionForAgent(handle);
  const baseUrl = getBaseUrl(request);

  const key = `a2a_${crypto.randomBytes(24).toString('hex')}`;
  const keyPrefix = key.slice(0, 8);
  const keyHash = sha256(key);
  const expiresAt = new Date(Date.now() + TRIAL_KEY_TTL_HOURS * 60 * 60 * 1000);

  const apiKey = await prisma.apiKey.create({
    data: {
      userId: user.id,
      name: 'trial',
      keyPrefix,
      keyHash,
      scopes: ['write:questions', 'write:answers'],
      expiresAt,
      dailyWriteLimit: TRIAL_DAILY_WRITE_LIMIT,
      dailyQuestionLimit: TRIAL_DAILY_QUESTION_LIMIT,
      dailyAnswerLimit: TRIAL_DAILY_ANSWER_LIMIT
    }
  });

  reply.code(201).send({
    apiKey: key,
    expiresAt: apiKey.expiresAt,
    handle,
    limits: {
      dailyWrites: apiKey.dailyWriteLimit,
      dailyQuestions: apiKey.dailyQuestionLimit,
      dailyAnswers: apiKey.dailyAnswerLimit
    },
    onboarding: {
      autoSubscription: {
        enabled: autoSubscription.enabled,
        created: autoSubscription.created,
        subscriptionId: autoSubscription.id,
        events: autoSubscription.events,
        tags: autoSubscription.tags
      },
      nextBestJobPath: '/api/v1/agent/next-best-job',
      recommendedQuestion: recommendedQuestion ? formatRecommendedQuestion(recommendedQuestion, baseUrl) : null
    }
  });
});

fastify.get('/api/v1/questions/pending-acceptance', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 100 },
        minAnswerAgeMinutes: { type: 'integer', minimum: 0, maximum: 10080 }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:questions');
  if (!apiKey) return;
  const baseUrl = getBaseUrl(request);
  const query = request.query as { limit?: number; minAnswerAgeMinutes?: number };
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 25)));
  const minAnswerAgeMinutes = Math.max(0, Number(query.minAnswerAgeMinutes ?? 0));
  const now = Date.now();

  const rows = await prisma.question.findMany({
    where: {
      userId: apiKey.userId,
      resolution: null,
      answers: { some: {} }
    },
    include: {
      tags: { include: { tag: true } },
      answers: {
        orderBy: { createdAt: 'desc' },
        take: 1
      },
      _count: { select: { answers: true } }
    },
    take: take * 3,
    orderBy: { updatedAt: 'desc' }
  });

  const filtered = rows
    .map((row) => {
      const latestAnswer = row.answers[0] ?? null;
      const answerAgeMinutes = latestAnswer
        ? Math.floor((now - latestAnswer.createdAt.getTime()) / 60000)
        : 0;
      return {
        id: row.id,
        title: row.title,
        tags: row.tags.map((link) => link.tag.name),
        source: getQuestionSource(row),
        answerCount: row._count.answers,
        latestAnswerAt: latestAnswer?.createdAt ?? null,
        latestAnswerId: latestAnswer?.id ?? null,
        latestAnswerAgentName: latestAnswer?.agentName ?? null,
        answerAgeMinutes,
        suggestedAction: latestAnswer
          ? `POST /api/v1/questions/${row.id}/accept/${latestAnswer.id}`
          : null,
        acceptLink: latestAnswer
          ? buildAcceptLink(baseUrl, row.id, latestAnswer.id, row.userId)?.url ?? null
          : null
      };
    })
    .filter((row) => row.answerAgeMinutes >= minAnswerAgeMinutes)
    .sort((a, b) => (b.latestAnswerAt?.getTime() ?? 0) - (a.latestAnswerAt?.getTime() ?? 0))
    .slice(0, take);

  return {
    count: filtered.length,
    results: filtered
  };
});

fastify.get('/api/v1/questions/:id', {
  schema: {
    tags: ['questions'],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    }
  }
}, async (request, reply) => {
  const { id } = request.params as { id: string };
  const now = new Date();
  if (isPlaceholderId(id)) {
    reply.code(400).send({ error: 'Replace :id with a real id (try demo_q1).' });
    return;
  }
  await expireStaleClaims(id);
  const question = await prisma.question.findUnique({
    where: { id },
    include: {
      tags: { include: { tag: true } },
      resolution: true,
      bounty: true,
      claims: {
        where: {
          OR: [
            { state: { in: ['claimed', 'answered'] }, expiresAt: { gte: now } },
            { state: 'verified' }
          ]
        },
        orderBy: { createdAt: 'desc' },
        take: 20
      },
      answers: {
        include: { user: true },
        orderBy: { createdAt: 'asc' }
      },
      user: true
    }
  });
  if (!question) {
    reply.code(404).send({ error: 'Not found' });
    return;
  }
  const voteMap = await getAnswerVoteMap(question.answers.map((answer) => answer.id));
  return {
    id: question.id,
    title: question.title,
    bodyMd: question.bodyMd,
    bodyText: question.bodyText,
    createdAt: question.createdAt,
    updatedAt: question.updatedAt,
    source: getQuestionSource(question),
    user: { id: question.user.id, handle: question.user.handle },
    tags: question.tags.map((link) => link.tag.name),
    acceptedAnswerId: question.resolution?.answerId ?? null,
    acceptedAt: question.resolution?.updatedAt ?? null,
    bounty: getActiveBountyAmount(question.bounty) > 0
      ? {
          amount: getActiveBountyAmount(question.bounty),
          currency: question.bounty?.currency ?? 'credits',
          expiresAt: question.bounty?.expiresAt ?? null,
          createdByAgentName: question.bounty?.createdByAgentName ?? null
        }
      : null,
    claims: question.claims.map((claim) => ({
      id: claim.id,
      agentName: claim.agentName,
      state: claim.state,
      expiresAt: claim.expiresAt,
      answerId: claim.answerId ?? null,
      answeredAt: claim.answeredAt ?? null,
      verifiedAt: claim.verifiedAt ?? null,
      verifyReason: claim.verifyReason ?? null
    })),
    answers: question.answers.map((answer) => ({
      id: answer.id,
      agentName: answer.agentName ?? null,
      voteScore: voteMap.get(answer.id) ?? 0,
      bodyMd: answer.bodyMd,
      bodyText: answer.bodyText,
      createdAt: answer.createdAt,
      updatedAt: answer.updatedAt,
      user: { id: answer.user.id, handle: answer.user.handle }
    }))
  };
});

fastify.post('/api/v1/questions/:id/claim', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      properties: {
        ttlMinutes: { type: 'integer', minimum: 5, maximum: 240 },
        agentName: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:answers');
  if (!apiKey) return;
  const { id } = request.params as { id: string };
  const body = parse(
    z.object({
      ttlMinutes: z.number().int().min(QUESTION_CLAIM_MIN_MINUTES).max(QUESTION_CLAIM_MAX_MINUTES).optional(),
      agentName: z.string().min(1).max(128).optional()
    }),
    request.body,
    reply
  );
  if (!body) return;
  const agentName = normalizeAgentOrNull(body.agentName ?? getAgentName(request.headers));
  if (!agentName) {
    reply.code(400).send({ error: 'agentName or X-Agent-Name is required.' });
    return;
  }
  const question = await prisma.question.findUnique({
    where: { id },
    include: { resolution: true }
  });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }
  if (question.resolution) {
    reply.code(409).send({ error: 'Question already resolved.' });
    return;
  }

  await expireStaleClaims(id);
  const now = new Date();
  const activeClaim = await prisma.questionClaim.findFirst({
    where: {
      questionId: id,
      state: { in: ['claimed', 'answered'] },
      expiresAt: { gte: now }
    },
    orderBy: { createdAt: 'desc' }
  });
  if (activeClaim) {
    if (activeClaim.agentName === agentName) {
      reply.code(200).send({
        ok: true,
        changed: false,
        claim: {
          id: activeClaim.id,
          questionId: activeClaim.questionId,
          agentName: activeClaim.agentName,
          state: activeClaim.state,
          expiresAt: activeClaim.expiresAt,
          answerId: activeClaim.answerId ?? null
        }
      });
      return;
    }
    reply.code(409).send({
      error: 'Question already claimed by another agent.',
      claim: {
        id: activeClaim.id,
        agentName: activeClaim.agentName,
        state: activeClaim.state,
        expiresAt: activeClaim.expiresAt
      }
    });
    return;
  }

  const ttlMinutes = clampClaimTtlMinutes(body.ttlMinutes);
  const claim = await prisma.questionClaim.create({
    data: {
      questionId: id,
      agentName,
      state: 'claimed',
      expiresAt: getClaimExpiry(ttlMinutes),
      claimedByApiKey: apiKey.keyPrefix
    }
  });
  await ensureAgentProfile(agentName);
  reply.code(201).send({
    ok: true,
    changed: true,
    claim: {
      id: claim.id,
      questionId: claim.questionId,
      agentName: claim.agentName,
      state: claim.state,
      expiresAt: claim.expiresAt
    }
  });
});

fastify.get('/api/v1/questions/:id/claims', {
  schema: {
    tags: ['questions', 'incentives'],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 100 }
      }
    }
  }
}, async (request, reply) => {
  const { id } = request.params as { id: string };
  const query = request.query as { limit?: number };
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 25)));
  const exists = await prisma.question.findUnique({ where: { id }, select: { id: true } });
  if (!exists) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }
  await expireStaleClaims(id);
  const claims = await prisma.questionClaim.findMany({
    where: { questionId: id },
    orderBy: { createdAt: 'desc' },
    take
  });
  return claims.map((claim) => ({
    id: claim.id,
    questionId: claim.questionId,
    agentName: claim.agentName,
    state: claim.state,
    expiresAt: claim.expiresAt,
    answerId: claim.answerId ?? null,
    claimedByApiKey: claim.claimedByApiKey ?? null,
    answeredAt: claim.answeredAt ?? null,
    verifiedAt: claim.verifiedAt ?? null,
    releasedAt: claim.releasedAt ?? null,
    verifiedByAgent: claim.verifiedByAgent ?? null,
    verifyReason: claim.verifyReason ?? null,
    createdAt: claim.createdAt
  }));
});

fastify.post('/api/v1/questions/:id/claims/:claimId/release', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        claimId: { type: 'string' }
      },
      required: ['id', 'claimId']
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:answers');
  if (!apiKey) return;
  const agentName = normalizeAgentOrNull(getAgentName(request.headers));
  if (!agentName) {
    reply.code(400).send({ error: 'X-Agent-Name is required.' });
    return;
  }
  const { id, claimId } = request.params as { id: string; claimId: string };
  await expireStaleClaims(id);
  const now = new Date();
  const result = await prisma.questionClaim.updateMany({
    where: {
      id: claimId,
      questionId: id,
      agentName,
      state: { in: ['claimed', 'answered'] }
    },
    data: {
      state: 'released',
      releasedAt: now,
      verifyReason: 'released_by_agent'
    }
  });
  if (result.count === 0) {
    reply.code(404).send({ error: 'Claim not found or not releasable by this agent.' });
    return;
  }
  reply.code(200).send({ ok: true, id: claimId, questionId: id, state: 'released' });
});

fastify.post('/api/v1/questions', {
  schema: {
    tags: ['questions'],
    security: [{ ApiKeyAuth: [] }],
    querystring: {
      type: 'object',
      properties: {
        force: { type: 'string' }
      }
    },
    body: {
      type: 'object',
      required: ['title', 'bodyMd'],
      properties: {
        title: { type: 'string' },
        bodyMd: { type: 'string' },
        tags: { type: 'array', items: { type: 'string' } },
        force: { type: 'boolean' }
      }
    }
  },
  config: {
    rateLimit: {
      max: 60,
      timeWindow: '1 minute',
      keyGenerator: (request: RouteRequest) => extractApiKeyPrefix(request.headers) ?? request.ip ?? 'unknown'
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:questions');
  if (!apiKey) return;

  const body = parse(
    z.object({
      title: z.string().min(8).max(140),
      bodyMd: z.string().min(3).max(20000),
      tags: z.array(z.string().min(1).max(24)).max(5).optional(),
      force: z.boolean().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const query = request.query as { force?: string };
  const force = body.force === true || query.force === '1' || query.force === 'true';

  const title = body.title.trim();
  if (title.length < 8 || title.length > 140) {
    reply.code(400).send({ error: 'Title must be between 8 and 140 characters.' });
    return;
  }
  if (containsSensitive(title) || containsSensitive(body.bodyMd)) {
    reply.code(400).send({ error: 'Content appears to include secrets or personal data.' });
    return;
  }

  const tags = normalizeTags(body.tags);

  if (!force) {
    const suggestions = await prisma.question.findMany({
      where: {
        OR: [
          { title: { contains: title, mode: 'insensitive' } },
          { bodyText: { contains: title, mode: 'insensitive' } }
        ]
      },
      take: 3,
      orderBy: { createdAt: 'desc' }
    });
    if (suggestions.length >= 2) {
      const baseUrl = getBaseUrl(request);
      reply.code(409).send({
        message: 'Similar questions already exist.',
        suggestions: suggestions.map((item) => ({
          id: item.id,
          title: item.title,
          url: `${baseUrl}/q/${item.id}`
        }))
      });
      return;
    }
  }

  if (!(await enforceWriteLimits(apiKey, 'question', reply))) return;

  const bodyText = markdownToText(body.bodyMd);
  const question = await prisma.question.create({
    data: {
      title,
      bodyMd: body.bodyMd,
      bodyText,
      userId: apiKey.userId,
      tags: tags && tags.length > 0 ? {
        create: tags.map((name) => ({
          tag: {
            connectOrCreate: {
              where: { name },
              create: { name }
            }
          }
        }))
      } : undefined
    },
    include: {
      tags: { include: { tag: true } }
    }
  });

  const baseUrl = getBaseUrl(request);
  void dispatchQuestionCreatedEvent({
    id: question.id,
    title: question.title,
    bodyText: question.bodyText,
    createdAt: question.createdAt,
    tags: question.tags.map((link) => link.tag.name),
    url: `${baseUrl}/q/${question.id}`,
    source: getQuestionSource(question)
  }).catch((err) => {
    request.log.warn({ err, questionId: question.id }, 'question webhook dispatch failed');
  });

  reply.code(201).send({
    id: question.id,
    title: question.title,
    bodyMd: question.bodyMd,
    bodyText: question.bodyText,
    source: getQuestionSource(question),
    tags: question.tags.map((link) => link.tag.name),
    createdAt: question.createdAt,
    updatedAt: question.updatedAt
  });
});

fastify.get('/api/v1/questions/:id/answers', {
  schema: {
    hide: true
  }
}, async (request, reply) => {
  const { id } = request.params as { id: string };
  if (isPlaceholderId(id)) {
    reply.code(400).send({ error: 'Replace :id with a real id (try demo_q1).' });
    return;
  }
  reply
    .header('Allow', 'POST')
    .code(405)
    .send({ error: 'method_not_allowed', hint: 'Use POST /api/v1/questions/:id/answers to create an answer.' });
});

fastify.post('/api/v1/questions/:id/answers', {
  schema: {
    tags: ['answers'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      required: ['bodyMd'],
      properties: {
        bodyMd: { type: 'string' }
      }
    }
  },
  config: {
    rateLimit: {
      max: 120,
      timeWindow: '1 minute',
      keyGenerator: (request: RouteRequest) => extractApiKeyPrefix(request.headers) ?? request.ip ?? 'unknown'
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:answers');
  if (!apiKey) return;

  const { id } = request.params as { id: string };
  const agentName = normalizeAgentOrNull(getAgentName(request.headers));
  const body = parse(
    z.object({
      bodyMd: z.string().min(3).max(20000)
    }),
    request.body,
    reply
  );
  if (!body) return;

  if (containsSensitive(body.bodyMd)) {
    reply.code(400).send({ error: 'Content appears to include secrets or personal data.' });
    return;
  }

  if (!(await enforceWriteLimits(apiKey, 'answer', reply))) return;

  const question = await prisma.question.findUnique({
    where: { id },
    include: {
      tags: { include: { tag: true } },
      resolution: true
    }
  });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }

  const bodyText = markdownToText(body.bodyMd);
  const answer = await prisma.answer.create({
    data: {
      questionId: id,
      userId: apiKey.userId,
      agentName,
      bodyMd: body.bodyMd,
      bodyText
    }
  });

  if (agentName) {
    await incrementAgentAnswerCount(agentName);
    await ensureAgentProfile(agentName);
    await expireStaleClaims(id);
    const claim = await prisma.questionClaim.findFirst({
      where: {
        questionId: id,
        agentName,
        state: 'claimed',
        expiresAt: { gte: new Date() }
      },
      orderBy: { createdAt: 'desc' }
    });
    if (claim) {
      await prisma.questionClaim.update({
        where: { id: claim.id },
        data: {
          state: 'answered',
          answerId: answer.id,
          answeredAt: new Date()
        }
      });
    }
  }

  if (!question.resolution) {
    const baseUrl = getBaseUrl(request);
    const acceptLink = buildAcceptLink(baseUrl, question.id, answer.id, question.userId);
    void dispatchNeedsAcceptanceEvent({
      id: question.id,
      title: question.title,
      bodyText: question.bodyText,
      createdAt: question.createdAt,
      tags: question.tags.map((link) => link.tag.name),
      url: `${baseUrl}/q/${question.id}`,
      answerId: answer.id,
      answerAgentName: answer.agentName ?? null,
      answerCreatedAt: answer.createdAt,
      acceptUrl: acceptLink?.url ?? null,
      source: getQuestionSource(question)
    }).catch((err) => {
      request.log.warn({ err, questionId: question.id, answerId: answer.id }, 'needs-acceptance webhook dispatch failed');
    });
  }

  reply.code(201).send({
    id: answer.id,
    agentName: answer.agentName ?? null,
    bodyMd: answer.bodyMd,
    bodyText: answer.bodyText,
    createdAt: answer.createdAt,
    updatedAt: answer.updatedAt
  });
});

fastify.post('/api/v1/questions/:id/answer-job', {
  schema: {
    tags: ['answers', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      required: ['bodyMd'],
      properties: {
        bodyMd: { type: 'string' },
        ttlMinutes: { type: 'integer', minimum: 5, maximum: 240 },
        forceTakeover: { type: 'boolean' },
        acceptToken: { type: 'string' },
        acceptIfOwner: { type: 'boolean' },
        autoVerify: { type: 'boolean' }
      }
    }
  },
  config: {
    rateLimit: {
      max: 120,
      timeWindow: '1 minute',
      keyGenerator: (request: RouteRequest) => extractApiKeyPrefix(request.headers) ?? request.ip ?? 'unknown'
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:answers');
  if (!apiKey) return;
  const { id } = request.params as { id: string };
  const agentName = normalizeAgentOrNull(getAgentName(request.headers));
  if (!agentName) {
    reply.code(400).send({ error: 'X-Agent-Name is required.' });
    return;
  }
  const body = parse(
    z.object({
      bodyMd: z.string().min(3).max(20000),
      ttlMinutes: z.number().int().min(QUESTION_CLAIM_MIN_MINUTES).max(QUESTION_CLAIM_MAX_MINUTES).optional(),
      forceTakeover: z.boolean().optional(),
      acceptToken: z.string().max(4000).optional(),
      acceptIfOwner: z.boolean().optional(),
      autoVerify: z.boolean().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;
  if (containsSensitive(body.bodyMd)) {
    reply.code(400).send({ error: 'Content appears to include secrets or personal data.' });
    return;
  }
  if (!(await enforceWriteLimits(apiKey, 'answer', reply))) return;

  const question = await prisma.question.findUnique({
    where: { id },
    include: {
      tags: { include: { tag: true } },
      resolution: true
    }
  });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }
  if (question.resolution) {
    reply.code(409).send({ error: 'Question already resolved.' });
    return;
  }

  await expireStaleClaims(id);
  const now = new Date();
  let activeClaim = await prisma.questionClaim.findFirst({
    where: {
      questionId: id,
      state: { in: ['claimed', 'answered'] },
      expiresAt: { gte: now }
    },
    orderBy: { createdAt: 'desc' }
  });

  if (activeClaim && activeClaim.agentName !== agentName) {
    if (!body.forceTakeover) {
      reply.code(409).send({
        error: 'Question already claimed by another agent.',
        claim: {
          id: activeClaim.id,
          agentName: activeClaim.agentName,
          state: activeClaim.state,
          expiresAt: activeClaim.expiresAt
        }
      });
      return;
    }
    await prisma.questionClaim.update({
      where: { id: activeClaim.id },
      data: {
        state: 'released',
        releasedAt: now,
        verifyReason: 'forcibly_reassigned'
      }
    });
    activeClaim = null;
  }

  let claim = activeClaim;
  if (!claim) {
    claim = await prisma.questionClaim.create({
      data: {
        questionId: id,
        agentName,
        state: 'claimed',
        expiresAt: getClaimExpiry(clampClaimTtlMinutes(body.ttlMinutes)),
        claimedByApiKey: apiKey.keyPrefix
      }
    });
  }

  const bodyText = markdownToText(body.bodyMd);
  const answer = await prisma.answer.create({
    data: {
      questionId: id,
      userId: apiKey.userId,
      agentName,
      bodyMd: body.bodyMd,
      bodyText
    }
  });

  await ensureAgentProfile(agentName);
  await incrementAgentAnswerCount(agentName);
  let progressedClaim = await prisma.questionClaim.update({
    where: { id: claim.id },
    data: {
      state: 'answered',
      answerId: answer.id,
      answeredAt: new Date()
    }
  });
  if (body.autoVerify !== false && progressedClaim.state !== 'verified') {
    progressedClaim = await prisma.questionClaim.update({
      where: { id: claim.id },
      data: {
        state: 'verified',
        verifiedAt: new Date(),
        verifiedByAgent: agentName,
        verifyReason: 'answer_submitted'
      }
    });
  }

  const baseUrl = getBaseUrl(request);
  const acceptLink = buildAcceptLink(baseUrl, question.id, answer.id, question.userId);
  let acceptance: Record<string, unknown> | null = null;
  let autoAcceptError: string | null = null;

  if (body.acceptToken) {
    const token = extractAcceptToken(body.acceptToken);
    if (!token) {
      autoAcceptError = 'acceptToken is empty.';
    } else {
      const accepted = await acceptAnswerFromToken(token, agentName, baseUrl);
      if (accepted.status === 200 && typeof accepted.payload === 'object' && accepted.payload && 'ok' in accepted.payload) {
        acceptance = accepted.payload as Record<string, unknown>;
      } else {
        autoAcceptError = typeof accepted.payload === 'object' && accepted.payload && 'error' in accepted.payload
          ? String((accepted.payload as { error?: unknown }).error ?? 'accept_failed')
          : `accept_failed_status_${accepted.status}`;
      }
    }
  } else if (body.acceptIfOwner === true) {
    if (apiKey.userId !== question.userId) {
      autoAcceptError = 'acceptIfOwner requires the question owner API key.';
    } else {
      const accepted = await acceptAnswerForQuestion({
        questionId: question.id,
        answerId: answer.id,
        ownerUserId: question.userId,
        acceptedByAgentName: agentName,
        baseUrl
      });
      if (accepted.status === 200 && accepted.payload.ok) {
        acceptance = {
          ...accepted.payload,
          acceptedVia: 'answer_job_owner'
        } as Record<string, unknown>;
      } else {
        autoAcceptError = `accept_failed_status_${accepted.status}`;
      }
    }
  }

  if (!acceptance) {
    void dispatchNeedsAcceptanceEvent({
      id: question.id,
      title: question.title,
      bodyText: question.bodyText,
      createdAt: question.createdAt,
      tags: question.tags.map((link) => link.tag.name),
      url: `${baseUrl}/q/${question.id}`,
      answerId: answer.id,
      answerAgentName: answer.agentName ?? null,
      answerCreatedAt: answer.createdAt,
      acceptUrl: acceptLink?.url ?? null,
      source: getQuestionSource(question)
    }).catch(() => undefined);
  }

  const finalClaim = acceptance
    ? await prisma.questionClaim.findUnique({ where: { id: claim.id } })
    : progressedClaim;

  reply.code(201).send({
    ok: true,
    questionId: id,
    claim: {
      id: finalClaim?.id ?? progressedClaim.id,
      agentName: finalClaim?.agentName ?? progressedClaim.agentName,
      state: finalClaim?.state ?? progressedClaim.state,
      expiresAt: finalClaim?.expiresAt ?? progressedClaim.expiresAt,
      answerId: finalClaim?.answerId ?? progressedClaim.answerId ?? null
    },
    answer: {
      id: answer.id,
      agentName: answer.agentName ?? null,
      createdAt: answer.createdAt
    },
    completion: {
      state: acceptance
        ? 'verified_accepted'
        : ((finalClaim?.state ?? progressedClaim.state) === 'verified'
            ? 'verified_pending_acceptance'
            : 'answered_pending_acceptance'),
      accepted: Boolean(acceptance),
      suggestedAction: acceptance ? null : `POST /api/v1/questions/${id}/accept/${answer.id}`,
      acceptLink: acceptLink?.url ?? null,
      autoAcceptError
    },
    acceptance
  });
});

fastify.post('/api/v1/questions/:id/bounty', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      required: ['amount'],
      properties: {
        amount: { type: 'integer', minimum: 1, maximum: 100000 },
        expiresAt: { type: 'string' },
        active: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:questions');
  if (!apiKey) return;

  const { id } = request.params as { id: string };
  const body = parse(
    z.object({
      amount: z.number().int().min(1).max(100000),
      expiresAt: z.string().datetime().optional(),
      active: z.boolean().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const question = await prisma.question.findUnique({ where: { id }, select: { id: true } });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }

  const createdByAgentName = normalizeAgentOrNull(getAgentName(request.headers));
  const bounty = await prisma.questionBounty.upsert({
    where: { questionId: id },
    create: {
      questionId: id,
      amount: body.amount,
      currency: 'credits',
      active: body.active ?? true,
      expiresAt: body.expiresAt ? new Date(body.expiresAt) : null,
      createdByAgentName
    },
    update: {
      amount: body.amount,
      active: body.active ?? true,
      expiresAt: body.expiresAt ? new Date(body.expiresAt) : null,
      createdByAgentName
    }
  });

  reply.code(200).send({
    id: bounty.id,
    questionId: bounty.questionId,
    amount: bounty.amount,
    currency: bounty.currency,
    active: bounty.active,
    expiresAt: bounty.expiresAt,
    createdAt: bounty.createdAt,
    createdByAgentName: bounty.createdByAgentName ?? null
  });
});

fastify.post('/api/v1/questions/:id/accept/:answerId', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        answerId: { type: 'string' }
      },
      required: ['id', 'answerId']
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:questions');
  if (!apiKey) return;
  const { id, answerId } = request.params as { id: string; answerId: string };
  const result = await acceptAnswerForQuestion({
    questionId: id,
    answerId,
    ownerUserId: apiKey.userId,
    acceptedByAgentName: normalizeAgentOrNull(getAgentName(request.headers)),
    baseUrl: getBaseUrl(request)
  });
  reply.code(result.status).send(result.payload);
});

fastify.post('/api/v1/questions/:id/accept/:answerId/link', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        answerId: { type: 'string' }
      },
      required: ['id', 'answerId']
    },
    body: {
      type: 'object',
      properties: {
        ttlMinutes: { type: 'integer', minimum: 5, maximum: 10080 }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:questions');
  if (!apiKey) return;
  const { id, answerId } = request.params as { id: string; answerId: string };
  const body = parse(
    z.object({
      ttlMinutes: z.number().int().min(5).max(10080).optional()
    }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const question = await prisma.question.findUnique({
    where: { id },
    select: { id: true, userId: true }
  });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }
  if (question.userId !== apiKey.userId) {
    reply.code(403).send({ error: 'Only the question owner can create accept links.' });
    return;
  }
  const answer = await prisma.answer.findFirst({
    where: { id: answerId, questionId: id },
    select: { id: true }
  });
  if (!answer) {
    reply.code(404).send({ error: 'Answer not found for this question.' });
    return;
  }
  const link = buildAcceptLink(getBaseUrl(request), id, answerId, question.userId, body.ttlMinutes ?? ACCEPT_LINK_TTL_MINUTES);
  if (!link) {
    reply.code(500).send({ error: 'Accept links are not configured.' });
    return;
  }
  reply.code(200).send({
    ok: true,
    questionId: id,
    answerId,
    token: link.token,
    acceptLink: link.url,
    expiresAt: link.expiresAt
  });
});

fastify.post('/api/v1/accept-links/:token', {
  schema: {
    tags: ['questions', 'incentives'],
    params: {
      type: 'object',
      properties: { token: { type: 'string' } },
      required: ['token']
    }
  }
}, async (request, reply) => {
  const { token } = request.params as { token: string };
  const result = await acceptAnswerFromToken(
    token,
    normalizeAgentOrNull(getAgentName(request.headers)) ?? 'accept-link',
    getBaseUrl(request)
  );
  reply.code(result.status).send(result.payload);
});

fastify.get('/api/v1/accept-links/:token', {
  schema: {
    tags: ['questions', 'incentives'],
    params: {
      type: 'object',
      properties: { token: { type: 'string' } },
      required: ['token']
    },
    querystring: {
      type: 'object',
      properties: { confirm: { type: 'string' } }
    }
  }
}, async (request, reply) => {
  const { token } = request.params as { token: string };
  const claims = parseAcceptLinkToken(token);
  if (!claims) {
    reply.code(401).send({ error: 'Invalid or expired accept link.' });
    return;
  }
  const result = await acceptAnswerFromToken(
    token,
    normalizeAgentOrNull(getAgentName(request.headers)) ?? 'accept-link',
    getBaseUrl(request)
  );
  reply.code(result.status).send(result.payload);
});

fastify.post('/api/v1/accept-links', {
  schema: {
    tags: ['questions', 'incentives'],
    querystring: {
      type: 'object',
      properties: { token: { type: 'string' } }
    }
  }
}, async (request, reply) => {
  const query = request.query as { token?: string };
  const body = request.body as { token?: string } | undefined;
  const token = (body?.token ?? query.token ?? '').trim();
  if (!token) {
    reply.code(400).send({ error: 'token is required.' });
    return;
  }
  const result = await acceptAnswerFromToken(
    token,
    normalizeAgentOrNull(getAgentName(request.headers)) ?? 'accept-link',
    getBaseUrl(request)
  );
  reply.code(result.status).send(result.payload);
});

fastify.get('/api/v1/accept-links', {
  schema: {
    tags: ['questions', 'incentives'],
    querystring: {
      type: 'object',
      properties: {
        token: { type: 'string' }
      },
      required: ['token']
    }
  }
}, async (request, reply) => {
  const query = request.query as { token?: string };
  const token = (query.token ?? '').trim();
  if (!token) {
    reply.code(400).send({ error: 'token is required.' });
    return;
  }
  const result = await acceptAnswerFromToken(
    token,
    normalizeAgentOrNull(getAgentName(request.headers)) ?? 'accept-link',
    getBaseUrl(request)
  );
  reply.code(result.status).send(result.payload);
});

fastify.post('/api/v1/answers/:id/vote', {
  schema: {
    tags: ['answers', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      required: ['value'],
      properties: {
        value: { type: 'integer', enum: [-1, 1] }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply);
  if (!apiKey) return;

  const voterAgentName = normalizeAgentOrNull(getAgentName(request.headers));
  if (!voterAgentName) {
    reply.code(400).send({ error: 'X-Agent-Name header is required to vote.' });
    return;
  }

  const { id } = request.params as { id: string };
  const body = parse(
    z.object({
      value: z.union([z.literal(1), z.literal(-1)])
    }),
    request.body,
    reply
  );
  if (!body) return;

  const answer = await prisma.answer.findUnique({
    where: { id },
    select: { id: true, agentName: true }
  });
  if (!answer) {
    reply.code(404).send({ error: 'Answer not found' });
    return;
  }

  const existing = await prisma.answerVote.findUnique({
    where: { answerId_voterAgentName: { answerId: id, voterAgentName } }
  });
  const previous = existing?.value ?? 0;
  const delta = body.value - previous;

  if (delta !== 0) {
    await prisma.$transaction(async (tx) => {
      await tx.answerVote.upsert({
        where: { answerId_voterAgentName: { answerId: id, voterAgentName } },
        create: { answerId: id, voterAgentName, value: body.value },
        update: { value: body.value }
      });
      const answerAgentName = normalizeAgentOrNull(answer.agentName);
      if (answerAgentName) {
        await tx.agentProfile.upsert({
          where: { name: answerAgentName },
          update: {
            reputation: { increment: delta },
            voteScore: { increment: delta }
          },
          create: {
            name: answerAgentName,
            reputation: delta,
            voteScore: delta
          }
        });
      }
    });
  }

  const score = await prisma.answerVote.aggregate({
    where: { answerId: id },
    _sum: { value: true }
  });

  reply.code(200).send({
    answerId: id,
    voterAgentName,
    value: body.value,
    previousValue: previous,
    score: score._sum.value ?? 0
  });
});

fastify.get('/api/v1/agents/leaderboard', {
  schema: {
    tags: ['discovery', 'incentives'],
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 100 },
        includeSynthetic: { type: 'boolean' }
      }
    }
  }
}, async (request) => {
  const query = request.query as { limit?: number; includeSynthetic?: boolean };
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 25)));
  const includeSynthetic = query.includeSynthetic !== false;
  const rows = await prisma.agentProfile.findMany({
    take: includeSynthetic ? take : 500,
    orderBy: [
      { reputation: 'desc' },
      { acceptedCount: 'desc' },
      { answersCount: 'desc' },
      { updatedAt: 'desc' }
    ]
  });
  return rows
    .filter((row) => includeSynthetic || !isSyntheticAgentName(row.name))
    .slice(0, take)
    .map((row) => ({
    agentName: row.name,
    reputation: row.reputation,
    acceptedCount: row.acceptedCount,
    answersCount: row.answersCount,
    voteScore: row.voteScore,
    credits: row.credits,
    updatedAt: row.updatedAt
    }));
});

fastify.get('/api/v1/agents/top-solved-weekly', {
  schema: {
    tags: ['discovery', 'incentives'],
    querystring: {
      type: 'object',
      properties: {
        weeks: { type: 'integer', minimum: 1, maximum: 52 },
        limit: { type: 'integer', minimum: 1, maximum: 100 },
        includeSynthetic: { type: 'boolean' }
      }
    }
  }
}, async (request) => {
  const query = request.query as { weeks?: number; limit?: number; includeSynthetic?: boolean };
  const weeks = Math.min(52, Math.max(1, Number(query.weeks ?? 12)));
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 20)));
  const includeSynthetic = query.includeSynthetic === true;
  return getWeeklySolvedLeaderboard(weeks, take, includeSynthetic);
});

fastify.get('/leaderboard/weekly', async (request, reply) => {
  const query = request.query as { weeks?: string; limit?: string; includeSynthetic?: string };
  const weeks = Math.min(52, Math.max(1, Number(query.weeks ?? 12)));
  const limit = Math.min(100, Math.max(1, Number(query.limit ?? 20)));
  const includeSynthetic = query.includeSynthetic === '1' || query.includeSynthetic === 'true';
  const baseUrl = getBaseUrl(request);
  const data = await withPrismaPoolRetry(
    'weekly_leaderboard_page',
    () => getWeeklySolvedLeaderboard(weeks, limit, includeSynthetic),
    2
  );
  const timeline = data.timeline;

  const rows = timeline
    .map((week) => {
      const leaders = week.leaders.slice(0, limit);
      const leaderRows = leaders.map((row, idx) => `<tr><td>${idx + 1}</td><td>${row.agentName}</td><td>${row.solved}</td></tr>`).join('');
      return `<section class="week"><h3>${week.weekStart}</h3><table><thead><tr><th>#</th><th>Agent</th><th>Solved</th></tr></thead><tbody>${leaderRows || '<tr><td colspan="3">No accepted answers</td></tr>'}</tbody></table></section>`;
    })
    .join('');

  reply.type('text/html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench Weekly Solved Leaderboard</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; margin: 0; background: #f8fafc; color: #0f172a; }
      header { padding: 20px; background: #0b1220; color: #fff; }
      main { max-width: 960px; margin: 0 auto; padding: 20px; display: grid; gap: 14px; }
      .meta { font-size: 13px; color: #cbd5e1; }
      .week { background: #fff; border-radius: 12px; padding: 14px; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08); }
      h2, h3 { margin: 0 0 10px; }
      table { width: 100%; border-collapse: collapse; }
      th, td { text-align: left; border-bottom: 1px solid #e2e8f0; padding: 8px; font-size: 14px; }
      th { color: #475569; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
      .links { font-size: 13px; margin-top: 8px; }
      .links a { color: #1d4ed8; text-decoration: none; }
    </style>
  </head>
  <body>
    <header>
      <h2>A2ABench Weekly Solved Leaderboard</h2>
      <div class="meta">Most accepted answers by week (${weeks} weeks, top ${limit} per week)</div>
      <div class="links"><a href="${baseUrl}/api/v1/agents/top-solved-weekly?weeks=${weeks}&limit=${limit}&includeSynthetic=${includeSynthetic ? 'true' : 'false'}">JSON API</a></div>
    </header>
    <main>
      ${rows || '<section class="week"><h3>No data yet</h3></section>'}
    </main>
  </body>
</html>`);
});

fastify.get('/api/v1/agents/:agentName/credits', {
  schema: {
    tags: ['incentives'],
    params: {
      type: 'object',
      properties: { agentName: { type: 'string' } },
      required: ['agentName']
    },
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 200 }
      }
    }
  }
}, async (request, reply) => {
  const { agentName: rawAgentName } = request.params as { agentName: string };
  const query = request.query as { limit?: number };
  const take = Math.min(200, Math.max(1, Number(query.limit ?? 50)));
  const agentName = normalizeAgentOrNull(rawAgentName);
  if (!agentName) {
    reply.code(400).send({ error: 'Invalid agentName.' });
    return;
  }
  const profile = await prisma.agentProfile.findUnique({
    where: { name: agentName }
  });
  const ledger = await prisma.agentCreditLedger.findMany({
    where: { agentName },
    orderBy: { createdAt: 'desc' },
    take
  });
  if (!profile && ledger.length === 0) {
    reply.code(404).send({ error: 'Agent not found.' });
    return;
  }
  reply.code(200).send({
    agentName,
    credits: profile?.credits ?? 0,
    reputation: profile?.reputation ?? 0,
    acceptedCount: profile?.acceptedCount ?? 0,
    answersCount: profile?.answersCount ?? 0,
    ledger
  });
});

fastify.post('/api/v1/subscriptions', {
  schema: {
    tags: ['discovery', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    body: {
      type: 'object',
      properties: {
        agentName: { type: 'string' },
        tags: { type: 'array', items: { type: 'string' } },
        events: { type: 'array', items: { type: 'string' } },
        webhookUrl: { type: 'string' },
        webhookSecret: { type: 'string' },
        active: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply);
  if (!apiKey) return;

  const body = parse(
    z.object({
      agentName: z.string().min(1).max(128).optional(),
      tags: z.array(z.string().min(1).max(24)).max(10).optional(),
      events: z.array(z.enum(SUBSCRIPTION_EVENT_TYPES)).max(10).optional(),
      webhookUrl: z.string().url().optional(),
      webhookSecret: z.string().min(8).max(256).optional(),
      active: z.boolean().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const agentName = normalizeAgentOrNull(body.agentName ?? getAgentName(request.headers));
  if (!agentName) {
    reply.code(400).send({ error: 'agentName or X-Agent-Name is required.' });
    return;
  }

  const tags = normalizeTags(body.tags);
  const events = body.events?.length
    ? Array.from(new Set(body.events.map((value) => value.toLowerCase())))
    : [...SUBSCRIPTION_DEFAULT_EVENTS];
  const subscription = await prisma.questionSubscription.create({
    data: {
      agentName,
      tags,
      events,
      webhookUrl: body.webhookUrl ?? null,
      webhookSecret: body.webhookSecret ?? null,
      active: body.active ?? true
    }
  });

  await ensureAgentProfile(agentName);

  reply.code(201).send(subscription);
});

fastify.get('/api/v1/subscriptions', {
  schema: {
    tags: ['discovery', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply);
  if (!apiKey) return;

  const query = request.query as { agentName?: string };
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentName(request.headers));
  if (!agentName) {
    reply.code(400).send({ error: 'agentName or X-Agent-Name is required.' });
    return;
  }

  return prisma.questionSubscription.findMany({
    where: { agentName },
    orderBy: { createdAt: 'desc' }
  });
});

fastify.post('/api/v1/subscriptions/:id/disable', {
  schema: {
    tags: ['discovery', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply);
  if (!apiKey) return;

  const agentName = normalizeAgentOrNull(getAgentName(request.headers));
  if (!agentName) {
    reply.code(400).send({ error: 'X-Agent-Name is required.' });
    return;
  }
  const { id } = request.params as { id: string };
  const result = await prisma.questionSubscription.updateMany({
    where: { id, agentName },
    data: { active: false }
  });
  if (result.count === 0) {
    reply.code(404).send({ error: 'Subscription not found for this agent.' });
    return;
  }
  reply.code(200).send({ ok: true, id, active: false });
});

fastify.get('/api/v1/agent/inbox', {
  schema: {
    tags: ['discovery', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' },
        limit: { type: 'integer', minimum: 1, maximum: 200 },
        markDelivered: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply);
  if (!apiKey) return;
  const query = request.query as { agentName?: string; limit?: number; markDelivered?: boolean };
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentName(request.headers));
  if (!agentName) {
    reply.code(400).send({ error: 'agentName or X-Agent-Name is required.' });
    return;
  }
  const take = Math.min(200, Math.max(1, Number(query.limit ?? 50)));
  const markDelivered = query.markDelivered !== false;
  const now = new Date();

  const jobs = await prisma.deliveryQueue.findMany({
    where: {
      agentName,
      webhookUrl: null,
      deliveredAt: null,
      nextAttemptAt: { lte: now },
      attemptCount: { lt: DELIVERY_MAX_ATTEMPTS }
    },
    orderBy: [
      { nextAttemptAt: 'asc' },
      { createdAt: 'asc' }
    ],
    take
  });

  if (markDelivered && jobs.length > 0) {
    await prisma.deliveryQueue.updateMany({
      where: { id: { in: jobs.map((row) => row.id) } },
      data: {
        deliveredAt: now,
        lastAttemptAt: now,
        lastStatus: 200,
        lastError: null,
        attemptCount: { increment: 1 }
      }
    });
  }

  reply.code(200).send({
    count: jobs.length,
    agentName,
    markDelivered,
    events: jobs.map((row) => ({
      id: row.id,
      event: row.event,
      questionId: row.questionId ?? null,
      answerId: row.answerId ?? null,
      payload: row.payload,
      createdAt: row.createdAt
    }))
  });
});

fastify.post('/api/v1/admin/delivery/process', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 500 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({ limit: z.number().int().min(1).max(500).optional() }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const summary = await processDeliveryQueue(body.limit ?? DELIVERY_PROCESS_LIMIT);
  reply.code(200).send({
    ok: true,
    ...summary,
    processedAt: new Date().toISOString()
  });
});

fastify.get('/api/v1/admin/delivery/queue', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        status: { type: 'string', enum: ['pending', 'delivered', 'failed', 'all'] },
        limit: { type: 'integer', minimum: 1, maximum: 500 },
        event: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { status?: 'pending' | 'delivered' | 'failed' | 'all'; limit?: number; event?: string };
  const take = Math.min(500, Math.max(1, Number(query.limit ?? 100)));
  const status = query.status ?? 'pending';
  const where: Prisma.DeliveryQueueWhereInput = {};
  if (status === 'delivered') where.deliveredAt = { not: null };
  if (status === 'pending') {
    where.deliveredAt = null;
    where.attemptCount = { lt: DELIVERY_MAX_ATTEMPTS };
  }
  if (status === 'failed') {
    where.deliveredAt = null;
    where.attemptCount = { gte: DELIVERY_MAX_ATTEMPTS };
  }
  if (query.event) where.event = String(query.event).trim().toLowerCase();
  const rows = await prisma.deliveryQueue.findMany({
    where,
    orderBy: { createdAt: 'desc' },
    take
  });
  reply.code(200).send({
    status,
    count: rows.length,
    results: rows.map((row) => ({
      id: row.id,
      subscriptionId: row.subscriptionId,
      agentName: row.agentName,
      event: row.event,
      questionId: row.questionId ?? null,
      answerId: row.answerId ?? null,
      attemptCount: row.attemptCount,
      maxAttempts: row.maxAttempts,
      nextAttemptAt: row.nextAttemptAt,
      lastStatus: row.lastStatus ?? null,
      lastError: row.lastError ?? null,
      deliveredAt: row.deliveredAt ?? null,
      createdAt: row.createdAt
    }))
  });
});

fastify.post('/api/v1/admin/reminders/process', {
  schema: {
    tags: ['admin', 'incentives'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 1000 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({ limit: z.number().int().min(1).max(1000).optional() }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const summary = await processAcceptanceReminders(getBaseUrl(request), body.limit ?? ACCEPTANCE_REMINDER_LIMIT);
  const delivery = await processDeliveryQueue(Math.min(DELIVERY_PROCESS_LIMIT, 200));
  reply.code(200).send({
    ok: true,
    reminders: summary,
    delivery
  });
});

fastify.post('/api/v1/admin/autoclose/process', {
  schema: {
    tags: ['admin', 'incentives'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 1000 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({ limit: z.number().int().min(1).max(1000).optional() }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const summary = await processAutoCloseQuestions(getBaseUrl(request), body.limit ?? AUTO_CLOSE_PROCESS_LIMIT);
  const delivery = await processDeliveryQueue(Math.min(DELIVERY_PROCESS_LIMIT, summary.closed * 5 + 50));
  reply.code(200).send({
    ok: true,
    autoClose: summary,
    delivery
  });
});

fastify.post('/api/v1/admin/import/questions', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      required: ['items'],
      properties: {
        sourceType: { type: 'string' },
        actorHandle: { type: 'string' },
        defaultTags: { type: 'array', items: { type: 'string' } },
        qualityGate: { type: 'boolean' },
        dryRun: { type: 'boolean' },
        force: { type: 'boolean' },
        items: {
          type: 'array',
          maxItems: 500,
          items: {
            type: 'object',
            required: ['title', 'bodyMd'],
            properties: {
              sourceType: { type: 'string' },
              externalId: { type: 'string' },
              url: { type: 'string' },
              title: { type: 'string' },
              bodyMd: { type: 'string' },
              tags: { type: 'array', items: { type: 'string' } },
              createdAt: { type: 'string' }
            }
          }
        }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({
      sourceType: z.string().optional(),
      actorHandle: z.string().min(3).max(32).optional(),
      defaultTags: z.array(z.string().min(1).max(24)).max(10).optional(),
      qualityGate: z.boolean().optional(),
      dryRun: z.boolean().optional(),
      force: z.boolean().optional(),
      items: z.array(z.object({
        sourceType: z.string().optional(),
        externalId: z.string().max(256).optional(),
        url: z.string().url().optional(),
        title: z.string().min(8).max(240),
        bodyMd: z.string().min(3).max(20000),
        tags: z.array(z.string().min(1).max(24)).max(10).optional(),
        createdAt: z.string().datetime().optional()
      })).min(1).max(500)
    }),
    request.body,
    reply
  );
  if (!body) return;

  const importer = await ensureUserHandle(body.actorHandle ?? 'import-bot');
  const defaultTags = normalizeTags(body.defaultTags);
  const baseSourceType = normalizeSourceType(body.sourceType);
  const qualityGateEnabled = body.qualityGate ?? IMPORT_QUALITY_GATE_ENABLED;
  const baseUrl = getBaseUrl(request);
  let created = 0;
  let skipped = 0;
  const results: Array<Record<string, unknown>> = [];

  for (const item of body.items) {
    const quality = assessImportQualityCandidate(item);
    if (qualityGateEnabled && !quality.ok && !body.force) {
      skipped += 1;
      results.push({
        status: 'skipped',
        reason: 'low_quality',
        title: item.title,
        quality
      });
      continue;
    }

    const sourceType = normalizeSourceType(item.sourceType ?? baseSourceType);
    const sourceUrl = item.url?.trim() ?? null;
    const sourceExternalId = item.externalId?.trim() ?? null;
    const dedupeConditions: Prisma.QuestionWhereInput[] = [];
    if (sourceType && sourceExternalId) {
      dedupeConditions.push({ sourceType, sourceExternalId });
    }
    if (sourceUrl) {
      dedupeConditions.push({ sourceUrl });
    }
    if (dedupeConditions.length > 0) {
      const existing = await prisma.question.findFirst({
        where: { OR: dedupeConditions },
        select: { id: true, title: true }
      });
      if (existing && !body.force) {
        skipped += 1;
        results.push({
          status: 'skipped',
          reason: 'duplicate_source',
          title: item.title,
          existingId: existing.id
        });
        continue;
      }
    }
    if (!body.force) {
      const existingTitle = await prisma.question.findFirst({
        where: {
          title: {
            equals: item.title.trim(),
            mode: 'insensitive'
          }
        },
        select: { id: true }
      });
      if (existingTitle) {
        skipped += 1;
        results.push({
          status: 'skipped',
          reason: 'duplicate_title',
          title: item.title,
          existingId: existingTitle.id
        });
        continue;
      }
    }

    if (body.dryRun) {
      created += 1;
      results.push({
        status: 'dry_run',
        title: item.title,
        sourceType,
        quality
      });
      continue;
    }

    const tags = normalizeTags([...(defaultTags ?? []), ...(item.tags ?? [])]);
    const bodyText = markdownToText(item.bodyMd);
    const createdAt = item.createdAt ? new Date(item.createdAt) : undefined;
    const question = await prisma.question.create({
      data: {
        title: item.title.trim(),
        bodyMd: item.bodyMd,
        bodyText,
        userId: importer.id,
        sourceType,
        sourceUrl,
        sourceExternalId,
        sourceTitle: item.title.trim(),
        sourceImportedAt: new Date(),
        sourceImportedBy: importer.handle,
        createdAt,
        tags: tags.length > 0 ? {
          create: tags.map((name) => ({
            tag: {
              connectOrCreate: {
                where: { name },
                create: { name }
              }
            }
          }))
        } : undefined
      },
      include: {
        tags: { include: { tag: true } }
      }
    });
    created += 1;
    results.push({
      status: 'created',
      id: question.id,
      title: question.title,
      sourceType: question.sourceType ?? null,
      quality
    });

    void dispatchQuestionCreatedEvent({
      id: question.id,
      title: question.title,
      bodyText: question.bodyText,
      createdAt: question.createdAt,
      tags: question.tags.map((link) => link.tag.name),
      url: `${baseUrl}/q/${question.id}`,
      source: getQuestionSource(question)
    }).catch(() => undefined);
  }

  reply.code(200).send({
    ok: true,
    dryRun: body.dryRun === true,
    force: body.force === true,
    qualityGate: qualityGateEnabled,
    created,
    skipped,
    results
  });
});

fastify.get('/api/v1/admin/partners/teams', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }]
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const teams = await prisma.partnerTeam.findMany({
    include: {
      members: {
        where: { active: true },
        orderBy: { agentName: 'asc' }
      }
    },
    orderBy: { createdAt: 'asc' }
  });
  return teams.map((team) => ({
    id: team.id,
    name: team.name,
    displayName: team.displayName ?? null,
    description: team.description ?? null,
    active: team.active,
    targets: {
      weeklyActiveAnswerers: team.targetWeeklyActiveAnswerers ?? null,
      weeklyAcceptanceRate: team.targetWeeklyAcceptanceRate ?? null,
      weeklyRetainedAnswerers: team.targetWeeklyRetainedAnswerers ?? null,
      payoutPerAccepted: team.targetPayoutPerAccepted ?? null
    },
    members: team.members.map((member) => member.agentName),
    createdAt: team.createdAt,
    updatedAt: team.updatedAt
  }));
});

fastify.post('/api/v1/admin/partners/teams', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      required: ['name'],
      properties: {
        name: { type: 'string' },
        displayName: { type: 'string' },
        description: { type: 'string' },
        active: { type: 'boolean' },
        targetWeeklyActiveAnswerers: { type: 'integer', minimum: 0 },
        targetWeeklyAcceptanceRate: { type: 'number', minimum: 0, maximum: 1 },
        targetWeeklyRetainedAnswerers: { type: 'integer', minimum: 0 },
        targetPayoutPerAccepted: { type: 'number', minimum: 0 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({
      name: z.string().min(3).max(64),
      displayName: z.string().min(1).max(120).optional(),
      description: z.string().min(1).max(1000).optional(),
      active: z.boolean().optional(),
      targetWeeklyActiveAnswerers: z.number().int().min(0).optional(),
      targetWeeklyAcceptanceRate: z.number().min(0).max(1).optional(),
      targetWeeklyRetainedAnswerers: z.number().int().min(0).optional(),
      targetPayoutPerAccepted: z.number().min(0).optional()
    }),
    request.body,
    reply
  );
  if (!body) return;
  const name = body.name.trim().toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
  const team = await prisma.partnerTeam.upsert({
    where: { name },
    create: {
      name,
      displayName: body.displayName ?? null,
      description: body.description ?? null,
      active: body.active ?? true,
      targetWeeklyActiveAnswerers: body.targetWeeklyActiveAnswerers ?? null,
      targetWeeklyAcceptanceRate: body.targetWeeklyAcceptanceRate ?? null,
      targetWeeklyRetainedAnswerers: body.targetWeeklyRetainedAnswerers ?? null,
      targetPayoutPerAccepted: body.targetPayoutPerAccepted ?? null
    },
    update: {
      displayName: body.displayName ?? undefined,
      description: body.description ?? undefined,
      active: body.active ?? undefined,
      targetWeeklyActiveAnswerers: body.targetWeeklyActiveAnswerers ?? undefined,
      targetWeeklyAcceptanceRate: body.targetWeeklyAcceptanceRate ?? undefined,
      targetWeeklyRetainedAnswerers: body.targetWeeklyRetainedAnswerers ?? undefined,
      targetPayoutPerAccepted: body.targetPayoutPerAccepted ?? undefined
    }
  });
  reply.code(200).send(team);
});

fastify.post('/api/v1/admin/partners/teams/:id/members', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      required: ['agentNames'],
      properties: {
        agentNames: { type: 'array', items: { type: 'string' }, minItems: 1, maxItems: 200 },
        replace: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const { id } = request.params as { id: string };
  const body = parse(
    z.object({
      agentNames: z.array(z.string().min(1).max(128)).min(1).max(200),
      replace: z.boolean().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;
  const team = await prisma.partnerTeam.findUnique({ where: { id }, select: { id: true } });
  if (!team) {
    reply.code(404).send({ error: 'Team not found.' });
    return;
  }
  const agentNames = Array.from(new Set(body.agentNames.map((value) => normalizeAgentOrNull(value)).filter((value): value is string => Boolean(value))));
  if (agentNames.length === 0) {
    reply.code(400).send({ error: 'No valid agent names provided.' });
    return;
  }
  await prisma.$transaction(async (tx) => {
    if (body.replace) {
      await tx.partnerTeamMember.updateMany({
        where: { teamId: id },
        data: { active: false }
      });
    }
    for (const agentName of agentNames) {
      await tx.partnerTeamMember.upsert({
        where: { teamId_agentName: { teamId: id, agentName } },
        create: { teamId: id, agentName, active: true },
        update: { active: true }
      });
      await tx.agentProfile.upsert({
        where: { name: agentName },
        update: {},
        create: { name: agentName }
      });
    }
  });
  const members = await prisma.partnerTeamMember.findMany({
    where: { teamId: id, active: true },
    orderBy: { agentName: 'asc' }
  });
  reply.code(200).send({
    ok: true,
    teamId: id,
    members: members.map((member) => member.agentName)
  });
});

fastify.get('/api/v1/admin/partners/teams/:id/metrics/weekly', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    querystring: {
      type: 'object',
      properties: {
        weeks: { type: 'integer', minimum: 2, maximum: 26 },
        includeSynthetic: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const { id } = request.params as { id: string };
  const query = request.query as { weeks?: number; includeSynthetic?: boolean };
  const weeks = Math.min(26, Math.max(2, Number(query.weeks ?? 8)));
  const includeSynthetic = query.includeSynthetic === true;
  const team = await prisma.partnerTeam.findUnique({
    where: { id },
    include: {
      members: {
        where: { active: true },
        orderBy: { agentName: 'asc' }
      }
    }
  });
  if (!team) {
    reply.code(404).send({ error: 'Team not found.' });
    return;
  }

  const agentList = team.members
    .map((member) => normalizeAgentOrNull(member.agentName))
    .filter((value): value is string => Boolean(value))
    .filter((value) => includeSynthetic || !isSyntheticAgentName(value));
  const agentSet = new Set(agentList);
  const endWeek = startOfUtcWeek(new Date());
  const startWeek = new Date(endWeek);
  startWeek.setUTCDate(startWeek.getUTCDate() - (weeks - 1) * 7);

  if (agentList.length === 0) {
    const timeline: Array<Record<string, unknown>> = [];
    let cursor = new Date(startWeek);
    let previous = new Set<string>();
    while (cursor <= endWeek) {
      const weekStart = cursor.toISOString().slice(0, 10);
      timeline.push({
        weekStart,
        activeAnswerers: 0,
        retainedAnswerers: 0,
        answers: 0,
        accepted: 0,
        acceptanceRate: null,
        payouts: 0,
        payoutPerAccepted: null
      });
      previous = new Set<string>();
      cursor.setUTCDate(cursor.getUTCDate() + 7);
    }
    reply.code(200).send({
      team: {
        id: team.id,
        name: team.name,
        displayName: team.displayName ?? null,
        targets: {
          weeklyActiveAnswerers: team.targetWeeklyActiveAnswerers ?? null,
          weeklyAcceptanceRate: team.targetWeeklyAcceptanceRate ?? null,
          weeklyRetainedAnswerers: team.targetWeeklyRetainedAnswerers ?? null,
          payoutPerAccepted: team.targetPayoutPerAccepted ?? null
        }
      },
      weeks,
      includeSynthetic,
      agents: [],
      timeline
    });
    return;
  }

  const answerActorRows = await prisma.$queryRaw<Array<{ week: Date | string; actor: string }>>`
    SELECT
      date_trunc('week', "createdAt") AS week,
      COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId")) AS actor
    FROM "Answer"
    WHERE "createdAt" >= ${startWeek}
      AND COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId")) IN (${Prisma.join(agentList)})
    GROUP BY 1, 2
    ORDER BY 1 ASC
  `;

  const answersByWeekRows = await prisma.$queryRaw<Array<{ week: Date | string; count: bigint | number | string }>>`
    SELECT
      date_trunc('week', "createdAt") AS week,
      COUNT(*) AS count
    FROM "Answer"
    WHERE "createdAt" >= ${startWeek}
      AND COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId")) IN (${Prisma.join(agentList)})
    GROUP BY 1
    ORDER BY 1 ASC
  `;

  const acceptedByWeekRows = await prisma.$queryRaw<Array<{ week: Date | string; count: bigint | number | string }>>`
    SELECT
      date_trunc('week', qr."createdAt") AS week,
      COUNT(*) AS count
    FROM "QuestionResolution" qr
    JOIN "Answer" a ON a."id" = qr."answerId"
    WHERE qr."createdAt" >= ${startWeek}
      AND COALESCE(NULLIF(a."agentName", ''), CONCAT('user:', a."userId")) IN (${Prisma.join(agentList)})
    GROUP BY 1
    ORDER BY 1 ASC
  `;

  const payoutsByWeekRows = await prisma.$queryRaw<Array<{ week: Date | string; sum: bigint | number | string }>>`
    SELECT
      date_trunc('week', "createdAt") AS week,
      COALESCE(SUM("delta"), 0) AS sum
    FROM "AgentCreditLedger"
    WHERE "createdAt" >= ${startWeek}
      AND "agentName" IN (${Prisma.join(agentList)})
      AND "reason" IN ('bounty_payout', 'starter_bonus_first_accepted')
    GROUP BY 1
    ORDER BY 1 ASC
  `;

  const activeByWeek = new Map<string, Set<string>>();
  for (const row of answerActorRows) {
    const date = row.week instanceof Date ? row.week : new Date(row.week);
    const weekKey = date.toISOString().slice(0, 10);
    const actor = normalizeAgentOrNull(row.actor);
    if (!actor || !agentSet.has(actor)) continue;
    const set = activeByWeek.get(weekKey) ?? new Set<string>();
    set.add(actor);
    activeByWeek.set(weekKey, set);
  }

  const answersByWeek = new Map<string, number>();
  for (const row of answersByWeekRows) {
    const date = row.week instanceof Date ? row.week : new Date(row.week);
    answersByWeek.set(date.toISOString().slice(0, 10), toNumber(row.count));
  }
  const acceptedByWeek = new Map<string, number>();
  for (const row of acceptedByWeekRows) {
    const date = row.week instanceof Date ? row.week : new Date(row.week);
    acceptedByWeek.set(date.toISOString().slice(0, 10), toNumber(row.count));
  }
  const payoutsByWeek = new Map<string, number>();
  for (const row of payoutsByWeekRows) {
    const date = row.week instanceof Date ? row.week : new Date(row.week);
    payoutsByWeek.set(date.toISOString().slice(0, 10), toNumber(row.sum));
  }

  const timeline: Array<Record<string, unknown>> = [];
  let cursor = new Date(startWeek);
  let previous = new Set<string>();
  while (cursor <= endWeek) {
    const weekStart = cursor.toISOString().slice(0, 10);
    const current = activeByWeek.get(weekStart) ?? new Set<string>();
    let retained = 0;
    for (const actor of current) {
      if (previous.has(actor)) retained += 1;
    }
    const answers = answersByWeek.get(weekStart) ?? 0;
    const accepted = acceptedByWeek.get(weekStart) ?? 0;
    const payouts = payoutsByWeek.get(weekStart) ?? 0;
    timeline.push({
      weekStart,
      activeAnswerers: current.size,
      retainedAnswerers: retained,
      answers,
      accepted,
      acceptanceRate: answers > 0 ? accepted / answers : null,
      payouts,
      payoutPerAccepted: accepted > 0 ? payouts / accepted : null
    });
    previous = current;
    cursor.setUTCDate(cursor.getUTCDate() + 7);
  }

  reply.code(200).send({
    team: {
      id: team.id,
      name: team.name,
      displayName: team.displayName ?? null,
      targets: {
        weeklyActiveAnswerers: team.targetWeeklyActiveAnswerers ?? null,
        weeklyAcceptanceRate: team.targetWeeklyAcceptanceRate ?? null,
        weeklyRetainedAnswerers: team.targetWeeklyRetainedAnswerers ?? null,
        payoutPerAccepted: team.targetPayoutPerAccepted ?? null
      }
    },
    weeks,
    includeSynthetic,
    agents: agentList,
    timeline
  });
});

fastify.get('/api/v1/admin/retention/weekly', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        weeks: { type: 'integer', minimum: 2, maximum: 26 },
        includeSynthetic: { type: 'boolean' },
        agents: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { weeks?: number; includeSynthetic?: boolean; agents?: string };
  const weeks = Math.min(26, Math.max(2, Number(query.weeks ?? 8)));
  const includeSynthetic = query.includeSynthetic === true;
  const allowList = (query.agents ?? '')
    .split(',')
    .map((value) => normalizeAgentOrNull(value))
    .filter((value): value is string => Boolean(value));
  const allowSet = new Set(allowList);

  const endWeek = startOfUtcWeek(new Date());
  const startWeek = new Date(endWeek);
  startWeek.setUTCDate(startWeek.getUTCDate() - (weeks - 1) * 7);

  const rows = await prisma.$queryRaw<Array<{ week: Date | string; actor: string }>>`
    SELECT
      date_trunc('week', "createdAt") AS week,
      COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId")) AS actor
    FROM "Answer"
    WHERE "createdAt" >= ${startWeek}
    GROUP BY 1, 2
    ORDER BY 1 ASC
  `;

  const byWeek = new Map<string, Set<string>>();
  for (const row of rows) {
    const date = row.week instanceof Date ? row.week : new Date(row.week);
    const weekKey = date.toISOString().slice(0, 10);
    const actor = normalizeAgentOrNull(row.actor);
    if (!actor) continue;
    if (!includeSynthetic && isSyntheticAgentName(actor)) continue;
    if (allowSet.size > 0 && !allowSet.has(actor)) continue;
    const set = byWeek.get(weekKey) ?? new Set<string>();
    set.add(actor);
    byWeek.set(weekKey, set);
  }

  const timeline: Array<{
    weekStart: string;
    activeAnswerers: number;
    retainedFromPrevious: number;
    newAnswerers: number;
    retentionRate: number | null;
  }> = [];

  let cursor = new Date(startWeek);
  let previous = new Set<string>();
  while (cursor <= endWeek) {
    const weekKey = cursor.toISOString().slice(0, 10);
    const current = byWeek.get(weekKey) ?? new Set<string>();
    let retained = 0;
    for (const actor of current) {
      if (previous.has(actor)) retained += 1;
    }
    const newAnswerers = Array.from(current).filter((actor) => !previous.has(actor)).length;
    timeline.push({
      weekStart: weekKey,
      activeAnswerers: current.size,
      retainedFromPrevious: retained,
      newAnswerers,
      retentionRate: previous.size > 0 ? retained / previous.size : null
    });
    previous = current;
    cursor.setUTCDate(cursor.getUTCDate() + 7);
  }

  const retentionRates = timeline
    .map((row) => row.retentionRate)
    .filter((value): value is number => value != null);
  const averageRetentionRate = retentionRates.length
    ? retentionRates.reduce((acc, value) => acc + value, 0) / retentionRates.length
    : null;

  return {
    weeks,
    includeSynthetic,
    partnerAgents: allowList,
    averageRetentionRate,
    timeline
  };
});

fastify.get('/api/v1/admin/agent-events', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 500 },
        source: { type: 'string' },
        kind: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { limit?: number; source?: string; kind?: string };
  const take = Math.min(500, Math.max(1, Number(query.limit ?? 100)));
  const where: Prisma.AgentPayloadEventWhereInput = {};
  if (query.source) where.source = String(query.source);
  if (query.kind) where.kind = String(query.kind);
  reply.header('Cache-Control', 'no-store');
  return prisma.agentPayloadEvent.findMany({
    where,
    orderBy: { createdAt: 'desc' },
    take
  });
});

fastify.post('/api/v1/admin/agent-events/ingest', {
  schema: {
    hide: true,
    body: {
      type: 'object',
      required: ['source', 'kind'],
      properties: {
        source: { type: 'string' },
        kind: { type: 'string' },
        method: { type: 'string' },
        route: { type: 'string' },
        status: { type: 'integer' },
        durationMs: { type: 'integer' },
        tool: { type: 'string' },
        requestId: { type: 'string' },
        agentName: { type: 'string' },
        userAgent: { type: 'string' },
        ip: { type: 'string' },
        apiKeyPrefix: { type: 'string' },
        requestBody: {},
        responseBody: {}
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAgentEventToken(request, reply))) return;
  const body = parse(
    z.object({
      source: z.string().min(1),
      kind: z.string().min(1),
      method: z.string().optional(),
      route: z.string().optional(),
      status: z.number().int().optional(),
      durationMs: z.number().int().optional(),
      tool: z.string().optional(),
      requestId: z.string().optional(),
      agentName: z.string().optional(),
      userAgent: z.string().optional(),
      ip: z.string().optional(),
      apiKeyPrefix: z.string().optional(),
      requestBody: z.unknown().optional(),
      responseBody: z.unknown().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;
  await storeAgentPayloadEvent({
    source: body.source,
    kind: body.kind,
    method: body.method ?? null,
    route: body.route ?? null,
    status: body.status ?? null,
    durationMs: body.durationMs ?? null,
    tool: body.tool ?? null,
    requestId: body.requestId ?? null,
    agentName: body.agentName ?? null,
    userAgent: body.userAgent ?? null,
    ip: body.ip ?? null,
    apiKeyPrefix: body.apiKeyPrefix ?? null,
    requestBody: body.requestBody,
    responseBody: body.responseBody
  });
  reply.code(200).send({ ok: true });
});

fastify.post('/api/v1/admin/users', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      required: ['handle'],
      properties: {
        handle: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;

  const body = parse(
    z.object({
      handle: z.string().min(2)
    }),
    request.body,
    reply
  );
  if (!body) return;

  const user = await prisma.user.create({ data: { handle: body.handle } });
  reply.code(201).send(user);
});

fastify.post('/api/v1/admin/api-keys', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      required: ['userId', 'name'],
      properties: {
        userId: { type: 'string' },
        name: { type: 'string' },
        scopes: { type: 'array', items: { type: 'string' } },
        expiresAt: { type: 'string' },
        dailyWriteLimit: { type: 'integer' },
        dailyQuestionLimit: { type: 'integer' },
        dailyAnswerLimit: { type: 'integer' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;

  const body = parse(
    z.object({
      userId: z.string(),
      name: z.string().min(2),
      scopes: z.array(z.string()).optional(),
      expiresAt: z.string().datetime().optional(),
      dailyWriteLimit: z.number().int().min(1).optional(),
      dailyQuestionLimit: z.number().int().min(1).optional(),
      dailyAnswerLimit: z.number().int().min(1).optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const key = `a2a_${crypto.randomBytes(24).toString('hex')}`;
  const keyPrefix = key.slice(0, 8);
  const keyHash = sha256(key);
  const scopes = body.scopes?.length ? body.scopes : ['write:questions', 'write:answers'];

  const apiKey = await prisma.apiKey.create({
    data: {
      userId: body.userId,
      name: body.name,
      keyPrefix,
      keyHash,
      scopes,
      expiresAt: body.expiresAt ? new Date(body.expiresAt) : undefined,
      dailyWriteLimit: body.dailyWriteLimit,
      dailyQuestionLimit: body.dailyQuestionLimit,
      dailyAnswerLimit: body.dailyAnswerLimit
    }
  });

  reply.code(201).send({
    id: apiKey.id,
    userId: apiKey.userId,
    name: apiKey.name,
    scopes: apiKey.scopes,
    keyPrefix: apiKey.keyPrefix,
    apiKey: key
  });
});

fastify.post('/api/v1/admin/seed', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }]
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const { seedContent } = await import('./seedData.js');
  const result = await seedContent(prisma);
  reply.send(result);
});

fastify.post('/api/v1/admin/api-keys/:id/revoke', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;

  const { id } = request.params as { id: string };
  const apiKey = await prisma.apiKey.update({
    where: { id },
    data: { revokedAt: new Date() }
  });
  reply.send({ id: apiKey.id, revokedAt: apiKey.revokedAt });
});

fastify.get('/q/:id', async (request, reply) => {
  const { id } = request.params as { id: string };
  if (isPlaceholderId(id)) {
    reply.code(400).type('text/plain').send('Replace :id with a real id (try demo_q1).');
    return;
  }
  const question = await prisma.question.findUnique({
    where: { id },
    include: {
      answers: { include: { user: true }, orderBy: { createdAt: 'asc' } },
      user: true,
      resolution: true,
      bounty: true
    }
  });
  if (!question) {
    reply.code(404).type('text/plain').send('Not found');
    return;
  }
  const lines: string[] = [];
  lines.push(`# ${question.title}`);
  lines.push('');
  lines.push(`Asked by ${question.user.handle} on ${question.createdAt.toISOString()}`);
  lines.push('');
  const source = getQuestionSource(question);
  if (source) {
    lines.push(`Source: ${source.type ?? 'external'}${source.url ? ` ${source.url}` : ''}`);
    if (source.externalId) lines.push(`Source ID: ${source.externalId}`);
    lines.push('');
  }
  lines.push(question.bodyText || markdownToText(question.bodyMd));
  lines.push('');
  if (question.bounty && question.bounty.active) {
    const amount = getActiveBountyAmount(question.bounty);
    if (amount > 0) {
      lines.push(`Bounty: ${amount} ${question.bounty.currency}`);
      lines.push('');
    }
  }
  if (question.resolution?.answerId) {
    lines.push(`Accepted answer: ${question.resolution.answerId}`);
    lines.push('');
  }
  lines.push('Answers:');
  const voteMap = await getAnswerVoteMap(question.answers.map((answer) => answer.id));
  if (question.answers.length === 0) {
    lines.push('No answers yet.');
  } else {
    question.answers.forEach((answer, index) => {
      lines.push('');
      const acceptedMark = question.resolution?.answerId === answer.id ? ' [ACCEPTED]' : '';
      const voteScore = voteMap.get(answer.id) ?? 0;
      const agentLabel = answer.agentName ? ` agent:${answer.agentName}` : '';
      lines.push(`${index + 1}. ${answer.user.handle}${agentLabel} (${answer.createdAt.toISOString()}) score=${voteScore}${acceptedMark}`);
      lines.push(answer.bodyText || markdownToText(answer.bodyMd));
    });
  }
  reply.type('text/plain').send(lines.join('\n'));
});

fastify.addHook('onClose', async () => {
  await stopBackgroundWorkers();
  await prisma.$disconnect();
});

fastify.listen({ port: PORT, host: '0.0.0.0' })
  .then(() => {
    startBackgroundWorkers();
  })
  .catch((err) => {
    fastify.log.error(err);
    process.exit(1);
  });
