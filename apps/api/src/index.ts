import 'dotenv/config';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import rateLimit from '@fastify/rate-limit';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { PrismaClient, Prisma } from '@prisma/client';
import { markdownToText } from './markdown.js';
import { ANSWER_REQUEST_SCHEMA, runAnswer, createDefaultLlmFromEnv } from './answer.js';
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
const CAPTURE_AGENT_PAYLOADS = (process.env.CAPTURE_AGENT_PAYLOADS ?? '').toLowerCase() === 'true';
const AGENT_PAYLOAD_TTL_HOURS = Number(process.env.AGENT_PAYLOAD_TTL_HOURS ?? 24);
const AGENT_PAYLOAD_MAX_EVENTS = Number(process.env.AGENT_PAYLOAD_MAX_EVENTS ?? 1000);
const AGENT_PAYLOAD_MAX_BYTES = Number(process.env.AGENT_PAYLOAD_MAX_BYTES ?? 16_384);
const AGENT_EVENT_TOKEN = process.env.AGENT_EVENT_TOKEN ?? '';
const LLM_CLIENT = createDefaultLlmFromEnv();
const LLM_ENABLED = (process.env.LLM_ENABLED ?? '').toLowerCase() === 'true';
const LLM_REQUIRE_API_KEY = (process.env.LLM_REQUIRE_API_KEY ?? 'true').toLowerCase() === 'true';
const LLM_AGENT_ALLOWLIST = new Set(
  (process.env.LLM_AGENT_ALLOWLIST ?? '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
);
const LLM_DAILY_LIMIT = Number(process.env.LLM_DAILY_LIMIT ?? 50);
const llmUsage = new Map<string, { dateKey: string; count: number }>();

await fastify.register(cors, { origin: true });
await fastify.register(rateLimit, { global: false });

await fastify.register(swagger, {
  mode: 'dynamic',
  openapi: {
    info: {
      title: 'A2ABench API',
      description: 'Agent-native developer Q&A service',
      version: '0.1.25'
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

const PAYLOAD_REDACT_KEYS = ['authorization', 'apiKey', 'api_key', 'token', 'secret', 'password'];

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
    version: '0.1.25',
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
  if (!LLM_ENABLED || !LLM_CLIENT) {
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
  '/api/v1/questions/:id/answers',
  '/api/v1/search'
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

fastify.addHook('onRequest', async (request, reply) => {
  (request as { startTimeNs?: bigint }).startTimeNs = process.hrtime.bigint();
  if (request.method === 'GET') {
    const rawUrl = request.raw.url ?? request.url;
    if (rawUrl) {
      const canonicalPaths = ['/.well-known/agent.json', '/.well-known/agent-card.json'];
      for (const canonical of canonicalPaths) {
        if (rawUrl.startsWith(canonical) && rawUrl !== canonical && !rawUrl.startsWith(`${canonical}?`)) {
          reply.redirect(canonical, 301);
          return;
        }
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

  void prisma.usageEvent.create({
    data: {
      method: request.method,
      route,
      status: reply.statusCode,
      durationMs: Math.round(durationMs),
      apiKeyPrefix,
      userAgent,
      ip,
      referer,
      agentName
    }
  }).catch((err) => {
    request.log.warn({ err }, 'usage event logging failed');
  });

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
  return getUsageSummary(days, Boolean(query.includeNoise));
});

async function getUsageSummary(days: number, includeNoise: boolean) {
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
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
      { route: '/api/v1/usage/summary', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } },
      { route: '/admin/usage', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } },
      { route: '/admin/usage/data', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } }
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
        OR ("route" = '/api/v1/usage/summary' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
        OR ("route" = '/admin/usage' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
        OR ("route" = '/admin/usage/data' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
      )
    `;

  const [total, lastDay, byRoute, byStatus, byIp, byReferer, byUserAgent, byAgentName, dailyRows] = await Promise.all([
    prisma.usageEvent.count({ where: usageWhere }),
    prisma.usageEvent.count({ where: last24hWhere }),
    prisma.usageEvent.groupBy({
      by: ['route'],
      where: usageWhere,
      _count: { route: true },
      orderBy: { _count: { route: 'desc' } },
      take: 10
    }),
    prisma.usageEvent.groupBy({
      by: ['status'],
      where: usageWhere,
      _count: { status: true },
      orderBy: { _count: { status: 'desc' } }
    }),
    prisma.usageEvent.groupBy({
      by: ['ip'],
      where: { ...usageWhere, ip: { not: null } },
      _count: { ip: true },
      orderBy: { _count: { ip: 'desc' } },
      take: 10
    }),
    prisma.usageEvent.groupBy({
      by: ['referer'],
      where: { ...usageWhere, referer: { not: null } },
      _count: { referer: true },
      orderBy: { _count: { referer: 'desc' } },
      take: 10
    }),
    prisma.usageEvent.groupBy({
      by: ['userAgent'],
      where: { ...usageWhere, userAgent: { not: null } },
      _count: { userAgent: true },
      orderBy: { _count: { userAgent: 'desc' } },
      take: 10
    }),
    prisma.usageEvent.groupBy({
      by: ['agentName'],
      where: { ...usageWhere, agentName: { not: null } },
      _count: { agentName: true },
      orderBy: { _count: { agentName: 'desc' } },
      take: 10
    }),
    prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
      SELECT date_trunc('day', "createdAt") AS day, COUNT(*) AS count
      FROM "UsageEvent"
      WHERE "createdAt" >= ${since}
      ${noiseSql}
      GROUP BY 1
      ORDER BY day ASC
    `
  ]);

  const recentErrors = await prisma.usageEvent.findMany({
    where: { AND: [usageWhere, { status: { gte: 400 } }] },
    orderBy: { createdAt: 'desc' },
    take: 50,
    select: {
      createdAt: true,
      status: true,
      route: true,
      ip: true,
      referer: true,
      userAgent: true,
      agentName: true
    }
  });

  return {
    days,
    since: since.toISOString(),
    total,
    last24h: lastDay,
    byRoute: byRoute.map((row) => ({ route: row.route, count: row._count.route })),
    byStatus: byStatus.map((row) => ({ status: row.status, count: row._count.status })),
    byIp: byIp.map((row) => ({ ip: row.ip ?? 'unknown', count: row._count.ip })),
    byReferer: byReferer.map((row) => ({ referer: row.referer ?? 'unknown', count: row._count.referer })),
    byUserAgent: byUserAgent.map((row) => ({ userAgent: row.userAgent ?? 'unknown', count: row._count.userAgent })),
    byAgentName: byAgentName.map((row) => ({ agentName: row.agentName ?? 'unknown', count: row._count.agentName })),
    recentErrors: recentErrors.map((row) => ({
      createdAt: row.createdAt.toISOString(),
      status: row.status,
      route: row.route,
      ip: row.ip ?? null,
      referer: row.referer ?? null,
      userAgent: row.userAgent ?? null,
      agentName: row.agentName ?? null
    })),
    daily: dailyRows.map((row) => {
      const date = row.day instanceof Date ? row.day : new Date(row.day);
      return {
        day: date.toISOString().slice(0, 10),
        count: Number(row.count)
      };
    })
  };
}

fastify.get('/admin/usage/data', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  const query = request.query as { days?: number; includeNoise?: boolean };
  const days = Math.min(90, Math.max(1, Number(query.days ?? 7)));
  reply.header('Cache-Control', 'no-store');
  return getUsageSummary(days, Boolean(query.includeNoise));
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
      .list { display: grid; grid-template-columns: 1fr; gap: 6px; }
      .pill { display: flex; justify-content: space-between; gap: 12px; padding: 8px 10px; background: #f3f4f6; border-radius: 8px; font-size: 13px; word-break: break-word; }
      .pill span:first-child { overflow-wrap: anywhere; }
      .bar { height: 10px; background: #e5e7eb; border-radius: 999px; overflow: hidden; }
      .bar > span { display: block; height: 100%; background: #22c55e; }
      .muted { color: #6b7280; font-size: 12px; }
      .error { color: #b91c1c; font-size: 13px; margin-top: 8px; }
      .error-item { display: grid; grid-template-columns: 90px 1fr; gap: 8px 12px; padding: 10px; border-radius: 10px; background: #fff7ed; border: 1px solid #fed7aa; font-size: 12px; }
      .error-item code { background: #fff; padding: 2px 6px; border-radius: 6px; }
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
          <div class="metric"><h3>Total (range)</h3><div id="total">—</div></div>
          <div class="metric"><h3>Last 24h</h3><div id="last24h">—</div></div>
          <div class="metric"><h3>Since</h3><div id="since">—</div></div>
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
      const routesEl = document.getElementById('routes');
      const statusesEl = document.getElementById('statuses');
      const dailyEl = document.getElementById('daily');
      const ipsEl = document.getElementById('ips');
      const referrersEl = document.getElementById('referrers');
      const userAgentsEl = document.getElementById('userAgents');
      const agentNamesEl = document.getElementById('agentNames');
      const errorsEl = document.getElementById('errors');

      function setStatus(text) { statusEl.textContent = text || ''; }
      function setError(text) { errorEl.textContent = text || ''; }

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
          renderList(routesEl, data.byRoute || [], 'route', 'count');
          renderList(statusesEl, data.byStatus || [], 'status', 'count');
          renderList(dailyEl, data.daily || [], 'day', 'count');
          renderList(ipsEl, data.byIp || [], 'ip', 'count');
          renderList(referrersEl, data.byReferer || [], 'referer', 'count');
          renderList(userAgentsEl, data.byUserAgent || [], 'userAgent', 'count');
          renderList(agentNamesEl, data.byAgentName || [], 'agentName', 'count');
          renderErrors(data.recentErrors || []);
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
        page: { type: 'integer', minimum: 1 }
      }
    }
  }
}, async (request) => {
  const query = request.query as { q?: string; tag?: string; page?: number };
  const page = Math.max(1, Number(query.page ?? 1));
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
    take,
    skip,
    orderBy: { createdAt: 'desc' },
    include: {
      tags: { include: { tag: true } },
      _count: { select: { answers: true } }
    }
  });

  return {
    page,
    results: items.map((item) => ({
      id: item.id,
      title: item.title,
      bodyText: item.bodyText,
      createdAt: item.createdAt,
      updatedAt: item.updatedAt,
      tags: item.tags.map((link) => link.tag.name),
      answerCount: item._count.answers
    }))
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
  const agentName = getAgentName(request.headers as Record<string, string | string[] | undefined>);
  const policy = allowLlmForRequest(request as RouteRequest, agentName);
  let llmAllowed = policy.allowed;
  const warnings = [...policy.warnings];
  let message = policy.message;

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
    llm: llmAllowed ? LLM_CLIENT : null
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
      _count: { select: { answers: true } }
    }
  });
  return items.map((item) => ({
    id: item.id,
    title: item.title,
    bodyText: item.bodyText,
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
    tags: item.tags.map((link) => link.tag.name),
    answerCount: item._count.answers
  }));
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
      max: 3,
      timeWindow: '1 day',
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
    limits: {
      dailyWrites: apiKey.dailyWriteLimit,
      dailyQuestions: apiKey.dailyQuestionLimit,
      dailyAnswers: apiKey.dailyAnswerLimit
    }
  });
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
  if (isPlaceholderId(id)) {
    reply.code(400).send({ error: 'Replace :id with a real id (try demo_q1).' });
    return;
  }
  const question = await prisma.question.findUnique({
    where: { id },
    include: {
      tags: { include: { tag: true } },
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
  return {
    id: question.id,
    title: question.title,
    bodyMd: question.bodyMd,
    bodyText: question.bodyText,
    createdAt: question.createdAt,
    updatedAt: question.updatedAt,
    user: { id: question.user.id, handle: question.user.handle },
    tags: question.tags.map((link) => link.tag.name),
    answers: question.answers.map((answer) => ({
      id: answer.id,
      bodyMd: answer.bodyMd,
      bodyText: answer.bodyText,
      createdAt: answer.createdAt,
      updatedAt: answer.updatedAt,
      user: { id: answer.user.id, handle: answer.user.handle }
    }))
  };
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

  reply.code(201).send({
    id: question.id,
    title: question.title,
    bodyMd: question.bodyMd,
    bodyText: question.bodyText,
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

  const question = await prisma.question.findUnique({ where: { id } });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }

  const bodyText = markdownToText(body.bodyMd);
  const answer = await prisma.answer.create({
    data: {
      questionId: id,
      userId: apiKey.userId,
      bodyMd: body.bodyMd,
      bodyText
    }
  });

  reply.code(201).send({
    id: answer.id,
    bodyMd: answer.bodyMd,
    bodyText: answer.bodyText,
    createdAt: answer.createdAt,
    updatedAt: answer.updatedAt
  });
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
      user: true
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
  lines.push(question.bodyText || markdownToText(question.bodyMd));
  lines.push('');
  lines.push('Answers:');
  if (question.answers.length === 0) {
    lines.push('No answers yet.');
  } else {
    question.answers.forEach((answer, index) => {
      lines.push('');
      lines.push(`${index + 1}. ${answer.user.handle} (${answer.createdAt.toISOString()})`);
      lines.push(answer.bodyText || markdownToText(answer.bodyMd));
    });
  }
  reply.type('text/plain').send(lines.join('\n'));
});

fastify.addHook('onClose', async () => {
  await prisma.$disconnect();
});

fastify.listen({ port: PORT, host: '0.0.0.0' }).catch((err) => {
  fastify.log.error(err);
  process.exit(1);
});
