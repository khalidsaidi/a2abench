import 'dotenv/config';
import { createServer } from 'node:http';
import { AsyncLocalStorage } from 'node:async_hooks';
import { createHmac, randomUUID } from 'node:crypto';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { z } from 'zod';

const API_BASE_URL = process.env.API_BASE_URL ?? 'http://localhost:3000';
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL ?? API_BASE_URL;
const PUBLIC_MCP_URL =
  process.env.PUBLIC_MCP_URL ?? 'https://a2abench-mcp.web.app/mcp';
const API_KEY = process.env.API_KEY ?? '';
const MCP_USE_API_KEY_BY_DEFAULT = (process.env.MCP_USE_API_KEY_BY_DEFAULT ?? 'false').toLowerCase() === 'true';
const MCP_AUTO_TRIAL_KEYS = (process.env.MCP_AUTO_TRIAL_KEYS ?? 'true').toLowerCase() === 'true';
const PORT = Number(process.env.PORT ?? process.env.MCP_PORT ?? 4000);
const MCP_AGENT_NAME = process.env.MCP_AGENT_NAME ?? 'a2abench-mcp-remote';
const MCP_DERIVE_AGENT_NAME = (process.env.MCP_DERIVE_AGENT_NAME ?? 'true').toLowerCase() === 'true';
const MCP_DERIVED_AGENT_PREFIX = (process.env.MCP_DERIVED_AGENT_PREFIX ?? 'a2abench-mcp-proxy').trim().toLowerCase() || 'a2abench-mcp-proxy';
const MCP_DERIVED_AGENT_HASH_LEN = Math.max(8, Math.min(32, Number(process.env.MCP_DERIVED_AGENT_HASH_LEN ?? 16)));
const MCP_DERIVED_AGENT_SALT = process.env.MCP_DERIVED_AGENT_SALT ?? PUBLIC_MCP_URL;
const MCP_INLINE_NEXT_JOB = (process.env.MCP_INLINE_NEXT_JOB ?? 'true').toLowerCase() === 'true';
const SERVICE_VERSION = process.env.SERVICE_VERSION ?? '0.1.30';
const COMMIT_SHA = process.env.COMMIT_SHA ?? process.env.GIT_SHA ?? 'unknown';
const LOG_LEVEL = process.env.LOG_LEVEL ?? 'info';
const AGENT_SIGNATURE_SIGN_WRITES = (process.env.AGENT_SIGNATURE_SIGN_WRITES ?? 'true').toLowerCase() === 'true';
const CAPTURE_AGENT_PAYLOADS = (process.env.CAPTURE_AGENT_PAYLOADS ?? '').toLowerCase() === 'true';
const AGENT_EVENT_TOKEN = process.env.AGENT_EVENT_TOKEN ?? '';
const AGENT_EVENT_ENDPOINT =
  process.env.AGENT_EVENT_ENDPOINT ?? `${API_BASE_URL.replace(/\/$/, '')}/api/v1/admin/agent-events/ingest`;
const ALLOWED_ORIGINS = (process.env.MCP_ALLOWED_ORIGINS ?? '')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);
const TRIAL_KEY_HINT = 'Keyless writes use X-Agent-Name. Optional bearer fallback: POST /api/v1/auth/trial-key';
const TRIAL_KEY_CACHE_MS = 55 * 60 * 1000;
const trialKeyCache = new Map<string, { authHeader: string; expiresAtMs: number }>();

const requestContext = new AsyncLocalStorage<{
  agentName?: string;
  requestId?: string;
  authHeader?: string;
  llmProvider?: string;
  llmApiKey?: string;
  llmModel?: string;
  userAgent?: string;
  ip?: string;
}>();

const metrics = {
  startedAt: new Date().toISOString(),
  totalRequests: 0,
  totalToolCalls: 0,
  byStatus: new Map<number, number>(),
  byTool: new Map<string, number>(),
  toolErrors: new Map<string, number>()
};

function bumpMap<K>(map: Map<K, number>, key: K) {
  map.set(key, (map.get(key) ?? 0) + 1);
}

const LOG_LEVELS: Record<string, number> = {
  silent: 0,
  error: 1,
  warn: 2,
  info: 3,
  debug: 4
};

function shouldLog(level: 'error' | 'warn' | 'info' | 'debug') {
  const current = LOG_LEVELS[LOG_LEVEL] ?? LOG_LEVELS.info;
  return current >= LOG_LEVELS[level];
}

function logEvent(level: 'error' | 'warn' | 'info' | 'debug', payload: Record<string, unknown>) {
  if (!shouldLog(level)) return;
  console.log(
    JSON.stringify({
      level,
      ts: new Date().toISOString(),
      ...payload
    })
  );
}

function logMetricsSummary() {
  const byStatus: Record<string, number> = {};
  const byTool: Record<string, number> = {};
  const toolErrors: Record<string, number> = {};
  for (const [key, value] of metrics.byStatus) byStatus[String(key)] = value;
  for (const [key, value] of metrics.byTool) byTool[key] = value;
  for (const [key, value] of metrics.toolErrors) toolErrors[key] = value;
  logEvent('info', {
    kind: 'mcp_metrics_summary',
    startedAt: metrics.startedAt,
    totalRequests: metrics.totalRequests,
    totalToolCalls: metrics.totalToolCalls,
    byStatus,
    byTool,
    toolErrors
  });
}

function captureToolEvent(
  tool: string,
  requestBody: Record<string, unknown>,
  responseBody: Record<string, unknown>,
  status: number,
  durationMs: number
) {
  if (!CAPTURE_AGENT_PAYLOADS || !AGENT_EVENT_TOKEN) return;
  const ctx = requestContext.getStore();
  void postAgentEvent({
    source: 'mcp-remote',
    kind: 'mcp_tool',
    tool,
    status,
    durationMs,
    requestId: ctx?.requestId ?? null,
    agentName: ctx?.agentName ?? null,
    userAgent: ctx?.userAgent ?? null,
    ip: ctx?.ip ?? null,
    apiKeyPrefix: extractApiKeyPrefix(ctx?.authHeader),
    requestBody: sanitizePayload(requestBody),
    responseBody: sanitizePayload(responseBody)
  });
}

function extractApiKeyPrefix(authHeader?: string) {
  if (!authHeader) return null;
  const [scheme, ...rest] = authHeader.split(' ');
  if (!scheme || scheme.toLowerCase() !== 'bearer') return null;
  const token = rest.join(' ').trim();
  if (!token) return null;
  return token.slice(0, 8);
}

function extractBearerToken(authHeader?: string) {
  if (!authHeader) return null;
  const [scheme, ...rest] = authHeader.split(' ');
  if (!scheme || scheme.toLowerCase() !== 'bearer') return null;
  const token = rest.join(' ').trim();
  return token || null;
}

function buildWriteSignatureHeaders(authHeader: string, method: string, path: string) {
  const token = extractBearerToken(authHeader);
  if (!token) return null;
  const keyPrefix = token.slice(0, 8);
  if (!keyPrefix) return null;
  const timestamp = String(Date.now());
  const canonical = `${method.toUpperCase()}\n${path}\n${timestamp}\n${keyPrefix}`;
  const signature = createHmac('sha256', token).update(canonical).digest('hex');
  return {
    'X-Agent-Timestamp': timestamp,
    'X-Agent-Signature': signature
  };
}

function getClientIp(headers: Record<string, string | string[] | undefined>, fallback?: string | null) {
  const forwarded = headers['x-forwarded-for'];
  const forwardedValue = Array.isArray(forwarded) ? forwarded[0] : forwarded;
  if (forwardedValue) return forwardedValue.split(',')[0]?.trim();
  const realIp = headers['x-real-ip'];
  const realIpValue = Array.isArray(realIp) ? realIp[0] : realIp;
  if (realIpValue) return realIpValue;
  const cfIp = headers['cf-connecting-ip'];
  const cfIpValue = Array.isArray(cfIp) ? cfIp[0] : cfIp;
  if (cfIpValue) return cfIpValue;
  return fallback ?? null;
}

function normalizeAgentNameInput(value: string | null | undefined) {
  const normalized = (value ?? '').trim().toLowerCase();
  if (!normalized) return null;
  return normalized.slice(0, 128);
}

function sanitizeAgentLabel(value: string | null | undefined) {
  const cleaned = (value ?? '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 20);
  return cleaned || 'client';
}

function inferClientLabel(userAgent: string | null | undefined) {
  const ua = (userAgent ?? '').trim().toLowerCase();
  if (!ua) return 'client';
  if (ua.includes('claude')) return 'claude';
  if (ua.includes('openai')) return 'openai';
  if (ua.includes('cursor')) return 'cursor';
  if (ua.includes('vscode')) return 'vscode';
  if (ua.includes('copilot')) return 'copilot';
  const firstToken = ua.split(/[;()\s/]+/g).find((part) => part.trim().length >= 3) ?? ua.slice(0, 20);
  return sanitizeAgentLabel(firstToken);
}

function deriveAnonymousAgentName(input: {
  ip?: string;
  userAgent?: string;
  llmProvider?: string;
  llmModel?: string;
  authHeader?: string;
}) {
  const fingerprint = [
    (input.ip ?? '').trim().toLowerCase(),
    (input.userAgent ?? '').trim().toLowerCase(),
    (input.llmProvider ?? '').trim().toLowerCase(),
    (input.llmModel ?? '').trim().toLowerCase(),
    extractApiKeyPrefix(input.authHeader ?? '') ?? ''
  ].join('|');
  if (!fingerprint.replace(/\|/g, '')) {
    return normalizeAgentNameInput(MCP_AGENT_NAME) ?? 'a2abench-mcp-remote';
  }
  const digest = createHmac('sha256', MCP_DERIVED_AGENT_SALT).update(fingerprint).digest('hex').slice(0, MCP_DERIVED_AGENT_HASH_LEN);
  const label = inferClientLabel(input.userAgent);
  return `${MCP_DERIVED_AGENT_PREFIX}-${label}-${digest}`.slice(0, 128);
}

function resolveRequestAgentName(input: {
  presentedAgentName?: string | null;
  ip?: string;
  userAgent?: string;
  llmProvider?: string;
  llmModel?: string;
  authHeader?: string;
}) {
  const presented = normalizeAgentNameInput(input.presentedAgentName);
  if (presented) return presented;
  if (MCP_DERIVE_AGENT_NAME) {
    return deriveAnonymousAgentName({
      ip: input.ip,
      userAgent: input.userAgent,
      llmProvider: input.llmProvider,
      llmModel: input.llmModel,
      authHeader: input.authHeader
    });
  }
  return normalizeAgentNameInput(MCP_AGENT_NAME) ?? 'a2abench-mcp-remote';
}

function redactString(value: string) {
  return value.replace(/Bearer\s+[A-Za-z0-9._-]+/gi, 'Bearer [redacted]');
}

function sanitizePayload(value: unknown): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') return redactString(value);
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (Array.isArray(value)) return value.map((item) => sanitizePayload(item));
  if (typeof value === 'object') {
    const output: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
      const lower = key.toLowerCase();
      if (lower.includes('authorization') || lower.includes('token') || lower.includes('apikey') || lower.includes('secret')) {
        output[key] = '[redacted]';
      } else {
        output[key] = sanitizePayload(val);
      }
    }
    return output;
  }
  return value;
}

async function postAgentEvent(payload: Record<string, unknown>) {
  if (!CAPTURE_AGENT_PAYLOADS || !AGENT_EVENT_TOKEN) return;
  try {
    await fetch(AGENT_EVENT_ENDPOINT, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-agent-event-token': AGENT_EVENT_TOKEN
      },
      body: JSON.stringify(payload)
    });
  } catch (err) {
    logEvent('warn', { kind: 'agent_event_failed', errorName: err instanceof Error ? err.name : 'unknown' });
  }
}

async function apiGet(path: string, params?: Record<string, string>, authHeaderOverride?: string) {
  const url = new URL(path, API_BASE_URL);
  if (params) {
    Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));
  }
  const headers: Record<string, string> = { accept: 'application/json' };
  const ctxAgent = requestContext.getStore()?.agentName;
  const agentName = (ctxAgent ?? MCP_AGENT_NAME).trim();
  if (agentName) {
    headers['X-Agent-Name'] = agentName;
  }
  const ctxAuth = requestContext.getStore()?.authHeader;
  if (authHeaderOverride) {
    headers.authorization = authHeaderOverride;
  } else if (ctxAuth) {
    headers.authorization = ctxAuth;
  } else if (MCP_USE_API_KEY_BY_DEFAULT && API_KEY) {
    headers.authorization = `Bearer ${API_KEY}`;
  }
  const response = await fetch(url, { headers });
  return response;
}

async function apiPost(
  path: string,
  body: Record<string, unknown>,
  query?: Record<string, string>,
  authHeaderOverride?: string
) {
  const url = new URL(path, API_BASE_URL);
  if (query) {
    Object.entries(query).forEach(([key, value]) => url.searchParams.set(key, value));
  }
  const headers: Record<string, string> = {
    accept: 'application/json',
    'content-type': 'application/json'
  };
  const ctxAgent = requestContext.getStore()?.agentName;
  const agentName = (ctxAgent ?? MCP_AGENT_NAME).trim();
  if (agentName) {
    headers['X-Agent-Name'] = agentName;
  }
  const ctxAuth = requestContext.getStore()?.authHeader;
  if (authHeaderOverride) {
    headers.authorization = authHeaderOverride;
  } else if (ctxAuth) {
    headers.authorization = ctxAuth;
  } else if (MCP_USE_API_KEY_BY_DEFAULT && API_KEY) {
    headers.authorization = `Bearer ${API_KEY}`;
  }
  if (AGENT_SIGNATURE_SIGN_WRITES && headers.authorization) {
    const signatureHeaders = buildWriteSignatureHeaders(headers.authorization, 'POST', url.pathname);
    if (signatureHeaders) {
      Object.assign(headers, signatureHeaders);
    }
  }
  if (path === '/answer') {
    const ctx = requestContext.getStore();
    if (ctx?.llmProvider) headers['x-llm-provider'] = ctx.llmProvider;
    if (ctx?.llmApiKey) headers['x-llm-api-key'] = ctx.llmApiKey;
    if (ctx?.llmModel) headers['x-llm-model'] = ctx.llmModel;
  }
  return fetch(url, { method: 'POST', headers, body: JSON.stringify(body) });
}

function getWriteCacheKey() {
  const ctx = requestContext.getStore();
  const agentName = (ctx?.agentName ?? MCP_AGENT_NAME).trim().toLowerCase() || 'unknown';
  const ip = (ctx?.ip ?? '').trim();
  return `${agentName}|${ip}`;
}

function sanitizeHandle(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/^-+|-+$/g, '').slice(0, 32);
}

function getCachedTrialAuth() {
  const key = getWriteCacheKey();
  const entry = trialKeyCache.get(key);
  if (!entry) return null;
  if (Date.now() >= entry.expiresAtMs) {
    trialKeyCache.delete(key);
    return null;
  }
  return entry.authHeader;
}

function cacheTrialAuth(authHeader: string, expiresAt?: string | null) {
  const key = getWriteCacheKey();
  const parsedMs = expiresAt ? Date.parse(expiresAt) : Number.NaN;
  const expiresAtMs = Number.isFinite(parsedMs) ? parsedMs : Date.now() + TRIAL_KEY_CACHE_MS;
  trialKeyCache.set(key, { authHeader, expiresAtMs });
}

async function mintTrialAuthHeader(forceRefresh = false) {
  if (!MCP_AUTO_TRIAL_KEYS) return null;
  if (!forceRefresh) {
    const cached = getCachedTrialAuth();
    if (cached) return cached;
  }
  const ctxAgent = requestContext.getStore()?.agentName;
  const agentName = (ctxAgent ?? MCP_AGENT_NAME).trim();
  const handle = sanitizeHandle(agentName);
  const payload = handle.length >= 3 ? { handle } : {};
  const response = await fetch(new URL('/api/v1/auth/trial-key', API_BASE_URL), {
    method: 'POST',
    headers: {
      accept: 'application/json',
      'content-type': 'application/json',
      ...(agentName ? { 'X-Agent-Name': agentName } : {})
    },
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    throw new Error(`trial-key mint failed: ${response.status} ${await response.text()}`);
  }
  const json = (await response.json()) as { apiKey?: string; expiresAt?: string };
  if (!json.apiKey) {
    throw new Error('trial-key mint failed: missing apiKey');
  }
  const authHeader = `Bearer ${json.apiKey}`;
  cacheTrialAuth(authHeader, json.expiresAt);
  return authHeader;
}

function isAuthOrLimitFailure(status: number, bodyText: string) {
  if (status === 401) return true;
  if (status !== 429) return false;
  const lower = bodyText.toLowerCase();
  return (
    lower.includes('daily') ||
    lower.includes('limit') ||
    lower.includes('too many requests') ||
    lower.includes('rate limit')
  );
}

async function postWriteWithAutoTrial(
  path: string,
  body: Record<string, unknown>,
  query?: Record<string, string>
) {
  const requestAuth = requestContext.getStore()?.authHeader?.trim() ?? '';
  let authHeader = requestAuth || (MCP_USE_API_KEY_BY_DEFAULT && API_KEY ? `Bearer ${API_KEY}` : '');
  let usedTrialKey = false;
  let response = await apiPost(path, body, query, authHeader || undefined);
  const failureText = await response.clone().text();
  if (isAuthOrLimitFailure(response.status, failureText) && MCP_AUTO_TRIAL_KEYS) {
    const shouldForceRefresh = usedTrialKey || response.status === 429;
    try {
      const minted = await mintTrialAuthHeader(shouldForceRefresh);
      if (minted && (minted !== authHeader || shouldForceRefresh)) {
        authHeader = minted;
        usedTrialKey = true;
        response = await apiPost(path, body, query, authHeader);
      }
    } catch (error) {
      logEvent('warn', {
        kind: 'trial_key_retry_failed',
        status: response.status,
        error: error instanceof Error ? error.message : 'unknown'
      });
    }
  }

  return { response, usedTrialKey };
}

async function getProtectedWithAutoTrial(path: string, query?: Record<string, string>) {
  const requestAuth = requestContext.getStore()?.authHeader?.trim() ?? '';
  let authHeader = requestAuth || (MCP_USE_API_KEY_BY_DEFAULT && API_KEY ? `Bearer ${API_KEY}` : '');
  let usedTrialKey = false;
  let response = await apiGet(path, query, authHeader || undefined);
  const failureText = await response.clone().text();
  if (isAuthOrLimitFailure(response.status, failureText) && MCP_AUTO_TRIAL_KEYS) {
    const shouldForceRefresh = usedTrialKey || response.status === 429;
    try {
      const minted = await mintTrialAuthHeader(shouldForceRefresh);
      if (minted && (minted !== authHeader || shouldForceRefresh)) {
        authHeader = minted;
        usedTrialKey = true;
        response = await apiGet(path, query, authHeader);
      }
    } catch (error) {
      logEvent('warn', {
        kind: 'trial_key_retry_failed',
        status: response.status,
        error: error instanceof Error ? error.message : 'unknown'
      });
    }
  }

  return { response, usedTrialKey };
}

async function getInlineNextJobSuggestion() {
  if (!MCP_INLINE_NEXT_JOB) return null;
  try {
    const response = await apiGet('/api/v1/agent/jobs/next');
    if (!response.ok) return null;
    const data = (await response.json()) as Record<string, unknown>;
    const nextJob = (data.nextJob && typeof data.nextJob === 'object')
      ? (data.nextJob as Record<string, unknown>)
      : null;
    if (nextJob) {
      const question = (nextJob.question && typeof nextJob.question === 'object')
        ? (nextJob.question as Record<string, unknown>)
        : null;
      if (!question || typeof question.id !== 'string' || !question.id.trim()) return null;
      const answerJobRequest = (nextJob.answerJobRequest && typeof nextJob.answerJobRequest === 'object')
        ? (nextJob.answerJobRequest as Record<string, unknown>)
        : null;
      const subscription = (data.onboarding && typeof data.onboarding === 'object')
        ? (data.onboarding as Record<string, unknown>)
        : null;
      return {
        question: {
          id: String(question.id),
          title: typeof question.title === 'string' ? question.title : '',
          url: typeof question.url === 'string' ? question.url : `${PUBLIC_BASE_URL}/q/${String(question.id)}`
        },
        answerJobRequest: answerJobRequest ?? null,
        subscription
      };
    }

    const recommended = (data.recommended && typeof data.recommended === 'object')
      ? (data.recommended as Record<string, unknown>)
      : null;
    if (!recommended || typeof recommended.id !== 'string' || !recommended.id.trim()) return null;
    const answerJobRequest = (data.answerJobRequest && typeof data.answerJobRequest === 'object')
      ? (data.answerJobRequest as Record<string, unknown>)
      : null;
    const subscription = (data.subscription && typeof data.subscription === 'object')
      ? (data.subscription as Record<string, unknown>)
      : null;
    return {
      question: {
        id: String(recommended.id),
        title: typeof recommended.title === 'string' ? recommended.title : '',
        url: typeof recommended.url === 'string' ? recommended.url : `${PUBLIC_BASE_URL}/q/${String(recommended.id)}`
      },
      answerJobRequest: answerJobRequest ?? null,
      subscription: subscription ?? null
    };
  } catch {
    return null;
  }
}

function createMcpServer() {
  return new McpServer({
    name: 'A2ABench',
    version: SERVICE_VERSION
  });
}

function registerTools(server: McpServer) {
  server.registerTool(
    'search',
    {
      title: 'Search questions',
      description: 'Search questions by keyword and return canonical URLs.',
      inputSchema: {
        query: z.string().min(1)
      }
    },
    async ({ query }) => {
      const toolStart = Date.now();
      const response = await apiGet('/api/v1/search', { q: query });
      const requestId = requestContext.getStore()?.requestId ?? null;
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'search');
        bumpMap(metrics.toolErrors, 'search');
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'search',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent('search', { query }, { results: [] }, response.status, Date.now() - toolStart);
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ results: [] })
            }
          ]
        };
      }
      const data = (await response.json()) as { results?: Array<{ id: string; title: string }> };
      const results = (data.results ?? []).map((item) => ({
        id: item.id,
        title: item.title,
        url: `${PUBLIC_BASE_URL}/q/${item.id}`
      }));
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'search');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'search',
        status: response.status,
        durationMs: Date.now() - toolStart,
        resultCount: results.length,
        requestId
      });
      captureToolEvent('search', { query }, { results }, response.status, Date.now() - toolStart);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({ results })
          }
        ]
      };
    }
  );

  server.registerTool(
    'quickstart',
    {
      title: 'Agent quickstart',
      description: 'Get immediate demand summary and the best open question to answer next.',
      inputSchema: {
        agentName: z.string().min(1).optional()
      }
    },
    async ({ agentName }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const params: Record<string, string> = {};
      if (agentName) params.agentName = agentName;
      const response = await apiGet('/api/v1/agent/quickstart', params);
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'quickstart');
        bumpMap(metrics.toolErrors, 'quickstart');
        const text = await response.text();
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'quickstart',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent('quickstart', { agentName }, { error: text || 'Failed to load quickstart' }, response.status, Date.now() - toolStart);
        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: text || 'Failed to load quickstart', status: response.status })
            }
          ]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'quickstart');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'quickstart',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('quickstart', { agentName }, data, response.status, Date.now() - toolStart);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(data)
          }
        ]
      };
    }
  );

  server.registerTool(
    'next_best_job',
    {
      title: 'Next best job',
      description: 'Get a personalized, scored next question to answer.',
      inputSchema: {
        agentName: z.string().min(1).optional()
      }
    },
    async ({ agentName }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const params: Record<string, string> = {};
      if (agentName) params.agentName = agentName;
      const response = await apiGet('/api/v1/agent/next-best-job', params);
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'next_best_job');
        bumpMap(metrics.toolErrors, 'next_best_job');
        const text = await response.text();
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'next_best_job',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent('next_best_job', { agentName }, { error: text || 'Failed to load next best job' }, response.status, Date.now() - toolStart);
        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: text || 'Failed to load next best job', status: response.status })
            }
          ]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'next_best_job');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'next_best_job',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('next_best_job', { agentName }, data, response.status, Date.now() - toolStart);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(data)
          }
        ]
      };
    }
  );

  server.registerTool(
    'fetch',
    {
      title: 'Fetch question thread',
      description: 'Fetch a question and its answers by id.',
      inputSchema: {
        id: z.string().min(1)
      }
    },
    async ({ id }) => {
      const toolStart = Date.now();
      const response = await apiGet(`/api/v1/questions/${id}`);
      const requestId = requestContext.getStore()?.requestId ?? null;
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'fetch');
        bumpMap(metrics.toolErrors, 'fetch');
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'fetch',
          status: response.status,
          durationMs: Date.now() - toolStart,
          id,
          requestId
        });
        captureToolEvent('fetch', { id }, { error: 'Not found' }, response.status, Date.now() - toolStart);
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ id, error: 'Not found' })
            }
          ]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'fetch');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'fetch',
        status: response.status,
        durationMs: Date.now() - toolStart,
        id,
        requestId
      });
      captureToolEvent('fetch', { id }, data as Record<string, unknown>, response.status, Date.now() - toolStart);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(data)
          }
        ]
      };
    }
  );

  server.registerTool(
    'answer',
    {
      title: 'Answer',
      description: 'Synthesize a grounded answer from A2ABench threads with citations.',
      inputSchema: {
        query: z.string().min(1),
        top_k: z.number().int().min(1).max(10).optional(),
        include_evidence: z.boolean().optional(),
        mode: z.enum(['balanced', 'strict']).optional(),
        max_chars_per_evidence: z.number().int().min(200).max(4000).optional()
      }
    },
    async ({ query, top_k, include_evidence, mode, max_chars_per_evidence }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const response = await apiPost('/answer', { query, top_k, include_evidence, mode, max_chars_per_evidence });
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'answer');
        bumpMap(metrics.toolErrors, 'answer');
        const text = await response.text();
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'answer',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent(
          'answer',
          { query, top_k, include_evidence, mode, max_chars_per_evidence },
          { error: text || 'Failed to generate answer', status: response.status },
          response.status,
          Date.now() - toolStart
        );
        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: text || 'Failed to generate answer', status: response.status })
            }
          ]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'answer');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'answer',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent(
        'answer',
        { query, top_k, include_evidence, mode, max_chars_per_evidence },
        data as Record<string, unknown>,
        response.status,
        Date.now() - toolStart
      );
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(data)
          }
        ]
      };
    }
  );

  server.registerTool(
    'create_question',
    {
      title: 'Create question',
      description: 'Create a new question thread (keyless by default; optional trial fallback).',
      inputSchema: {
        title: z.string().min(8),
        bodyMd: z.string().min(3),
        tags: z.array(z.string()).optional(),
        force: z.boolean().optional()
      }
    },
    async ({ title, bodyMd, tags, force }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const writeResult = await postWriteWithAutoTrial(
        '/api/v1/questions',
        { title, bodyMd, tags },
        force ? { force: '1' } : undefined
      );
      const response = writeResult.response;
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'create_question');
        bumpMap(metrics.toolErrors, 'create_question');
        const text = await response.text();
        const hint = response.status === 401 ? TRIAL_KEY_HINT : undefined;
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'create_question',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent(
          'create_question',
          { title, bodyMd, tags, force },
          { error: text || 'Failed to create question', status: response.status, hint },
          response.status,
          Date.now() - toolStart
        );
        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: text || 'Failed to create question', status: response.status, hint })
            }
          ]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      const inlineNextJob = await getInlineNextJobSuggestion();
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'create_question');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'create_question',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('create_question', { title, bodyMd, tags, force }, data as Record<string, unknown>, response.status, Date.now() - toolStart);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              ...data,
              auth: writeResult.usedTrialKey
                ? 'trial_key'
                : ((requestContext.getStore()?.authHeader || (MCP_USE_API_KEY_BY_DEFAULT && API_KEY)) ? 'provided_key' : 'keyless_managed'),
              url: `${PUBLIC_BASE_URL}/q/${data.id}`,
              nextJob: inlineNextJob
            })
          }
        ]
      };
    }
  );

  server.registerTool(
    'create_answer',
    {
      title: 'Create answer',
      description: 'Create an answer for a question (keyless by default; optional trial fallback).',
      inputSchema: {
        id: z.string().min(1),
        bodyMd: z.string().min(3)
      }
    },
    async ({ id, bodyMd }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const writeResult = await postWriteWithAutoTrial(`/api/v1/questions/${id}/answers`, { bodyMd });
      const response = writeResult.response;
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'create_answer');
        bumpMap(metrics.toolErrors, 'create_answer');
        const text = await response.text();
        const hint = response.status === 401 ? TRIAL_KEY_HINT : undefined;
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'create_answer',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent(
          'create_answer',
          { id, bodyMd },
          { error: text || 'Failed to create answer', status: response.status, hint },
          response.status,
          Date.now() - toolStart
        );
        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: text || 'Failed to create answer', status: response.status, hint })
            }
          ]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      const inlineNextJob = await getInlineNextJobSuggestion();
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'create_answer');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'create_answer',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('create_answer', { id, bodyMd }, data as Record<string, unknown>, response.status, Date.now() - toolStart);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              ...data,
              auth: writeResult.usedTrialKey
                ? 'trial_key'
                : ((requestContext.getStore()?.authHeader || (MCP_USE_API_KEY_BY_DEFAULT && API_KEY)) ? 'provided_key' : 'keyless_managed'),
              url: `${PUBLIC_BASE_URL}/q/${id}`,
              nextJob: inlineNextJob
            })
          }
        ]
      };
    }
  );

  server.registerTool(
    'answer_job',
    {
      title: 'Answer job',
      description: 'One-step flow: claim, submit, and verify completion (with optional immediate acceptance).',
      inputSchema: {
        questionId: z.string().min(1),
        bodyMd: z.string().min(3),
        ttlMinutes: z.number().int().min(5).max(240).optional(),
        forceTakeover: z.boolean().optional(),
        acceptToken: z.string().max(4000).optional(),
        acceptIfOwner: z.boolean().optional(),
        autoVerify: z.boolean().optional()
      }
    },
    async ({ questionId, bodyMd, ttlMinutes, forceTakeover, acceptToken, acceptIfOwner, autoVerify }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const payload: Record<string, unknown> = { bodyMd };
      if (ttlMinutes !== undefined) payload.ttlMinutes = ttlMinutes;
      if (forceTakeover !== undefined) payload.forceTakeover = forceTakeover;
      if (acceptToken !== undefined) payload.acceptToken = acceptToken;
      if (acceptIfOwner !== undefined) payload.acceptIfOwner = acceptIfOwner;
      if (autoVerify !== undefined) payload.autoVerify = autoVerify;
      const writeResult = await postWriteWithAutoTrial(`/api/v1/questions/${questionId}/answer-job`, payload);
      const response = writeResult.response;
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'answer_job');
        bumpMap(metrics.toolErrors, 'answer_job');
        const text = await response.text();
        const hint = response.status === 401 ? TRIAL_KEY_HINT : undefined;
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'answer_job',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent(
          'answer_job',
          { questionId, bodyMd, ttlMinutes, forceTakeover, acceptToken, acceptIfOwner, autoVerify },
          { error: text || 'Failed to complete answer job', status: response.status, hint },
          response.status,
          Date.now() - toolStart
        );
        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: text || 'Failed to complete answer job', status: response.status, hint })
            }
          ]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      const inlineNextJob = await getInlineNextJobSuggestion();
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'answer_job');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'answer_job',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('answer_job', { questionId, bodyMd, ttlMinutes, forceTakeover, acceptToken, acceptIfOwner, autoVerify }, data, response.status, Date.now() - toolStart);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              ...data,
              auth: writeResult.usedTrialKey
                ? 'trial_key'
                : ((requestContext.getStore()?.authHeader || (MCP_USE_API_KEY_BY_DEFAULT && API_KEY)) ? 'provided_key' : 'keyless_managed'),
              url: `${PUBLIC_BASE_URL}/q/${questionId}`,
              nextJob: inlineNextJob
            })
          }
        ]
      };
    }
  );

  server.registerTool(
    'claim_question',
    {
      title: 'Claim question',
      description: 'Claim a question before answering (keyless by default; optional trial fallback).',
      inputSchema: {
        questionId: z.string().min(1),
        ttlMinutes: z.number().int().min(5).max(240).optional()
      }
    },
    async ({ questionId, ttlMinutes }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const body: Record<string, unknown> = {};
      if (ttlMinutes !== undefined) body.ttlMinutes = ttlMinutes;
      const writeResult = await postWriteWithAutoTrial(`/api/v1/questions/${questionId}/claim`, body);
      const response = writeResult.response;
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'claim_question');
        bumpMap(metrics.toolErrors, 'claim_question');
        const text = await response.text();
        const hint = response.status === 401 ? TRIAL_KEY_HINT : undefined;
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'claim_question',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent(
          'claim_question',
          { questionId, ttlMinutes },
          { error: text || 'Failed to claim question', status: response.status, hint },
          response.status,
          Date.now() - toolStart
        );
        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: text || 'Failed to claim question', status: response.status, hint })
            }
          ]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'claim_question');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'claim_question',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('claim_question', { questionId, ttlMinutes }, data, response.status, Date.now() - toolStart);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              ...data,
              auth: writeResult.usedTrialKey
                ? 'trial_key'
                : ((requestContext.getStore()?.authHeader || (MCP_USE_API_KEY_BY_DEFAULT && API_KEY)) ? 'provided_key' : 'keyless_managed')
            })
          }
        ]
      };
    }
  );

  server.registerTool(
    'release_claim',
    {
      title: 'Release claim',
      description: 'Release a question claim you currently hold (keyless by default; optional trial fallback).',
      inputSchema: {
        questionId: z.string().min(1),
        claimId: z.string().min(1)
      }
    },
    async ({ questionId, claimId }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const writeResult = await postWriteWithAutoTrial(`/api/v1/questions/${questionId}/claims/${claimId}/release`, {});
      const response = writeResult.response;
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'release_claim');
        bumpMap(metrics.toolErrors, 'release_claim');
        const text = await response.text();
        const hint = response.status === 401 ? TRIAL_KEY_HINT : undefined;
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'release_claim',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent(
          'release_claim',
          { questionId, claimId },
          { error: text || 'Failed to release claim', status: response.status, hint },
          response.status,
          Date.now() - toolStart
        );
        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: text || 'Failed to release claim', status: response.status, hint })
            }
          ]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'release_claim');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'release_claim',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('release_claim', { questionId, claimId }, data, response.status, Date.now() - toolStart);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              ...data,
              auth: writeResult.usedTrialKey
                ? 'trial_key'
                : ((requestContext.getStore()?.authHeader || (MCP_USE_API_KEY_BY_DEFAULT && API_KEY)) ? 'provided_key' : 'keyless_managed')
            })
          }
        ]
      };
    }
  );

  server.registerTool(
    'pending_acceptance',
    {
      title: 'Pending acceptance',
      description: 'List your open questions with answers that still need acceptance.',
      inputSchema: {
        limit: z.number().int().min(1).max(100).optional(),
        minAnswerAgeMinutes: z.number().int().min(0).max(10080).optional()
      }
    },
    async ({ limit, minAnswerAgeMinutes }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const params: Record<string, string> = {};
      if (limit) params.limit = String(limit);
      if (minAnswerAgeMinutes !== undefined) params.minAnswerAgeMinutes = String(minAnswerAgeMinutes);
      const readResult = await getProtectedWithAutoTrial('/api/v1/questions/pending-acceptance', params);
      const response = readResult.response;
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'pending_acceptance');
        bumpMap(metrics.toolErrors, 'pending_acceptance');
        const text = await response.text();
        const hint = response.status === 401 ? TRIAL_KEY_HINT : undefined;
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'pending_acceptance',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent(
          'pending_acceptance',
          { limit, minAnswerAgeMinutes },
          { error: text || 'Failed to fetch pending acceptance queue', status: response.status, hint },
          response.status,
          Date.now() - toolStart
        );
        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: text || 'Failed to fetch pending acceptance queue', status: response.status, hint })
            }
          ]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'pending_acceptance');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'pending_acceptance',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('pending_acceptance', { limit, minAnswerAgeMinutes }, data, response.status, Date.now() - toolStart);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              ...data,
              auth: readResult.usedTrialKey
                ? 'trial_key'
                : ((requestContext.getStore()?.authHeader || (MCP_USE_API_KEY_BY_DEFAULT && API_KEY)) ? 'provided_key' : 'keyless_managed')
            })
          }
        ]
      };
    }
  );

  server.registerTool(
    'unanswered',
    {
      title: 'Unanswered queue',
      description: 'List unanswered questions, prioritized by bounty.',
      inputSchema: {
        tag: z.string().optional(),
        page: z.number().int().min(1).optional(),
        limit: z.number().int().min(1).max(100).optional()
      }
    },
    async ({ tag, page, limit }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const params: Record<string, string> = {};
      if (tag) params.tag = tag;
      if (page) params.page = String(page);
      if (limit) params.limit = String(limit);
      const response = await apiGet('/api/v1/questions/unanswered', params);
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'unanswered');
        bumpMap(metrics.toolErrors, 'unanswered');
        const text = await response.text();
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'unanswered',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent('unanswered', { tag, page, limit }, { error: text || 'Failed to fetch unanswered queue', status: response.status }, response.status, Date.now() - toolStart);
        return {
          isError: true,
          content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to fetch unanswered queue', status: response.status }) }]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'unanswered');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'unanswered',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('unanswered', { tag, page, limit }, data, response.status, Date.now() - toolStart);
      return {
        content: [{ type: 'text', text: JSON.stringify(data) }]
      };
    }
  );

  server.registerTool(
    'leaderboard',
    {
      title: 'Agent leaderboard',
      description: 'List top answering agents by reputation.',
      inputSchema: {
        limit: z.number().int().min(1).max(100).optional()
      }
    },
    async ({ limit }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const response = await apiGet('/api/v1/agents/leaderboard', limit ? { limit: String(limit) } : undefined);
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'leaderboard');
        bumpMap(metrics.toolErrors, 'leaderboard');
        const text = await response.text();
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'leaderboard',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent('leaderboard', { limit }, { error: text || 'Failed to fetch leaderboard', status: response.status }, response.status, Date.now() - toolStart);
        return {
          isError: true,
          content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to fetch leaderboard', status: response.status }) }]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'leaderboard');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'leaderboard',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('leaderboard', { limit }, data, response.status, Date.now() - toolStart);
      return {
        content: [{ type: 'text', text: JSON.stringify(data) }]
      };
    }
  );

  server.registerTool(
    'place_bounty',
    {
      title: 'Place bounty',
      description: 'Set or update bounty for a question.',
      inputSchema: {
        id: z.string().min(1),
        amount: z.number().int().min(1).max(100000),
        expiresAt: z.string().optional(),
        active: z.boolean().optional()
      }
    },
    async ({ id, amount, expiresAt, active }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const writeResult = await postWriteWithAutoTrial(`/api/v1/questions/${id}/bounty`, { amount, expiresAt, active });
      const response = writeResult.response;
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'place_bounty');
        bumpMap(metrics.toolErrors, 'place_bounty');
        const text = await response.text();
        const hint = response.status === 401 ? TRIAL_KEY_HINT : undefined;
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'place_bounty',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent('place_bounty', { id, amount, expiresAt, active }, { error: text || 'Failed to place bounty', status: response.status, hint }, response.status, Date.now() - toolStart);
        return {
          isError: true,
          content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to place bounty', status: response.status, hint }) }]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'place_bounty');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'place_bounty',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('place_bounty', { id, amount, expiresAt, active }, data, response.status, Date.now() - toolStart);
      if (writeResult.usedTrialKey) {
        data.authMode = 'trial_fallback';
      }
      return {
        content: [{ type: 'text', text: JSON.stringify(data) }]
      };
    }
  );

  server.registerTool(
    'vote_answer',
    {
      title: 'Vote answer',
      description: 'Vote +1 or -1 on an answer.',
      inputSchema: {
        id: z.string().min(1),
        value: z.union([z.literal(1), z.literal(-1)])
      }
    },
    async ({ id, value }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const writeResult = await postWriteWithAutoTrial(`/api/v1/answers/${id}/vote`, { value });
      const response = writeResult.response;
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'vote_answer');
        bumpMap(metrics.toolErrors, 'vote_answer');
        const text = await response.text();
        const hint = response.status === 401 ? TRIAL_KEY_HINT : undefined;
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'vote_answer',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent('vote_answer', { id, value }, { error: text || 'Failed to vote answer', status: response.status, hint }, response.status, Date.now() - toolStart);
        return {
          isError: true,
          content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to vote answer', status: response.status, hint }) }]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'vote_answer');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'vote_answer',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('vote_answer', { id, value }, data, response.status, Date.now() - toolStart);
      if (writeResult.usedTrialKey) {
        data.authMode = 'trial_fallback';
      }
      return {
        content: [{ type: 'text', text: JSON.stringify(data) }]
      };
    }
  );

  server.registerTool(
    'accept_answer',
    {
      title: 'Accept answer',
      description: 'Accept an answer for a question (must be question owner identity).',
      inputSchema: {
        questionId: z.string().min(1),
        answerId: z.string().min(1)
      }
    },
    async ({ questionId, answerId }) => {
      const toolStart = Date.now();
      const requestId = requestContext.getStore()?.requestId ?? null;
      const writeResult = await postWriteWithAutoTrial(`/api/v1/questions/${questionId}/accept/${answerId}`, {});
      const response = writeResult.response;
      if (!response.ok) {
        metrics.totalToolCalls += 1;
        bumpMap(metrics.byTool, 'accept_answer');
        bumpMap(metrics.toolErrors, 'accept_answer');
        const text = await response.text();
        const hint = response.status === 401 ? TRIAL_KEY_HINT : undefined;
        logEvent('warn', {
          kind: 'mcp_tool',
          tool: 'accept_answer',
          status: response.status,
          durationMs: Date.now() - toolStart,
          requestId
        });
        captureToolEvent('accept_answer', { questionId, answerId }, { error: text || 'Failed to accept answer', status: response.status, hint }, response.status, Date.now() - toolStart);
        return {
          isError: true,
          content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to accept answer', status: response.status, hint }) }]
        };
      }
      const data = (await response.json()) as Record<string, unknown>;
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'accept_answer');
      logEvent('info', {
        kind: 'mcp_tool',
        tool: 'accept_answer',
        status: response.status,
        durationMs: Date.now() - toolStart,
        requestId
      });
      captureToolEvent('accept_answer', { questionId, answerId }, data, response.status, Date.now() - toolStart);
      if (writeResult.usedTrialKey) {
        data.authMode = 'trial_fallback';
      }
      return {
        content: [{ type: 'text', text: JSON.stringify(data) }]
      };
    }
  );
}

function isOriginAllowed(origin: string | undefined) {
  if (!origin) return true;
  if (ALLOWED_ORIGINS.length === 0) return false;
  return ALLOWED_ORIGINS.includes(origin);
}

function applyCors(res: import('node:http').ServerResponse, origin: string | undefined) {
  if (!origin) return;
  if (!isOriginAllowed(origin)) return;
  res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, mcp-session-id, mcp-protocol-version, last-event-id');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,HEAD,OPTIONS');
}

async function checkApiHealth() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 3000);
  try {
    const response = await fetch(new URL('/api/v1/health', API_BASE_URL), {
      signal: controller.signal,
      headers: { accept: 'application/json' }
    });
    clearTimeout(timeout);
    return response.ok;
  } catch {
    clearTimeout(timeout);
    return false;
  }
}

function respondJson(res: import('node:http').ServerResponse, status: number, payload: Record<string, unknown>) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(payload));
}

function respondText(res: import('node:http').ServerResponse, status: number, text: string) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  res.end(text);
}

async function main() {
  const httpServer = createServer(async (req, res) => {
    if (!req.url || !req.method) {
      res.statusCode = 400;
      res.end('Bad request');
      return;
    }

    const origin = req.headers.origin as string | undefined;
    applyCors(res, origin);

    const agentHeader = req.headers['x-agent-name'] ?? req.headers['x-mcp-client-name'] ?? req.headers['mcp-client-name'];
    const presentedAgentName = Array.isArray(agentHeader) ? agentHeader[0] : agentHeader;
    const authHeader = Array.isArray(req.headers.authorization)
      ? req.headers.authorization[0]
      : req.headers.authorization;
    const llmProvider = Array.isArray(req.headers['x-llm-provider'])
      ? req.headers['x-llm-provider'][0]
      : req.headers['x-llm-provider'];
    const llmApiKey = Array.isArray(req.headers['x-llm-api-key'])
      ? req.headers['x-llm-api-key'][0]
      : req.headers['x-llm-api-key'];
    const llmModel = Array.isArray(req.headers['x-llm-model'])
      ? req.headers['x-llm-model'][0]
      : req.headers['x-llm-model'];
    const userAgent = Array.isArray(req.headers['user-agent']) ? req.headers['user-agent'][0] : req.headers['user-agent'];
    const ip = getClientIp(req.headers as Record<string, string | string[] | undefined>, req.socket?.remoteAddress ?? null) ?? undefined;
    const resolvedAgentName = resolveRequestAgentName({
      presentedAgentName,
      ip,
      userAgent: userAgent ?? undefined,
      llmProvider: llmProvider ?? undefined,
      llmModel: llmModel ?? undefined,
      authHeader: authHeader ?? undefined
    });
    const startMs = Date.now();
    const requestId =
      (Array.isArray(req.headers['x-request-id']) ? req.headers['x-request-id'][0] : req.headers['x-request-id']) ??
      randomUUID();
    const rawUrl = req.url;
    const rawPath = rawUrl?.split('?')[0];
    const pathname = (() => {
      try {
        return new URL(req.url, 'http://localhost').pathname;
      } catch {
        return req.url;
      }
    })();

    const isMcpPath = pathname.startsWith('/mcp');
    if (req.method === 'OPTIONS') {
      res.statusCode = 200;
      res.end();
      return;
    }

    if (!isOriginAllowed(origin)) {
      respondText(res, 403, 'Origin not allowed');
      return;
    }

    if (pathname === '/.well-known/glama.json') {
      respondJson(res, 200, {
        $schema: 'https://glama.ai/mcp/schemas/connector.json',
        maintainers: [
          {
            email: 'khalidsaidi66@gmail.com'
          }
        ]
      });
      metrics.totalRequests += 1;
      bumpMap(metrics.byStatus, res.statusCode);
      logEvent('info', {
        kind: 'mcp_request',
        method: req.method,
        status: res.statusCode,
        durationMs: Date.now() - startMs,
        requestId,
        agentName: resolvedAgentName ?? null,
        presentedAgentName: presentedAgentName ?? null,
        userAgent: userAgent ?? null,
        path: '/.well-known/glama.json'
      });
      return;
    }

    const isHealthPath =
      rawPath === '/health' ||
      rawPath === '/health/' ||
      pathname === '/health' ||
      pathname.startsWith('/health/') ||
      rawPath === '/healthz/' ||
      pathname === '/healthz/' ||
      pathname.startsWith('/healthz/');

    if (isHealthPath) {
      respondJson(res, 200, {
        status: 'ok',
        version: SERVICE_VERSION,
        commit: COMMIT_SHA,
        uptimeMs: Math.round(process.uptime() * 1000)
      });
      metrics.totalRequests += 1;
      bumpMap(metrics.byStatus, res.statusCode);
      logEvent('info', {
        kind: 'mcp_request',
        method: req.method,
        status: res.statusCode,
        durationMs: Date.now() - startMs,
        requestId,
        agentName: resolvedAgentName ?? null,
        presentedAgentName: presentedAgentName ?? null,
        userAgent: userAgent ?? null,
        path: rawPath
      });
      return;
    }

    const isReadyPath =
      rawPath === '/ready' ||
      rawPath === '/ready/' ||
      pathname === '/ready' ||
      pathname.startsWith('/ready/') ||
      rawPath === '/readyz' ||
      rawPath === '/readyz/' ||
      pathname === '/readyz' ||
      pathname.startsWith('/readyz/');

    if (isReadyPath) {
      const ok = await checkApiHealth();
      if (ok) {
        respondJson(res, 200, { status: 'ok' });
      } else {
        respondJson(res, 503, { status: 'error', reason: 'api_unreachable' });
      }
      metrics.totalRequests += 1;
      bumpMap(metrics.byStatus, res.statusCode);
      logEvent(ok ? 'info' : 'warn', {
        kind: 'mcp_request',
        method: req.method,
        status: res.statusCode,
        durationMs: Date.now() - startMs,
        requestId,
        agentName: resolvedAgentName ?? null,
        presentedAgentName: presentedAgentName ?? null,
        userAgent: userAgent ?? null,
        path: rawPath
      });
      return;
    }

    if (rawPath === '/' || pathname === '/') {
      const baseUrl = (() => {
        try {
          return new URL(PUBLIC_MCP_URL).origin;
        } catch {
          return PUBLIC_MCP_URL.replace(/\/mcp$/, '');
        }
      })();
      const agentUrl = `${PUBLIC_BASE_URL.replace(/\/$/, '')}/.well-known/agent.json`;
      if (req.method === 'HEAD') {
        res.statusCode = 200;
        res.end();
      } else if (req.method !== 'GET') {
        res.statusCode = 405;
        res.end('Method not allowed');
      } else {
        const accept = Array.isArray(req.headers.accept) ? req.headers.accept.join(',') : req.headers.accept ?? '';
        if (accept.includes('text/html')) {
          res.setHeader('Content-Type', 'text/html; charset=utf-8');
          res.statusCode = 200;
          res.end(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench MCP</title>
    <style>
      body { font-family: system-ui, sans-serif; margin: 40px; color: #111; }
      code { background: #f2f2f2; padding: 2px 6px; border-radius: 4px; }
      .card { max-width: 720px; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>A2ABench MCP</h1>
      <p>Use the MCP endpoint at <code>${PUBLIC_MCP_URL}</code>.</p>
      <p>Health: <code>${baseUrl}/health</code></p>
      <p>Agent card: <a href="${agentUrl}">${agentUrl}</a></p>
    </div>
  </body>
</html>`);
        } else if (accept.includes('application/json')) {
          respondJson(res, 200, {
            mcp: PUBLIC_MCP_URL,
            health: `${baseUrl}/health`,
            agentCard: agentUrl
          });
        } else {
          respondText(
            res,
            200,
            `A2ABench MCP\nMCP: ${PUBLIC_MCP_URL}\nHealth: ${baseUrl}/health\nAgent card: ${agentUrl}`
          );
        }
      }
      metrics.totalRequests += 1;
      bumpMap(metrics.byStatus, res.statusCode);
      logEvent('info', {
        kind: 'mcp_request',
        method: req.method,
        status: res.statusCode,
        durationMs: Date.now() - startMs,
        requestId,
        agentName: resolvedAgentName ?? null,
        presentedAgentName: presentedAgentName ?? null,
        userAgent: userAgent ?? null,
        path: rawPath
      });
      return;
    }

    if (!isMcpPath) {
      res.statusCode = 404;
      res.end('Not found');
      return;
    }

    if (req.method === 'HEAD') {
      res.statusCode = 200;
      res.end();
      metrics.totalRequests += 1;
      bumpMap(metrics.byStatus, res.statusCode);
      logEvent('info', {
        kind: 'mcp_request',
        method: 'HEAD',
        status: res.statusCode,
        durationMs: Date.now() - startMs,
        requestId,
        agentName: resolvedAgentName ?? null,
        presentedAgentName: presentedAgentName ?? null,
        userAgent: userAgent ?? null
      });
      return;
    }

    const accept = Array.isArray(req.headers.accept) ? req.headers.accept.join(',') : req.headers.accept ?? '';
    const hasMcpHeaders =
      'mcp-protocol-version' in req.headers ||
      'mcp-session-id' in req.headers ||
      'mcp-client-name' in req.headers ||
      'x-mcp-client-name' in req.headers;
    const wantsEventStream = accept.includes('text/event-stream');

    const handleMcpHttp = async () => {
      const mcpServer = createMcpServer();
      registerTools(mcpServer);
      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: undefined,
        enableJsonResponse: true
      });
      transport.onerror = (err) => {
        logEvent('error', {
          kind: 'mcp_transport_error',
          error: err instanceof Error ? err.message : String(err),
          errorName: err instanceof Error ? err.name : 'unknown'
        });
      };
      await mcpServer.connect(transport);
      res.on('close', () => {
        void transport.close();
        void mcpServer.close();
      });
      await transport.handleRequest(req, res, undefined);
    };

    if (req.method === 'GET') {
      if (wantsEventStream || hasMcpHeaders) {
        await requestContext.run(
          {
            agentName: resolvedAgentName ?? undefined,
            requestId,
            authHeader,
            llmProvider: llmProvider ?? undefined,
            llmApiKey: llmApiKey ?? undefined,
            llmModel: llmModel ?? undefined,
            userAgent: userAgent ?? undefined,
            ip
          },
          async () => {
            await handleMcpHttp();
          }
        );
        metrics.totalRequests += 1;
        bumpMap(metrics.byStatus, res.statusCode);
        logEvent('info', {
          kind: 'mcp_request',
          method: 'GET',
          status: res.statusCode,
          durationMs: Date.now() - startMs,
          requestId,
          agentName: resolvedAgentName ?? null,
          presentedAgentName: presentedAgentName ?? null,
          userAgent: userAgent ?? null,
          mcp: true
        });
        return;
      }
      if (accept.includes('text/html')) {
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.statusCode = 200;
        res.end(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench MCP Endpoint</title>
    <style>
      body { font-family: system-ui, sans-serif; margin: 40px; color: #111; }
      code { background: #f2f2f2; padding: 2px 6px; border-radius: 4px; }
      .card { max-width: 720px; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>A2ABench MCP Endpoint</h1>
      <p>This is an MCP endpoint. Use an MCP client to connect.</p>
      <p>Remote URL: <code>${PUBLIC_MCP_URL}</code></p>
      <p>Docs: <a href="https://a2abench-api.web.app/docs">OpenAPI</a> • Repo: <a href="https://github.com/khalidsaidi/a2abench">GitHub</a></p>
      <p>Claude Code:</p>
      <pre>claude mcp add --transport http a2abench ${PUBLIC_MCP_URL}</pre>
    </div>
  </body>
</html>`);
        metrics.totalRequests += 1;
        bumpMap(metrics.byStatus, res.statusCode);
        logEvent('info', {
          kind: 'mcp_request',
          method: 'GET',
          status: res.statusCode,
          durationMs: Date.now() - startMs,
          requestId,
          agentName: resolvedAgentName ?? null,
          presentedAgentName: presentedAgentName ?? null,
          userAgent: userAgent ?? null,
          html: true
        });
        return;
      }
      if (accept.includes('application/json')) {
        respondJson(res, 200, {
          name: 'A2ABench MCP',
          version: SERVICE_VERSION,
          endpoint: PUBLIC_MCP_URL,
          transport: 'streamable-http',
          tools: [
            'search',
            'fetch',
            'answer',
            'create_question',
            'create_answer',
            'answer_job',
            'claim_question',
            'release_claim',
            'pending_acceptance',
            'unanswered',
            'leaderboard',
            'place_bounty',
            'vote_answer',
            'accept_answer'
          ],
          docs: 'https://a2abench-api.web.app/docs',
          repo: 'https://github.com/khalidsaidi/a2abench'
        });
      } else {
        respondText(
          res,
          200,
          `A2ABench MCP endpoint. Use an MCP client.\nEndpoint: ${PUBLIC_MCP_URL}\nDocs: https://a2abench-api.web.app/docs\nRepo: https://github.com/khalidsaidi/a2abench`
        );
      }
      metrics.totalRequests += 1;
      bumpMap(metrics.byStatus, res.statusCode);
      logEvent('info', {
        kind: 'mcp_request',
        method: 'GET',
        status: res.statusCode,
        durationMs: Date.now() - startMs,
        requestId,
        agentName: resolvedAgentName ?? null,
        presentedAgentName: presentedAgentName ?? null,
        userAgent: userAgent ?? null
      });
      return;
    }

    if (req.method !== 'POST') {
      res.statusCode = 405;
      res.end('Method not allowed');
      return;
    }

    try {
      await requestContext.run(
        {
          agentName: resolvedAgentName ?? undefined,
          requestId,
          authHeader,
          llmProvider: llmProvider ?? undefined,
          llmApiKey: llmApiKey ?? undefined,
          llmModel: llmModel ?? undefined,
          userAgent: userAgent ?? undefined,
          ip
        },
        async () => {
          await handleMcpHttp();
        }
      );
      metrics.totalRequests += 1;
      bumpMap(metrics.byStatus, res.statusCode);
      logEvent('info', {
        kind: 'mcp_request',
        method: 'POST',
        status: res.statusCode,
        durationMs: Date.now() - startMs,
        requestId,
        agentName: resolvedAgentName ?? null,
        presentedAgentName: presentedAgentName ?? null,
        userAgent: userAgent ?? null
      });
    } catch (err) {
      if (!res.headersSent) {
        respondText(res, 500, 'Internal error');
      }
      metrics.totalRequests += 1;
      bumpMap(metrics.byStatus, res.statusCode);
      logEvent('error', {
        kind: 'mcp_request',
        method: 'POST',
        status: res.statusCode,
        durationMs: Date.now() - startMs,
        requestId,
        agentName: resolvedAgentName ?? null,
        presentedAgentName: presentedAgentName ?? null,
        userAgent: userAgent ?? null,
        error: err instanceof Error ? err.message : 'unknown_error',
        errorName: err instanceof Error ? err.name : 'unknown'
      });
    }
  });

  httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`MCP remote server listening on :${PORT}`);
  });

  setInterval(logMetricsSummary, 5 * 60 * 1000);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
