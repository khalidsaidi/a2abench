import 'dotenv/config';
import { createServer } from 'node:http';
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { z } from 'zod';

const API_BASE_URL = process.env.API_BASE_URL ?? 'http://localhost:3000';
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL ?? API_BASE_URL;
const PUBLIC_MCP_URL =
  process.env.PUBLIC_MCP_URL ?? 'https://a2abench-mcp.web.app/mcp';
const API_KEY = process.env.API_KEY ?? '';
const PORT = Number(process.env.PORT ?? process.env.MCP_PORT ?? 4000);
const MCP_AGENT_NAME = process.env.MCP_AGENT_NAME ?? 'a2abench-mcp-remote';
const SERVICE_VERSION = process.env.SERVICE_VERSION ?? '0.1.18';
const COMMIT_SHA = process.env.COMMIT_SHA ?? process.env.GIT_SHA ?? 'unknown';
const LOG_LEVEL = process.env.LOG_LEVEL ?? 'info';
const CAPTURE_AGENT_PAYLOADS = (process.env.CAPTURE_AGENT_PAYLOADS ?? '').toLowerCase() === 'true';
const AGENT_EVENT_TOKEN = process.env.AGENT_EVENT_TOKEN ?? '';
const AGENT_EVENT_ENDPOINT =
  process.env.AGENT_EVENT_ENDPOINT ?? `${API_BASE_URL.replace(/\/$/, '')}/api/v1/admin/agent-events/ingest`;
const ALLOWED_ORIGINS = (process.env.MCP_ALLOWED_ORIGINS ?? '')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

const requestContext = new AsyncLocalStorage<{
  agentName?: string;
  requestId?: string;
  authHeader?: string;
  userAgent?: string;
  ip?: string;
}>();

const server = new McpServer({
  name: 'A2ABench',
  version: SERVICE_VERSION
});

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

function redactString(value: string) {
  return value.replace(/Bearer\\s+[A-Za-z0-9._-]+/gi, 'Bearer [redacted]');
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

async function apiGet(path: string, params?: Record<string, string>) {
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
  if (ctxAuth) {
    headers.authorization = ctxAuth;
  } else if (API_KEY) {
    headers.authorization = `Bearer ${API_KEY}`;
  }
  const response = await fetch(url, { headers });
  return response;
}

async function apiPost(path: string, body: Record<string, unknown>, query?: Record<string, string>) {
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
  if (ctxAuth) {
    headers.authorization = ctxAuth;
  } else if (API_KEY) {
    headers.authorization = `Bearer ${API_KEY}`;
  }
  return fetch(url, { method: 'POST', headers, body: JSON.stringify(body) });
}

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
  'create_question',
  {
    title: 'Create question',
    description: 'Create a new question thread (requires API key).',
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
    const authHeader = requestContext.getStore()?.authHeader ?? (API_KEY ? `Bearer ${API_KEY}` : '');
    if (!authHeader) {
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'create_question');
      bumpMap(metrics.toolErrors, 'create_question');
      captureToolEvent(
        'create_question',
        { title, bodyMd, tags, force },
        { error: 'Missing API key' },
        401,
        Date.now() - toolStart
      );
      return {
        isError: true,
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: 'Missing API key',
              hint: 'Get a trial key at /api/v1/auth/trial-key'
            })
          }
        ]
      };
    }
    const response = await apiPost('/api/v1/questions', { title, bodyMd, tags }, force ? { force: '1' } : undefined);
    if (!response.ok) {
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'create_question');
      bumpMap(metrics.toolErrors, 'create_question');
      const text = await response.text();
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
        { error: text || 'Failed to create question', status: response.status },
        response.status,
        Date.now() - toolStart
      );
      return {
        isError: true,
        content: [
          {
            type: 'text',
            text: JSON.stringify({ error: text || 'Failed to create question', status: response.status })
          }
        ]
      };
    }
    const data = (await response.json()) as Record<string, unknown>;
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
            url: `${PUBLIC_BASE_URL}/q/${data.id}`
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
    description: 'Create an answer for a question (requires API key).',
    inputSchema: {
      id: z.string().min(1),
      bodyMd: z.string().min(3)
    }
  },
  async ({ id, bodyMd }) => {
    const toolStart = Date.now();
    const requestId = requestContext.getStore()?.requestId ?? null;
    const authHeader = requestContext.getStore()?.authHeader ?? (API_KEY ? `Bearer ${API_KEY}` : '');
    if (!authHeader) {
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'create_answer');
      bumpMap(metrics.toolErrors, 'create_answer');
      captureToolEvent(
        'create_answer',
        { id, bodyMd },
        { error: 'Missing API key' },
        401,
        Date.now() - toolStart
      );
      return {
        isError: true,
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: 'Missing API key',
              hint: 'Get a trial key at /api/v1/auth/trial-key'
            })
          }
        ]
      };
    }
    const response = await apiPost(`/api/v1/questions/${id}/answers`, { bodyMd });
    if (!response.ok) {
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'create_answer');
      bumpMap(metrics.toolErrors, 'create_answer');
      const text = await response.text();
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
        { error: text || 'Failed to create answer', status: response.status },
        response.status,
        Date.now() - toolStart
      );
      return {
        isError: true,
        content: [
          {
            type: 'text',
            text: JSON.stringify({ error: text || 'Failed to create answer', status: response.status })
          }
        ]
      };
    }
    const data = (await response.json()) as Record<string, unknown>;
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
            url: `${PUBLIC_BASE_URL}/q/${id}`
          })
        }
      ]
    };
  }
);

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
  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: undefined
  });

  await server.connect(transport);

  const httpServer = createServer(async (req, res) => {
    if (!req.url || !req.method) {
      res.statusCode = 400;
      res.end('Bad request');
      return;
    }

    const origin = req.headers.origin as string | undefined;
    applyCors(res, origin);

    const agentHeader = req.headers['x-agent-name'] ?? req.headers['x-mcp-client-name'] ?? req.headers['mcp-client-name'];
    const agentName = Array.isArray(agentHeader) ? agentHeader[0] : agentHeader;
    const authHeader = Array.isArray(req.headers.authorization)
      ? req.headers.authorization[0]
      : req.headers.authorization;
    const userAgent = Array.isArray(req.headers['user-agent']) ? req.headers['user-agent'][0] : req.headers['user-agent'];
    const ip = getClientIp(req.headers as Record<string, string | string[] | undefined>, req.socket?.remoteAddress ?? null) ?? undefined;
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
    if (req.method === 'POST' && isMcpPath) {
      const acceptHeader = Array.isArray(req.headers.accept)
        ? req.headers.accept.join(',')
        : req.headers.accept ?? '';
      if (!acceptHeader.includes('application/json') || !acceptHeader.includes('text/event-stream')) {
        const normalized = 'application/json, text/event-stream';
        req.headers.accept = normalized;
        if (Array.isArray(req.rawHeaders)) {
          let updated = false;
          for (let i = 0; i < req.rawHeaders.length; i += 2) {
            if (String(req.rawHeaders[i]).toLowerCase() === 'accept') {
              req.rawHeaders[i + 1] = normalized;
              updated = true;
              break;
            }
          }
          if (!updated) {
            req.rawHeaders.push('accept', normalized);
          }
        }
      }
    }

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
        agentName: agentName ?? null,
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
        agentName: agentName ?? null,
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
        agentName: agentName ?? null,
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
        agentName: agentName ?? null,
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
        agentName: agentName ?? null,
        userAgent: userAgent ?? null
      });
      return;
    }

    if (req.method === 'GET') {
      const accept = Array.isArray(req.headers.accept) ? req.headers.accept.join(',') : req.headers.accept ?? '';
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
          agentName: agentName ?? null,
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
          tools: ['search', 'fetch', 'create_question', 'create_answer'],
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
        agentName: agentName ?? null,
        userAgent: userAgent ?? null
      });
      return;
    }

    if (req.method !== 'POST') {
      res.statusCode = 405;
      res.end('Method not allowed');
      return;
    }

    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
    });
    req.on('end', async () => {
      try {
        const json = body.length ? JSON.parse(body) : undefined;
        await requestContext.run({
          agentName: agentName ?? undefined,
          requestId,
          authHeader,
          userAgent: userAgent ?? undefined,
          ip
        }, async () => {
          await transport.handleRequest(req, res, json);
        });
        metrics.totalRequests += 1;
        bumpMap(metrics.byStatus, res.statusCode);
        logEvent('info', {
          kind: 'mcp_request',
          method: 'POST',
          status: res.statusCode,
          durationMs: Date.now() - startMs,
          requestId,
          agentName: agentName ?? null,
          userAgent: userAgent ?? null
        });
      } catch (err) {
        respondText(res, 400, 'Invalid JSON');
        metrics.totalRequests += 1;
        bumpMap(metrics.byStatus, res.statusCode);
        logEvent('warn', {
          kind: 'mcp_request',
          method: 'POST',
          status: res.statusCode,
          durationMs: Date.now() - startMs,
          requestId,
          agentName: agentName ?? null,
          userAgent: userAgent ?? null,
          error: 'invalid_json',
          errorName: err instanceof Error ? err.name : 'unknown'
        });
      }
    });
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
