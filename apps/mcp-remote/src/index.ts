import 'dotenv/config';
import { createServer } from 'node:http';
import { AsyncLocalStorage } from 'node:async_hooks';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { z } from 'zod';

const API_BASE_URL = process.env.API_BASE_URL ?? 'http://localhost:3000';
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL ?? API_BASE_URL;
const PUBLIC_MCP_URL =
  process.env.PUBLIC_MCP_URL ?? 'https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp';
const API_KEY = process.env.API_KEY ?? '';
const PORT = Number(process.env.PORT ?? process.env.MCP_PORT ?? 4000);
const MCP_AGENT_NAME = process.env.MCP_AGENT_NAME ?? 'a2abench-mcp-remote';
const ALLOWED_ORIGINS = (process.env.MCP_ALLOWED_ORIGINS ?? '')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

const requestContext = new AsyncLocalStorage<{ agentName?: string }>();

const server = new McpServer({
  name: 'A2ABench',
  version: '0.1.9'
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

function logMetricsSummary() {
  const byStatus: Record<string, number> = {};
  const byTool: Record<string, number> = {};
  const toolErrors: Record<string, number> = {};
  for (const [key, value] of metrics.byStatus) byStatus[String(key)] = value;
  for (const [key, value] of metrics.byTool) byTool[key] = value;
  for (const [key, value] of metrics.toolErrors) toolErrors[key] = value;
  console.log(
    JSON.stringify({
      kind: 'mcp_metrics_summary',
      startedAt: metrics.startedAt,
      totalRequests: metrics.totalRequests,
      totalToolCalls: metrics.totalToolCalls,
      byStatus,
      byTool,
      toolErrors
    })
  );
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
  if (API_KEY) {
    headers.authorization = `Bearer ${API_KEY}`;
  }
  const response = await fetch(url, { headers });
  return response;
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
    if (!response.ok) {
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'search');
      bumpMap(metrics.toolErrors, 'search');
      console.log(
        JSON.stringify({
          kind: 'mcp_tool',
          tool: 'search',
          status: response.status,
          durationMs: Date.now() - toolStart
        })
      );
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
    console.log(
      JSON.stringify({
        kind: 'mcp_tool',
        tool: 'search',
        status: response.status,
        durationMs: Date.now() - toolStart,
        resultCount: results.length
      })
    );
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
    if (!response.ok) {
      metrics.totalToolCalls += 1;
      bumpMap(metrics.byTool, 'fetch');
      bumpMap(metrics.toolErrors, 'fetch');
      console.log(
        JSON.stringify({
          kind: 'mcp_tool',
          tool: 'fetch',
          status: response.status,
          durationMs: Date.now() - toolStart,
          id
        })
      );
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({ id, error: 'Not found' })
          }
        ]
      };
    }
    const data = await response.json();
    metrics.totalToolCalls += 1;
    bumpMap(metrics.byTool, 'fetch');
    console.log(
      JSON.stringify({
        kind: 'mcp_tool',
        tool: 'fetch',
        status: response.status,
        durationMs: Date.now() - toolStart,
        id
      })
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
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
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

    if (req.method === 'OPTIONS') {
      res.statusCode = 204;
      res.end();
      return;
    }

    if (!isOriginAllowed(origin)) {
      res.statusCode = 403;
      res.end('Origin not allowed');
      return;
    }

    const agentHeader = req.headers['x-agent-name'] ?? req.headers['x-mcp-client-name'] ?? req.headers['mcp-client-name'];
    const agentName = Array.isArray(agentHeader) ? agentHeader[0] : agentHeader;
    const userAgent = Array.isArray(req.headers['user-agent']) ? req.headers['user-agent'][0] : req.headers['user-agent'];
    const startMs = Date.now();
    const pathname = (() => {
      try {
        return new URL(req.url, 'http://localhost').pathname;
      } catch {
        return req.url;
      }
    })();

    if (pathname === '/healthz' || pathname.startsWith('/healthz/')) {
      res.setHeader('Content-Type', 'application/json');
      res.statusCode = 200;
      res.end(JSON.stringify({ status: 'ok' }));
      metrics.totalRequests += 1;
      bumpMap(metrics.byStatus, res.statusCode);
      console.log(
        JSON.stringify({
          kind: 'mcp_request',
          method: req.method,
          status: res.statusCode,
          durationMs: Date.now() - startMs,
          agentName: agentName ?? null,
          userAgent: userAgent ?? null,
          path: '/healthz'
        })
      );
      return;
    }

    if (!pathname.startsWith('/mcp')) {
      res.statusCode = 404;
      res.end('Not found');
      return;
    }

    if (req.method === 'HEAD') {
      res.statusCode = 200;
      res.end();
      metrics.totalRequests += 1;
      bumpMap(metrics.byStatus, res.statusCode);
      console.log(
        JSON.stringify({
          kind: 'mcp_request',
          method: 'HEAD',
          status: res.statusCode,
          durationMs: Date.now() - startMs,
          agentName: agentName ?? null,
          userAgent: userAgent ?? null
        })
      );
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
        console.log(
          JSON.stringify({
            kind: 'mcp_request',
            method: 'GET',
            status: res.statusCode,
            durationMs: Date.now() - startMs,
            agentName: agentName ?? null,
            userAgent: userAgent ?? null,
            html: true
          })
        );
        return;
      }
      await requestContext.run({ agentName }, async () => {
        await transport.handleRequest(req, res);
      });
      metrics.totalRequests += 1;
      bumpMap(metrics.byStatus, res.statusCode);
      console.log(
        JSON.stringify({
          kind: 'mcp_request',
          method: 'GET',
          status: res.statusCode,
          durationMs: Date.now() - startMs,
          agentName: agentName ?? null,
          userAgent: userAgent ?? null
        })
      );
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
        await requestContext.run({ agentName }, async () => {
          await transport.handleRequest(req, res, json);
        });
        metrics.totalRequests += 1;
        bumpMap(metrics.byStatus, res.statusCode);
        console.log(
          JSON.stringify({
            kind: 'mcp_request',
            method: 'POST',
            status: res.statusCode,
            durationMs: Date.now() - startMs,
            agentName: agentName ?? null,
            userAgent: userAgent ?? null
          })
        );
      } catch (err) {
        res.statusCode = 400;
        res.end('Invalid JSON');
        metrics.totalRequests += 1;
        bumpMap(metrics.byStatus, res.statusCode);
        console.log(
          JSON.stringify({
            kind: 'mcp_request',
            method: 'POST',
            status: res.statusCode,
            durationMs: Date.now() - startMs,
            agentName: agentName ?? null,
            userAgent: userAgent ?? null,
            error: 'invalid_json'
          })
        );
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
