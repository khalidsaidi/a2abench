import 'dotenv/config';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { URL } from 'node:url';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { z } from 'zod';

const PORT = Number(process.env.PORT ?? 3000);
const API_BASE_URL = (process.env.API_BASE_URL ?? 'https://a2abench-api.web.app').replace(/\/$/, '');
const PUBLIC_MCP_URL = (process.env.PUBLIC_MCP_URL ?? 'https://a2abench-mcp.web.app/mcp').replace(/\/$/, '');
const SERVICE_VERSION = '0.2.0';

function isMcpRequest(req: IncomingMessage) {
  const accept = Array.isArray(req.headers.accept) ? req.headers.accept.join(',') : (req.headers.accept ?? '');
  return accept.includes('text/event-stream') || 'mcp-session-id' in req.headers || 'mcp-protocol-version' in req.headers;
}

function writeJson(res: ServerResponse, status: number, payload: unknown) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS,HEAD');
  res.end(JSON.stringify(payload));
}

function writeText(res: ServerResponse, status: number, text: string) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS,HEAD');
  res.end(text);
}

async function apiGet(path: string, params?: Record<string, string>) {
  const target = new URL(path, API_BASE_URL);
  for (const [key, value] of Object.entries(params ?? {})) target.searchParams.set(key, value);
  return fetch(target, { headers: { accept: 'application/json' } });
}

async function apiPost(path: string, body: unknown, apiKey?: string) {
  const headers: Record<string, string> = { 'content-type': 'application/json', accept: 'application/json' };
  if (apiKey) headers.authorization = `Bearer ${apiKey}`;
  return fetch(new URL(path, API_BASE_URL), {
    method: 'POST',
    headers,
    body: JSON.stringify(body)
  });
}

function createMcpServer() {
  const server = new McpServer({ name: 'A2ABench', version: SERVICE_VERSION });

  server.registerTool(
    'list_benchmark_questions',
    {
      title: 'List benchmark questions',
      description: 'List A2ABench questions with pagination.',
      inputSchema: {
        page: z.number().int().min(1).optional()
      }
    },
    async ({ page }) => {
      const response = await apiGet('/v1/eval/questions', page ? { page: String(page) } : undefined);
      const text = await response.text();
      if (!response.ok) {
        return {
          isError: true,
          content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to list questions', status: response.status }) }]
        };
      }
      return { content: [{ type: 'text', text }] };
    }
  );

  server.registerTool(
    'submit_benchmark_run',
    {
      title: 'Submit benchmark run',
      description: 'Submit benchmark answers for scoring.',
      inputSchema: {
        entrant_name: z.string().min(1),
        api_key: z.string().min(1),
        submissions: z.array(
          z.object({
            question_id: z.string().min(1),
            answer: z.string().min(1)
          })
        ).min(1)
      }
    },
    async ({ entrant_name, api_key, submissions }) => {
      const response = await apiPost('/v1/eval/submit', { entrant_name, submissions }, api_key);
      const text = await response.text();
      if (!response.ok) {
        return {
          isError: true,
          content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to submit run', status: response.status }) }]
        };
      }
      return { content: [{ type: 'text', text }] };
    }
  );

  server.registerTool(
    'get_leaderboard',
    {
      title: 'Get leaderboard',
      description: 'Fetch public leaderboard ranked by score.',
      inputSchema: {
        limit: z.number().int().min(1).max(200).optional()
      }
    },
    async ({ limit }) => {
      const response = await apiGet('/v1/eval/leaderboard', limit ? { limit: String(limit) } : undefined);
      const text = await response.text();
      if (!response.ok) {
        return {
          isError: true,
          content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to fetch leaderboard', status: response.status }) }]
        };
      }
      return { content: [{ type: 'text', text }] };
    }
  );

  return server;
}

async function main() {
  const httpServer = createServer(async (req, res) => {
    if (!req.url || !req.method) {
      writeText(res, 400, 'Bad request');
      return;
    }

    if (req.method === 'OPTIONS') {
      res.statusCode = 200;
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Headers', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS,HEAD');
      res.end();
      return;
    }

    const pathname = new URL(req.url, 'http://localhost').pathname;

    if (pathname === '/.well-known/glama.json') {
      writeJson(res, 200, {
        name: 'A2ABench MCP',
        endpoint: 'https://a2abench-mcp.web.app/mcp',
        tools: ['list_benchmark_questions', 'submit_benchmark_run', 'get_leaderboard']
      });
      return;
    }

    if (pathname === '/health' || pathname === '/health/' || pathname === '/healthz' || pathname === '/healthz/') {
      writeJson(res, 200, { ok: true, service: 'a2abench-mcp-remote' });
      return;
    }

    if (pathname === '/readyz' || pathname === '/readyz/' || pathname === '/ready' || pathname === '/ready/') {
      writeJson(res, 200, { ok: true, service: 'a2abench-mcp-remote' });
      return;
    }

    if (pathname === '/' || pathname === '') {
      if (req.method === 'HEAD') {
        res.statusCode = 200;
        res.end();
        return;
      }
      writeText(res, 200, `A2ABench MCP\nMCP: ${PUBLIC_MCP_URL}\nHealth: /health`);
      return;
    }

    const isMcpPath = pathname === '/mcp' || pathname.startsWith('/mcp/');
    if (!isMcpPath) {
      writeJson(res, 404, { error: 'not_found' });
      return;
    }

    if (req.method === 'HEAD') {
      res.statusCode = 200;
      res.end();
      return;
    }

    if (!isMcpRequest(req)) {
      writeJson(res, 406, {
        jsonrpc: '2.0',
        error: { code: -32000, message: 'Not Acceptable: Client must accept text/event-stream' },
        id: null
      });
      return;
    }

    if (req.method !== 'GET' && req.method !== 'POST') {
      writeText(res, 405, 'Method not allowed');
      return;
    }

    const mcpServer = createMcpServer();
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
      enableJsonResponse: true
    });

    try {
      await mcpServer.connect(transport);
      res.on('close', () => {
        void transport.close();
        void mcpServer.close();
      });
      await transport.handleRequest(req, res, undefined);
    } catch {
      if (!res.headersSent) writeText(res, 500, 'Internal error');
    }
  });

  httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`MCP remote server listening on :${PORT}`);
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
