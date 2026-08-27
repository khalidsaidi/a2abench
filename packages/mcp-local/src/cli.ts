#!/usr/bin/env node
import 'dotenv/config';
import { readFileSync } from 'node:fs';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';

const PACKAGE_VERSION: string = (() => {
  try {
    const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
    return typeof pkg.version === 'string' ? pkg.version : '0.0.0';
  } catch {
    return '0.0.0';
  }
})();

const API_BASE_URL = (process.env.API_BASE_URL ?? 'https://a2abench-api.web.app').replace(/\/$/, '');

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

type ToolResult = {
  isError?: boolean;
  content: Array<{ type: 'text'; text: string }>;
};

/**
 * Turn an HTTP response into an MCP tool result.
 * Failures are surfaced as a short, structured message (status code + brief
 * detail); raw error bodies (e.g. hosting "Site Not Found" HTML pages) are
 * never passed through.
 */
async function toToolResult(action: string, response: Response): Promise<ToolResult> {
  const text = await response.text();
  if (response.ok) {
    return { content: [{ type: 'text', text }] };
  }
  const contentType = response.headers.get('content-type') ?? '';
  let detail: string | undefined;
  if (contentType.includes('application/json')) {
    try {
      const parsed = JSON.parse(text);
      detail = typeof parsed === 'string' ? parsed : (parsed?.error ?? parsed?.message);
      if (typeof detail !== 'string') detail = JSON.stringify(parsed).slice(0, 300);
    } catch {
      detail = undefined;
    }
  } else if (contentType.includes('text/plain')) {
    detail = text.slice(0, 300);
  }
  // Anything else (HTML error pages, etc.) is dropped: it is noise for an agent.
  return {
    isError: true,
    content: [
      {
        type: 'text',
        text: JSON.stringify({
          error: `${action} failed: HTTP ${response.status}${response.statusText ? ` ${response.statusText}` : ''}`,
          status: response.status,
          ...(detail ? { detail } : {}),
          hint: `Check that the A2ABench API is reachable at ${API_BASE_URL} (override with the API_BASE_URL environment variable).`
        })
      }
    ]
  };
}

function networkErrorResult(action: string, err: unknown): ToolResult {
  const message = err instanceof Error ? err.message : String(err);
  return {
    isError: true,
    content: [
      {
        type: 'text',
        text: JSON.stringify({
          error: `${action} failed: could not reach the A2ABench API (${message})`,
          hint: `Check that the A2ABench API is reachable at ${API_BASE_URL} (override with the API_BASE_URL environment variable).`
        })
      }
    ]
  };
}

function createServer() {
  const server = new McpServer({
    name: 'A2ABench',
    version: PACKAGE_VERSION
  });

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
      try {
        const response = await apiGet('/v1/eval/questions', page ? { page: String(page) } : undefined);
        return await toToolResult('list_benchmark_questions', response);
      } catch (err) {
        return networkErrorResult('list_benchmark_questions', err);
      }
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
      try {
        const response = await apiPost('/v1/eval/submit', { entrant_name, submissions }, api_key);
        return await toToolResult('submit_benchmark_run', response);
      } catch (err) {
        return networkErrorResult('submit_benchmark_run', err);
      }
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
      try {
        const response = await apiGet('/v1/eval/leaderboard', limit ? { limit: String(limit) } : undefined);
        return await toToolResult('get_leaderboard', response);
      } catch (err) {
        return networkErrorResult('get_leaderboard', err);
      }
    }
  );

  return server;
}

async function main() {
  const server = createServer();
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
