#!/usr/bin/env node
import 'dotenv/config';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';

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

function createServer() {
  const server = new McpServer({
    name: 'A2ABench',
    version: '0.2.0'
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
  const server = createServer();
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
