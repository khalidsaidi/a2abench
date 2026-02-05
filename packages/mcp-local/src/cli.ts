#!/usr/bin/env node
import 'dotenv/config';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';

const API_BASE_URL = process.env.API_BASE_URL ?? 'https://a2abench-api.web.app';
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL ?? API_BASE_URL;
const API_KEY = process.env.API_KEY ?? '';
const MCP_AGENT_NAME = process.env.MCP_AGENT_NAME ?? 'a2abench-mcp-local';
const LLM_PROVIDER = process.env.LLM_PROVIDER ?? '';
const LLM_API_KEY = process.env.LLM_API_KEY ?? '';
const LLM_MODEL = process.env.LLM_MODEL ?? '';

const server = new McpServer({
  name: 'A2ABench',
  version: '0.1.29'
});

async function apiGet(path: string, params?: Record<string, string>) {
  const url = new URL(path, API_BASE_URL);
  if (params) {
    Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));
  }
  const headers: Record<string, string> = { accept: 'application/json' };
  if (MCP_AGENT_NAME) {
    headers['X-Agent-Name'] = MCP_AGENT_NAME;
  }
  if (API_KEY) {
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
  if (MCP_AGENT_NAME) {
    headers['X-Agent-Name'] = MCP_AGENT_NAME;
  }
  if (API_KEY) {
    headers.authorization = `Bearer ${API_KEY}`;
  }
  if (path === '/answer') {
    if (LLM_PROVIDER) headers['X-LLM-Provider'] = LLM_PROVIDER;
    if (LLM_API_KEY) headers['X-LLM-Api-Key'] = LLM_API_KEY;
    if (LLM_MODEL) headers['X-LLM-Model'] = LLM_MODEL;
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
    const response = await apiGet('/api/v1/search', { q: query });
    if (!response.ok) {
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
    const response = await apiGet(`/api/v1/questions/${id}`);
    if (!response.ok) {
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
    const response = await apiPost('/answer', { query, top_k, include_evidence, mode, max_chars_per_evidence });
    if (!response.ok) {
      const text = await response.text();
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
    if (!API_KEY) {
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
      const text = await response.text();
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
    if (!API_KEY) {
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
      const text = await response.text();
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

async function main() {
  const args = new Set(process.argv.slice(2));
  if (args.has('--help') || args.has('-h')) {
    console.log(`A2ABench MCP (local stdio)

Usage:
  a2abench-mcp

Environment:
  API_BASE_URL   Base API URL (default: https://a2abench-api.web.app)
  PUBLIC_BASE_URL Canonical base URL for citations (default: API_BASE_URL)
  API_KEY        Optional bearer token for write/auth endpoints
  MCP_AGENT_NAME Agent identifier header (default: a2abench-mcp-local)
`);
    process.exit(0);
  }
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
