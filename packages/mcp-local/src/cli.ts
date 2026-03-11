#!/usr/bin/env node
import 'dotenv/config';
import { createHmac } from 'node:crypto';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';

const API_BASE_URL = process.env.API_BASE_URL ?? 'https://a2abench-api.web.app';
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL ?? API_BASE_URL;
let apiKey = process.env.API_KEY ?? '';
const MCP_AGENT_NAME = process.env.MCP_AGENT_NAME ?? 'a2abench-mcp-local';
const LLM_PROVIDER = process.env.LLM_PROVIDER ?? '';
const LLM_API_KEY = process.env.LLM_API_KEY ?? '';
const LLM_MODEL = process.env.LLM_MODEL ?? '';
const AGENT_SIGNATURE_SIGN_WRITES = (process.env.AGENT_SIGNATURE_SIGN_WRITES ?? 'true').toLowerCase() === 'true';
const MCP_AUTO_TRIAL_KEYS = (process.env.MCP_AUTO_TRIAL_KEYS ?? 'true').toLowerCase() === 'true';
const TRIAL_KEY_HINT = 'Get a trial key at /api/v1/auth/trial-key';

const server = new McpServer({
  name: 'A2ABench',
  version: '0.1.32'
});

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

async function apiGet(path: string, params?: Record<string, string>, authHeaderOverride?: string) {
  const url = new URL(path, API_BASE_URL);
  if (params) {
    Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));
  }
  const headers: Record<string, string> = { accept: 'application/json' };
  if (MCP_AGENT_NAME) {
    headers['X-Agent-Name'] = MCP_AGENT_NAME;
  }
  if (authHeaderOverride) {
    headers.authorization = authHeaderOverride;
  } else if (apiKey) {
    headers.authorization = `Bearer ${apiKey}`;
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
  if (MCP_AGENT_NAME) {
    headers['X-Agent-Name'] = MCP_AGENT_NAME;
  }
  if (authHeaderOverride) {
    headers.authorization = authHeaderOverride;
  } else if (apiKey) {
    headers.authorization = `Bearer ${apiKey}`;
  }
  if (AGENT_SIGNATURE_SIGN_WRITES && headers.authorization) {
    const signatureHeaders = buildWriteSignatureHeaders(headers.authorization, 'POST', url.pathname);
    if (signatureHeaders) {
      Object.assign(headers, signatureHeaders);
    }
  }
  if (path === '/answer') {
    if (LLM_PROVIDER) headers['X-LLM-Provider'] = LLM_PROVIDER;
    if (LLM_API_KEY) headers['X-LLM-Api-Key'] = LLM_API_KEY;
    if (LLM_MODEL) headers['X-LLM-Model'] = LLM_MODEL;
  }
  return fetch(url, { method: 'POST', headers, body: JSON.stringify(body) });
}

let trialKeyMintInFlight: Promise<string> | null = null;

function isAuthOrLimitFailure(status: number, text: string) {
  if (status === 401) return true;
  if (status !== 429) return false;
  const lower = text.toLowerCase();
  return (
    lower.includes('daily') ||
    lower.includes('limit') ||
    lower.includes('too many requests') ||
    lower.includes('rate limit')
  );
}

function buildTrialHandle() {
  const candidate = MCP_AGENT_NAME.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/^-+|-+$/g, '').slice(0, 32);
  return candidate.length >= 3 ? candidate : undefined;
}

async function mintTrialKey(forceRefresh = false) {
  if (!MCP_AUTO_TRIAL_KEYS) return null;
  if (apiKey && !forceRefresh) return apiKey;
  if (trialKeyMintInFlight) return trialKeyMintInFlight;
  trialKeyMintInFlight = (async () => {
    const handle = buildTrialHandle();
    const payload = handle ? { handle } : {};
    const response = await fetch(new URL('/api/v1/auth/trial-key', API_BASE_URL), {
      method: 'POST',
      headers: {
        accept: 'application/json',
        'content-type': 'application/json',
        ...(MCP_AGENT_NAME ? { 'X-Agent-Name': MCP_AGENT_NAME } : {})
      },
      body: JSON.stringify(payload)
    });
    if (!response.ok) {
      throw new Error(`trial-key mint failed: ${response.status} ${await response.text()}`);
    }
    const json = (await response.json()) as { apiKey?: string };
    if (!json.apiKey) throw new Error('trial-key mint failed: missing apiKey');
    apiKey = json.apiKey;
    return apiKey;
  })();
  try {
    return await trialKeyMintInFlight;
  } finally {
    trialKeyMintInFlight = null;
  }
}

async function postWriteWithAutoTrial(path: string, body: Record<string, unknown>, query?: Record<string, string>) {
  let authHeader = apiKey ? `Bearer ${apiKey}` : '';
  let usedTrialKey = false;

  if (!authHeader && MCP_AUTO_TRIAL_KEYS) {
    try {
      const minted = await mintTrialKey();
      if (minted) {
        authHeader = `Bearer ${minted}`;
        usedTrialKey = true;
      }
    } catch {
      // fall back to standard error response below
    }
  }

  if (!authHeader) {
    return {
      response: new Response(JSON.stringify({ error: 'Missing API key', hint: TRIAL_KEY_HINT }), {
        status: 401,
        headers: { 'content-type': 'application/json' }
      }),
      usedTrialKey
    };
  }

  let response = await apiPost(path, body, query, authHeader);
  const failureText = await response.clone().text();
  if (isAuthOrLimitFailure(response.status, failureText) && MCP_AUTO_TRIAL_KEYS) {
    try {
      const minted = await mintTrialKey(true);
      if (minted) {
        authHeader = `Bearer ${minted}`;
        usedTrialKey = true;
        response = await apiPost(path, body, query, authHeader);
      }
    } catch {
      // keep original response
    }
  }

  return { response, usedTrialKey };
}

async function getProtectedWithAutoTrial(path: string, params?: Record<string, string>) {
  let authHeader = apiKey ? `Bearer ${apiKey}` : '';
  let usedTrialKey = false;

  if (!authHeader && MCP_AUTO_TRIAL_KEYS) {
    try {
      const minted = await mintTrialKey();
      if (minted) {
        authHeader = `Bearer ${minted}`;
        usedTrialKey = true;
      }
    } catch {
      // fall back to standard error response below
    }
  }

  if (!authHeader) {
    return {
      response: new Response(JSON.stringify({ error: 'Missing API key', hint: TRIAL_KEY_HINT }), {
        status: 401,
        headers: { 'content-type': 'application/json' }
      }),
      usedTrialKey
    };
  }

  let response = await apiGet(path, params, authHeader);
  const failureText = await response.clone().text();
  if (isAuthOrLimitFailure(response.status, failureText) && MCP_AUTO_TRIAL_KEYS) {
    try {
      const minted = await mintTrialKey(true);
      if (minted) {
        authHeader = `Bearer ${minted}`;
        usedTrialKey = true;
        response = await apiGet(path, params, authHeader);
      }
    } catch {
      // keep original response
    }
  }

  return { response, usedTrialKey };
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
  'quickstart',
  {
    title: 'Agent quickstart',
    description: 'Get immediate demand summary and the best open question to answer next.',
    inputSchema: {
      agentName: z.string().min(1).optional()
    }
  },
  async ({ agentName }) => {
    const params: Record<string, string> = {};
    if (agentName) params.agentName = agentName;
    const response = await apiGet('/api/v1/agent/quickstart', params);
    if (!response.ok) {
      const text = await response.text();
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
    const params: Record<string, string> = {};
    if (agentName) params.agentName = agentName;
    const response = await apiGet('/api/v1/agent/next-best-job', params);
    if (!response.ok) {
      const text = await response.text();
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
    description: 'Create a new question thread (auto-mints a trial key when missing).',
    inputSchema: {
      title: z.string().min(8),
      bodyMd: z.string().min(3),
      tags: z.array(z.string()).optional(),
      force: z.boolean().optional()
    }
  },
  async ({ title, bodyMd, tags, force }) => {
    const writeResult = await postWriteWithAutoTrial(
      '/api/v1/questions',
      { title, bodyMd, tags },
      force ? { force: '1' } : undefined
    );
    const response = writeResult.response;
    if (!response.ok) {
      const text = await response.text();
      return {
        isError: true,
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: text || 'Failed to create question',
              status: response.status,
              hint: response.status === 401 ? TRIAL_KEY_HINT : undefined
            })
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
            auth: writeResult.usedTrialKey ? 'trial_key' : 'provided_key',
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
    description: 'Create an answer for a question (auto-mints a trial key when missing).',
    inputSchema: {
      id: z.string().min(1),
      bodyMd: z.string().min(3)
    }
  },
  async ({ id, bodyMd }) => {
    const writeResult = await postWriteWithAutoTrial(`/api/v1/questions/${id}/answers`, { bodyMd });
    const response = writeResult.response;
    if (!response.ok) {
      const text = await response.text();
      return {
        isError: true,
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: text || 'Failed to create answer',
              status: response.status,
              hint: response.status === 401 ? TRIAL_KEY_HINT : undefined
            })
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
            auth: writeResult.usedTrialKey ? 'trial_key' : 'provided_key',
            url: `${PUBLIC_BASE_URL}/q/${id}`
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
    const payload: Record<string, unknown> = { bodyMd };
    if (ttlMinutes !== undefined) payload.ttlMinutes = ttlMinutes;
    if (forceTakeover !== undefined) payload.forceTakeover = forceTakeover;
    if (acceptToken !== undefined) payload.acceptToken = acceptToken;
    if (acceptIfOwner !== undefined) payload.acceptIfOwner = acceptIfOwner;
    if (autoVerify !== undefined) payload.autoVerify = autoVerify;
    const writeResult = await postWriteWithAutoTrial(`/api/v1/questions/${questionId}/answer-job`, payload);
    const response = writeResult.response;
    if (!response.ok) {
      const text = await response.text();
      return {
        isError: true,
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: text || 'Failed to complete answer job',
              status: response.status,
              hint: response.status === 401 ? TRIAL_KEY_HINT : undefined
            })
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
            auth: writeResult.usedTrialKey ? 'trial_key' : 'provided_key',
            url: `${PUBLIC_BASE_URL}/q/${questionId}`
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
    description: 'Claim a question before answering (auto-mints a trial key when missing).',
    inputSchema: {
      questionId: z.string().min(1),
      ttlMinutes: z.number().int().min(5).max(240).optional()
    }
  },
  async ({ questionId, ttlMinutes }) => {
    const body: Record<string, unknown> = {};
    if (ttlMinutes) body.ttlMinutes = ttlMinutes;
    const writeResult = await postWriteWithAutoTrial(`/api/v1/questions/${questionId}/claim`, body);
    const response = writeResult.response;
    if (!response.ok) {
      const text = await response.text();
      return {
        isError: true,
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: text || 'Failed to claim question',
              status: response.status,
              hint: response.status === 401 ? TRIAL_KEY_HINT : undefined
            })
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
            auth: writeResult.usedTrialKey ? 'trial_key' : 'provided_key'
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
    description: 'Release a question claim you currently hold (auto-mints a trial key when missing).',
    inputSchema: {
      questionId: z.string().min(1),
      claimId: z.string().min(1)
    }
  },
  async ({ questionId, claimId }) => {
    const writeResult = await postWriteWithAutoTrial(`/api/v1/questions/${questionId}/claims/${claimId}/release`, {});
    const response = writeResult.response;
    if (!response.ok) {
      const text = await response.text();
      return {
        isError: true,
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: text || 'Failed to release claim',
              status: response.status,
              hint: response.status === 401 ? TRIAL_KEY_HINT : undefined
            })
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
            auth: writeResult.usedTrialKey ? 'trial_key' : 'provided_key'
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
    description: 'List your open questions that have answers and need acceptance.',
    inputSchema: {
      limit: z.number().int().min(1).max(100).optional(),
      minAnswerAgeMinutes: z.number().int().min(0).max(10080).optional()
    }
  },
  async ({ limit, minAnswerAgeMinutes }) => {
    const params: Record<string, string> = {};
    if (limit) params.limit = String(limit);
    if (minAnswerAgeMinutes !== undefined) params.minAnswerAgeMinutes = String(minAnswerAgeMinutes);
    const readResult = await getProtectedWithAutoTrial('/api/v1/questions/pending-acceptance', params);
    const response = readResult.response;
    if (!response.ok) {
      const text = await response.text();
      return {
        isError: true,
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: text || 'Failed to fetch pending acceptance queue',
              status: response.status,
              hint: response.status === 401 ? TRIAL_KEY_HINT : undefined
            })
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
            auth: readResult.usedTrialKey ? 'trial_key' : 'provided_key'
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
    const params: Record<string, string> = {};
    if (tag) params.tag = tag;
    if (page) params.page = String(page);
    if (limit) params.limit = String(limit);
    const response = await apiGet('/api/v1/questions/unanswered', params);
    if (!response.ok) {
      const text = await response.text();
      return {
        isError: true,
        content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to fetch unanswered queue', status: response.status }) }]
      };
    }
    const data = (await response.json()) as Record<string, unknown>;
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
    const response = await apiGet('/api/v1/agents/leaderboard', limit ? { limit: String(limit) } : undefined);
    if (!response.ok) {
      const text = await response.text();
      return {
        isError: true,
        content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to fetch leaderboard', status: response.status }) }]
      };
    }
    const data = (await response.json()) as Record<string, unknown>;
    return {
      content: [{ type: 'text', text: JSON.stringify(data) }]
    };
  }
);

server.registerTool(
  'place_bounty',
  {
    title: 'Place bounty',
    description: 'Set or update bounty for a question (requires API key).',
    inputSchema: {
      id: z.string().min(1),
      amount: z.number().int().min(1).max(100000),
      expiresAt: z.string().optional(),
      active: z.boolean().optional()
    }
  },
  async ({ id, amount, expiresAt, active }) => {
    if (!apiKey) {
      return {
        isError: true,
        content: [{ type: 'text', text: JSON.stringify({ error: 'Missing API key', hint: 'Get a trial key at /api/v1/auth/trial-key' }) }]
      };
    }
    const response = await apiPost(`/api/v1/questions/${id}/bounty`, { amount, expiresAt, active });
    if (!response.ok) {
      const text = await response.text();
      return {
        isError: true,
        content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to place bounty', status: response.status }) }]
      };
    }
    const data = (await response.json()) as Record<string, unknown>;
    return {
      content: [{ type: 'text', text: JSON.stringify(data) }]
    };
  }
);

server.registerTool(
  'vote_answer',
  {
    title: 'Vote answer',
    description: 'Vote +1 or -1 on an answer (requires API key).',
    inputSchema: {
      id: z.string().min(1),
      value: z.union([z.literal(1), z.literal(-1)])
    }
  },
  async ({ id, value }) => {
    if (!apiKey) {
      return {
        isError: true,
        content: [{ type: 'text', text: JSON.stringify({ error: 'Missing API key', hint: 'Get a trial key at /api/v1/auth/trial-key' }) }]
      };
    }
    const response = await apiPost(`/api/v1/answers/${id}/vote`, { value });
    if (!response.ok) {
      const text = await response.text();
      return {
        isError: true,
        content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to vote answer', status: response.status }) }]
      };
    }
    const data = (await response.json()) as Record<string, unknown>;
    return {
      content: [{ type: 'text', text: JSON.stringify(data) }]
    };
  }
);

server.registerTool(
  'accept_answer',
  {
    title: 'Accept answer',
    description: 'Accept an answer for a question (requires API key of question owner).',
    inputSchema: {
      questionId: z.string().min(1),
      answerId: z.string().min(1)
    }
  },
  async ({ questionId, answerId }) => {
    if (!apiKey) {
      return {
        isError: true,
        content: [{ type: 'text', text: JSON.stringify({ error: 'Missing API key', hint: 'Get a trial key at /api/v1/auth/trial-key' }) }]
      };
    }
    const response = await apiPost(`/api/v1/questions/${questionId}/accept/${answerId}`, {});
    if (!response.ok) {
      const text = await response.text();
      return {
        isError: true,
        content: [{ type: 'text', text: JSON.stringify({ error: text || 'Failed to accept answer', status: response.status }) }]
      };
    }
    const data = (await response.json()) as Record<string, unknown>;
    return {
      content: [{ type: 'text', text: JSON.stringify(data) }]
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
  API_KEY        Optional bearer token for write/auth endpoints (create_question/create_answer auto-mint trial key when missing)
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
