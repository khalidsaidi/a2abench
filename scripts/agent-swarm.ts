import 'dotenv/config';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

type JsonObject = Record<string, unknown>;

type ToolResult = {
  content?: Array<{ type?: string; text?: string }>;
  isError?: boolean;
};

type QuestionSummary = {
  id: string;
  title: string;
  bodyText?: string;
  tags?: string[];
  answerCount?: number;
  createdAt?: string;
  bounty?: {
    amount?: number;
    active?: boolean;
    expiresAt?: string | null;
  } | null;
};

type QuestionThread = {
  id: string;
  title: string;
  bodyText?: string;
  tags?: string[];
  answers?: Array<{ id: string; bodyMd?: string; createdAt?: string; agentName?: string | null }>;
};

const API_BASE_URL = process.env.API_BASE_URL ?? 'https://a2abench-api.web.app';
const MCP_URL = process.env.MCP_URL ?? 'https://a2abench-mcp.web.app/mcp';
const SWARM_PREFIX = process.env.SWARM_PREFIX ?? 'a2a-swarm';
const SWARM_AGENTS = Number.parseInt(process.env.SWARM_AGENTS ?? '12', 10);
const SWARM_POLL_MS = Number.parseInt(process.env.SWARM_POLL_MS ?? '3500', 10);
const SWARM_IDLE_MS = Number.parseInt(process.env.SWARM_IDLE_MS ?? '8000', 10);
const SWARM_CLAIM_TTL_MS = Number.parseInt(process.env.SWARM_CLAIM_TTL_MS ?? '180000', 10);
const SWARM_RECENT_TTL_MS = Number.parseInt(process.env.SWARM_RECENT_TTL_MS ?? '300000', 10);
const SWARM_LOW_ANSWER_CACHE_MS = Number.parseInt(process.env.SWARM_LOW_ANSWER_CACHE_MS ?? '12000', 10);
const SWARM_USE_ANSWER_TOOL = (process.env.SWARM_USE_ANSWER_TOOL ?? 'true').toLowerCase() === 'true';
const SWARM_SEED_WHEN_IDLE = (process.env.SWARM_SEED_WHEN_IDLE ?? 'true').toLowerCase() === 'true';
const SWARM_SEED_AFTER_IDLE_LOOPS = Number.parseInt(process.env.SWARM_SEED_AFTER_IDLE_LOOPS ?? '8', 10);
const SWARM_MAX_ANSWERS_TOTAL = Number.parseInt(process.env.SWARM_MAX_ANSWERS_TOTAL ?? '0', 10);
const SWARM_API_KEY = process.env.SWARM_API_KEY ?? '';
const SWARM_ROTATE_TRIAL_ON_LIMIT = (process.env.SWARM_ROTATE_TRIAL_ON_LIMIT ?? 'true').toLowerCase() === 'true';
const SWARM_INCLUDE_LOW_ANSWER = (process.env.SWARM_INCLUDE_LOW_ANSWER ?? 'true').toLowerCase() === 'true';
const SWARM_TARGET_MAX_ANSWERS = Math.max(1, Number.parseInt(process.env.SWARM_TARGET_MAX_ANSWERS ?? '2', 10));
const SWARM_TARGET_SCAN_LIMIT = Math.min(200, Math.max(10, Number.parseInt(process.env.SWARM_TARGET_SCAN_LIMIT ?? '80', 10)));
const SWARM_ALLOW_MULTI_ANSWERS_PER_THREAD =
  (process.env.SWARM_ALLOW_MULTI_ANSWERS_PER_THREAD ?? 'false').toLowerCase() === 'true';
const SWARM_LIMIT_COOLDOWN_MS = Number.parseInt(process.env.SWARM_LIMIT_COOLDOWN_MS ?? '300000', 10);

const sharedClaims = new Map<string, { agent: string; ts: number }>();
const recentlyAnswered = new Map<string, number>();
const lowAnswerCache: { ts: number; items: QuestionSummary[] } = { ts: 0, items: [] };

let mintKeyPromise: Promise<string> | null = null;

let totalAnswers = 0;
let stopRequested = false;

function nowIso() {
  return new Date().toISOString();
}

function log(agent: string, message: string, extra?: JsonObject) {
  const payload = extra ? ` ${JSON.stringify(extra)}` : '';
  process.stdout.write(`[${nowIso()}] [${agent}] ${message}${payload}\n`);
}

function jitter(baseMs: number, spreadMs = 750) {
  return baseMs + Math.floor(Math.random() * spreadMs);
}

function readToolText(result: ToolResult): string {
  return result.content?.find((item) => item.type === 'text')?.text ?? '{}';
}

function parseToolJson(result: ToolResult): JsonObject {
  const text = readToolText(result);
  try {
    return JSON.parse(text) as JsonObject;
  } catch {
    return { raw: text };
  }
}

function pruneMaps() {
  const now = Date.now();
  for (const [id, claim] of sharedClaims.entries()) {
    if (now - claim.ts > SWARM_CLAIM_TTL_MS) sharedClaims.delete(id);
  }
  for (const [id, ts] of recentlyAnswered.entries()) {
    if (now - ts > SWARM_RECENT_TTL_MS) recentlyAnswered.delete(id);
  }
}

function tryClaimQuestion(agent: string, id: string): boolean {
  pruneMaps();
  const existing = sharedClaims.get(id);
  if (existing) return false;
  sharedClaims.set(id, { agent, ts: Date.now() });
  return true;
}

function releaseClaim(id: string) {
  sharedClaims.delete(id);
}

function markAnswered(id: string) {
  recentlyAnswered.set(id, Date.now());
}

function isRecentlyAnswered(id: string) {
  pruneMaps();
  return recentlyAnswered.has(id);
}

async function mintTrialKey(handle: string): Promise<string> {
  const response = await fetch(new URL('/api/v1/auth/trial-key', API_BASE_URL), {
    method: 'POST',
    headers: { 'content-type': 'application/json', accept: 'application/json' },
    body: JSON.stringify({ handle })
  });
  if (!response.ok) {
    throw new Error(`trial-key failed: ${response.status} ${await response.text()}`);
  }
  const json = (await response.json()) as { apiKey?: string };
  if (!json.apiKey) {
    throw new Error('trial-key missing apiKey');
  }
  return json.apiKey;
}

function isAuthOrLimitError(status?: number, errorText?: string) {
  const lower = (errorText ?? '').toLowerCase();
  return status === 401 || status === 429 || lower.includes('invalid api key') || lower.includes('limit');
}

async function mintTrialKeyResilient(agentName: string): Promise<string> {
  if (mintKeyPromise) return mintKeyPromise;
  mintKeyPromise = (async () => {
    let lastError: string | null = null;
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      try {
        const key = await mintTrialKey(agentName);
        if (attempt > 1) {
          log(agentName, 'trial key mint recovered', { attempt });
        }
        return key;
      } catch (error) {
        lastError = (error as Error).message;
        const retryable = lastError.includes('429');
        log(agentName, 'trial key mint failed', {
          attempt,
          retryable,
          error: lastError
        });
        if (!retryable || attempt === 4) break;
        await sleep(700 * attempt);
      }
    }
    throw new Error(lastError ?? 'trial key mint failed');
  })();
  try {
    return await mintKeyPromise;
  } finally {
    mintKeyPromise = null;
  }
}

async function getLowAnswerCandidates(agentName: string, limit = SWARM_TARGET_SCAN_LIMIT): Promise<QuestionSummary[]> {
  if (!SWARM_INCLUDE_LOW_ANSWER) return [];
  const now = Date.now();
  if (now - lowAnswerCache.ts < SWARM_LOW_ANSWER_CACHE_MS && lowAnswerCache.items.length > 0) {
    return lowAnswerCache.items.slice(0, limit);
  }

  const response = await fetch(new URL('/api/v1/questions', API_BASE_URL), {
    headers: {
      accept: 'application/json',
      'X-Agent-Name': agentName
    }
  });
  if (!response.ok) {
    log(agentName, 'low-answer fetch failed', { status: response.status });
    return lowAnswerCache.items.slice(0, limit);
  }

  const rows = (await response.json()) as QuestionSummary[];
  const filtered = rows
    .filter((item) => {
      const count = item.answerCount ?? 0;
      return count > 0 && count <= SWARM_TARGET_MAX_ANSWERS;
    })
    .sort((a, b) => {
      const aCount = a.answerCount ?? 0;
      const bCount = b.answerCount ?? 0;
      if (aCount !== bCount) return aCount - bCount;
      const aTs = a.createdAt ? Date.parse(a.createdAt) : 0;
      const bTs = b.createdAt ? Date.parse(b.createdAt) : 0;
      return aTs - bTs;
    });

  lowAnswerCache.ts = now;
  lowAnswerCache.items = filtered.slice(0, limit);
  return lowAnswerCache.items;
}

function chooseQuestion(items: QuestionSummary[]): QuestionSummary | null {
  const candidates = items.filter((item) => !isRecentlyAnswered(item.id));
  candidates.sort((a, b) => {
    const aBounty = a.bounty?.active ? a.bounty.amount ?? 0 : 0;
    const bBounty = b.bounty?.active ? b.bounty.amount ?? 0 : 0;
    if (bBounty !== aBounty) return bBounty - aBounty;
    const aCount = a.answerCount ?? 0;
    const bCount = b.answerCount ?? 0;
    if (aCount !== bCount) return aCount - bCount;
    const aTs = a.createdAt ? Date.parse(a.createdAt) : 0;
    const bTs = b.createdAt ? Date.parse(b.createdAt) : 0;
    return aTs - bTs;
  });
  return candidates[0] ?? null;
}

function buildQuestionSeed(agentName: string): { title: string; bodyMd: string; tags: string[] } {
  const token = randomUUID().slice(0, 8);
  const title = `Agent question ${token}: MCP integration edge-case`;
  const bodyMd = [
    `Generated by ${agentName} to keep the network active.`,
    '',
    'How should an agent handle retries when MCP `create_answer` returns transient failures?',
    '',
    'Context:',
    '- Remote MCP over HTTP',
    '- Need idempotent behavior',
    '- Want to avoid duplicate answers',
    '',
    'Please propose practical retry + dedupe strategy.'
  ].join('\n');
  return { title, bodyMd, tags: ['agent', 'mcp', 'retries'] };
}

class AgentWorker {
  private client: Client | null = null;
  private apiKey = '';
  private apiKeySource: 'trial' | 'shared' = 'trial';
  private idleLoops = 0;
  private writeCooldownUntil = 0;

  constructor(private readonly agentName: string) {}

  async start() {
    if (SWARM_API_KEY) {
      this.apiKey = SWARM_API_KEY;
      this.apiKeySource = 'shared';
    } else {
      this.apiKey = await mintTrialKeyResilient(this.agentName);
      this.apiKeySource = 'trial';
    }
    await this.connect();
    log(this.agentName, 'connected', { mcpUrl: MCP_URL, keySource: this.apiKeySource });
  }

  async stop() {
    if (this.client) {
      try {
        await this.client.close();
      } catch {
        // ignore close errors
      } finally {
        this.client = null;
      }
    }
  }

  private async connect() {
    await this.stop();
    const client = new Client({ name: this.agentName, version: '1.0.0' });
    const transport = new StreamableHTTPClientTransport(new URL(MCP_URL), {
      requestInit: {
        headers: {
          'X-Agent-Name': this.agentName,
          Authorization: `Bearer ${this.apiKey}`
        }
      }
    });
    await client.connect(transport);
    this.client = client;
  }

  private async refreshAuth(reason: string): Promise<boolean> {
    if (this.apiKeySource === 'shared' && !SWARM_ROTATE_TRIAL_ON_LIMIT) {
      return false;
    }
    try {
      this.apiKey = await mintTrialKeyResilient(this.agentName);
      this.apiKeySource = 'trial';
      await this.connect();
      log(this.agentName, 'refreshed trial key', { reason });
      return true;
    } catch (error) {
      if (SWARM_API_KEY && this.apiKeySource === 'trial') {
        this.apiKey = SWARM_API_KEY;
        this.apiKeySource = 'shared';
        await this.connect();
        log(this.agentName, 'fell back to shared key', { reason });
        return true;
      }
      log(this.agentName, 'auth refresh failed', { reason, error: (error as Error).message });
      return false;
    }
  }

  private async callTool(name: string, args: JsonObject): Promise<ToolResult> {
    if (!this.client) {
      await this.connect();
    }
    try {
      return (await this.client!.callTool({ name, arguments: args })) as ToolResult;
    } catch (error) {
      log(this.agentName, 'tool call failed; reconnecting', {
        tool: name,
        error: (error as Error).message
      });
      await this.connect();
      return (await this.client!.callTool({ name, arguments: args })) as ToolResult;
    }
  }

  private async getUnanswered(limit = 25): Promise<QuestionSummary[]> {
    const result = await this.callTool('unanswered', { limit });
    const json = parseToolJson(result);
    const list = Array.isArray(json.results) ? (json.results as QuestionSummary[]) : [];
    return list;
  }

  private async fetchThread(id: string): Promise<QuestionThread | null> {
    const result = await this.callTool('fetch', { id });
    const json = parseToolJson(result);
    if ((json.error as string | undefined) && !json.id) return null;
    return json as unknown as QuestionThread;
  }

  private async synthesizeMarkdown(question: QuestionSummary): Promise<string | null> {
    if (!SWARM_USE_ANSWER_TOOL) return null;
    const query = `${question.title}\n${question.bodyText ?? ''}`.slice(0, 600);
    const result = await this.callTool('answer', {
      query,
      top_k: 3,
      include_evidence: true,
      mode: 'balanced'
    });
    const json = parseToolJson(result);
    const answerMarkdown = json.answer_markdown;
    if (typeof answerMarkdown !== 'string' || answerMarkdown.trim().length < 20) {
      return null;
    }
    return answerMarkdown.trim();
  }

  private buildFallbackMarkdown(question: QuestionSummary, thread: QuestionThread | null): string {
    const context = (thread?.bodyText ?? question.bodyText ?? '').slice(0, 260);
    return [
      `Agent response from \`${this.agentName}\`.`,
      '',
      `I reviewed: **${question.title}**.`,
      '',
      context
        ? `Current context: ${context}`
        : 'Need details to answer precisely. Include logs, versions, and exact expected vs actual behavior.',
      '',
      'Suggested next step:',
      '1. Share a minimal reproducible case.',
      '2. Include runtime/tool versions.',
      '3. Include exact command and error output.'
    ].join('\n');
  }

  private async createAnswer(question: QuestionSummary, bodyMd: string): Promise<{ ok: boolean; answerId?: string; status?: number }> {
    const result = await this.callTool('create_answer', { id: question.id, bodyMd });
    const json = parseToolJson(result);
    const status = typeof json.status === 'number' ? json.status : undefined;
    const errorText = typeof json.error === 'string' ? json.error : '';
    if (typeof json.id === 'string' && json.id.length > 0) {
      return { ok: true, answerId: json.id };
    }
    if (isAuthOrLimitError(status, errorText)) {
      const refreshed = await this.refreshAuth('create_answer');
      if (!refreshed) {
        if (status === 429 || errorText.toLowerCase().includes('limit')) {
          this.writeCooldownUntil = Date.now() + SWARM_LIMIT_COOLDOWN_MS;
        }
        return { ok: false, status };
      }
      const retried = await this.callTool('create_answer', { id: question.id, bodyMd });
      const retriedJson = parseToolJson(retried);
      if (typeof retriedJson.id === 'string' && retriedJson.id.length > 0) {
        return { ok: true, answerId: retriedJson.id };
      }
      return {
        ok: false,
        status: typeof retriedJson.status === 'number' ? retriedJson.status : undefined
      };
    }
    return { ok: false, status };
  }

  private async maybeSeedQuestion() {
    if (!SWARM_SEED_WHEN_IDLE || this.idleLoops < SWARM_SEED_AFTER_IDLE_LOOPS) return;
    this.idleLoops = 0;
    const payload = buildQuestionSeed(this.agentName);
    const result = await this.callTool('create_question', payload);
    const json = parseToolJson(result);
    if (typeof json.id === 'string') {
      log(this.agentName, 'seeded question', { questionId: json.id, title: payload.title });
      return;
    }
    const status = typeof json.status === 'number' ? json.status : undefined;
    const errorText = typeof json.error === 'string' ? json.error : '';
    if (isAuthOrLimitError(status, errorText)) {
      const refreshed = await this.refreshAuth('create_question');
      if (!refreshed) {
        if (status === 429 || errorText.toLowerCase().includes('limit')) {
          this.writeCooldownUntil = Date.now() + SWARM_LIMIT_COOLDOWN_MS;
        }
        return;
      }
      const retried = await this.callTool('create_question', payload);
      const retriedJson = parseToolJson(retried);
      if (typeof retriedJson.id === 'string') {
        log(this.agentName, 'seeded question', { questionId: retriedJson.id, title: payload.title, retried: true });
      }
    }
  }

  async runLoop() {
    while (!stopRequested) {
      try {
        if (Date.now() < this.writeCooldownUntil) {
          await sleep(jitter(SWARM_IDLE_MS));
          continue;
        }

        const unanswered = await this.getUnanswered(30);
        const lowAnswer = await getLowAnswerCandidates(this.agentName, SWARM_TARGET_SCAN_LIMIT);
        const merged = new Map<string, QuestionSummary>();
        for (const item of unanswered) merged.set(item.id, item);
        for (const item of lowAnswer) {
          if (!merged.has(item.id)) merged.set(item.id, item);
        }
        const question = chooseQuestion([...merged.values()]);

        if (!question) {
          this.idleLoops += 1;
          await this.maybeSeedQuestion();
          await sleep(jitter(SWARM_IDLE_MS));
          continue;
        }

        if (!tryClaimQuestion(this.agentName, question.id)) {
          await sleep(jitter(SWARM_POLL_MS));
          continue;
        }

        try {
          const thread = await this.fetchThread(question.id);
          if (
            thread &&
            !SWARM_ALLOW_MULTI_ANSWERS_PER_THREAD &&
            (thread.answers ?? []).some((answer) => (answer.agentName ?? '').startsWith(SWARM_PREFIX))
          ) {
            markAnswered(question.id);
            await sleep(jitter(SWARM_POLL_MS));
            continue;
          }

          if (thread && (thread.answers?.length ?? 0) > SWARM_TARGET_MAX_ANSWERS) {
            markAnswered(question.id);
            await sleep(jitter(SWARM_POLL_MS));
            continue;
          }

          const markdown = (await this.synthesizeMarkdown(question)) ?? this.buildFallbackMarkdown(question, thread);
          const created = await this.createAnswer(question, markdown);
          if (created.ok) {
            markAnswered(question.id);
            this.idleLoops = 0;
            totalAnswers += 1;
            log(this.agentName, 'answered', {
              questionId: question.id,
              answerId: created.answerId,
              totalAnswers
            });
            if (SWARM_MAX_ANSWERS_TOTAL > 0 && totalAnswers >= SWARM_MAX_ANSWERS_TOTAL) {
              stopRequested = true;
            }
          } else {
            if (created.status === 429) {
              markAnswered(question.id);
            }
            log(this.agentName, 'answer failed', { questionId: question.id, status: created.status ?? null });
          }
        } finally {
          releaseClaim(question.id);
        }

        await sleep(jitter(SWARM_POLL_MS));
      } catch (error) {
        log(this.agentName, 'loop error', { error: (error as Error).message });
        await sleep(jitter(SWARM_IDLE_MS));
      }
    }
  }
}

async function main() {
  const count = Number.isFinite(SWARM_AGENTS) && SWARM_AGENTS > 0 ? SWARM_AGENTS : 8;
  const workers = Array.from({ length: count }, (_, idx) => new AgentWorker(`${SWARM_PREFIX}-${idx + 1}`));
  const liveWorkers: AgentWorker[] = [];

  process.on('SIGINT', () => {
    stopRequested = true;
  });
  process.on('SIGTERM', () => {
    stopRequested = true;
  });

  log('swarm', 'starting', {
    agents: count,
    mcpUrl: MCP_URL,
    apiBaseUrl: API_BASE_URL,
    sharedApiKey: SWARM_API_KEY ? true : false,
    useAnswerTool: SWARM_USE_ANSWER_TOOL,
    seedWhenIdle: SWARM_SEED_WHEN_IDLE,
    maxAnswersTotal: SWARM_MAX_ANSWERS_TOTAL
  });

  for (const worker of workers) {
    try {
      await worker.start();
      liveWorkers.push(worker);
    } catch (error) {
      log('swarm', 'worker failed to start', { error: (error as Error).message });
    }
    await sleep(120);
  }

  if (liveWorkers.length === 0) {
    throw new Error('no agents could start');
  }

  await Promise.all(liveWorkers.map((worker) => worker.runLoop()));

  for (const worker of liveWorkers) {
    await worker.stop();
  }

  log('swarm', 'stopped', { totalAnswers });
}

main().catch((error) => {
  log('swarm', 'fatal', { error: (error as Error).message });
  process.exit(1);
});
