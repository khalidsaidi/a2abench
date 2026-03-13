import 'dotenv/config';
import { readFileSync } from 'node:fs';

type JsonObject = Record<string, unknown>;

type FunnelResponse = {
  since: string;
  totals: {
    queued: number;
    opened: number;
    pending: number;
    failed: number;
    answered: number;
    accepted: number;
  };
  conversion: {
    openRate: number;
    answerRateFromOpened: number;
    acceptRateFromAnswered: number;
    withinWindowRate: number;
  };
};

type ScorecardResponse = {
  summary: {
    status: string;
    passCount: number;
    failCount: number;
    score: number;
  };
  metrics: Array<{
    id: string;
    value: number;
    pass: boolean;
  }>;
};

type QueueResponse = {
  count: number;
  results: Array<{
    id: string;
    agentName: string;
    event: string;
    attemptCount: number;
    maxAttempts: number;
    createdAt: string;
  }>;
};

const API_BASE_URL = (process.env.API_BASE_URL ?? 'https://a2abench-api.web.app').replace(/\/+$/, '');
const ADMIN_API_BASE_URL = (process.env.ADMIN_API_BASE_URL ?? API_BASE_URL).replace(/\/+$/, '');
const ADMIN_TOKEN_FILE = (process.env.ADMIN_TOKEN_FILE ?? '').trim();
const ADMIN_TOKEN = resolveAdminToken();
const RUN_ONCE = boolEnv(process.env.AUTOPILOT_RUN_ONCE, false);
const LOOP_SECONDS = intEnv(process.env.AUTOPILOT_LOOP_SECONDS, 120, 15, 3600);
const IMPORT_INTERVAL_MINUTES = intEnv(process.env.AUTOPILOT_IMPORT_INTERVAL_MINUTES, 15, 1, 720);
const MAINTENANCE_INTERVAL_SECONDS = intEnv(process.env.AUTOPILOT_MAINTENANCE_INTERVAL_SECONDS, 120, 15, 1800);
const OPEN_RATE_TARGET = floatEnv(process.env.AUTOPILOT_OPEN_RATE_TARGET, 0.2, 0, 1);
const ANSWER_RATE_TARGET = floatEnv(process.env.AUTOPILOT_ANSWER_RATE_TARGET, 0.3, 0, 1);
const PENDING_QUEUE_TRIGGER = intEnv(process.env.AUTOPILOT_PENDING_QUEUE_TRIGGER, 120, 1, 5000);
const REMINDER_LIMIT = intEnv(process.env.AUTOPILOT_REMINDER_LIMIT, 250, 1, 1000);
const AUTOCLOSE_LIMIT = intEnv(process.env.AUTOPILOT_AUTOCLOSE_LIMIT, 250, 1, 1000);
const DELIVERY_LIMIT = intEnv(process.env.AUTOPILOT_DELIVERY_LIMIT, 500, 1, 500);
const REQUEUE_LIMIT = intEnv(process.env.AUTOPILOT_REQUEUE_LIMIT, 1000, 1, 2000);
const PRUNE_LIMIT = intEnv(process.env.AUTOPILOT_PRUNE_LIMIT, 500, 1, 2000);
const REQUEST_TIMEOUT_MS = intEnv(process.env.AUTOPILOT_REQUEST_TIMEOUT_MS, 180000, 5000, 900000);

function resolveAdminToken() {
  const fromEnv = (process.env.ADMIN_TOKEN ?? '').trim();
  if (fromEnv) return fromEnv;
  if (!ADMIN_TOKEN_FILE) return '';
  try {
    return readFileSync(ADMIN_TOKEN_FILE, 'utf8').trim();
  } catch {
    return '';
  }
}

if (!ADMIN_TOKEN) {
  console.error('Missing ADMIN_TOKEN or ADMIN_TOKEN_FILE. Set one before running traction-autopilot.');
  process.exit(1);
}

function boolEnv(raw: string | undefined, fallback: boolean) {
  if (raw == null) return fallback;
  const normalized = raw.trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return fallback;
}

function intEnv(raw: string | undefined, fallback: number, min: number, max: number) {
  const parsed = Number.parseInt(raw ?? `${fallback}`, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function floatEnv(raw: string | undefined, fallback: number, min: number, max: number) {
  const parsed = Number(raw ?? fallback);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(`${ADMIN_API_BASE_URL}${path}`, {
      ...init,
      signal: controller.signal,
      headers: {
        'content-type': 'application/json',
        'x-admin-token': ADMIN_TOKEN,
        ...(init?.headers ?? {})
      }
    });
  } finally {
    clearTimeout(timeout);
  }
  const text = await res.text();
  let json: unknown = text;
  try {
    json = JSON.parse(text);
  } catch {
    // keep text payload
  }
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} ${path}: ${typeof json === 'string' ? json : JSON.stringify(json)}`);
  }
  return json as T;
}

async function getFunnel() {
  return api<FunnelResponse>('/api/v1/admin/traction/funnel?days=1&externalOnly=true');
}

async function getScorecard() {
  return api<ScorecardResponse>('/api/v1/admin/traction/scorecard?days=7&externalOnly=true');
}

async function getPendingQueue() {
  return api<QueueResponse>('/api/v1/admin/delivery/queue?status=pending&event=question.created&limit=500');
}

async function postAdmin(path: string, body: JsonObject = {}) {
  return api<JsonObject>(path, {
    method: 'POST',
    body: JSON.stringify(body)
  });
}

type ActionResult = {
  name: string;
  ok: boolean;
  detail?: unknown;
  error?: string;
};

async function runAction(name: string, fn: () => Promise<unknown>): Promise<ActionResult> {
  try {
    const detail = await fn();
    return { name, ok: true, detail };
  } catch (error) {
    return {
      name,
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    };
  }
}

function summarizeQueueByAgent(queue: QueueResponse, topN = 8) {
  const counts = new Map<string, number>();
  for (const row of queue.results) {
    const key = row.agentName || 'unknown';
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([agentName, count]) => ({ agentName, count }));
}

async function main() {
  process.stdout.write(`${JSON.stringify({
    mode: RUN_ONCE ? 'once' : 'loop',
    apiBaseUrl: API_BASE_URL,
    adminApiBaseUrl: ADMIN_API_BASE_URL,
    requestTimeoutMs: REQUEST_TIMEOUT_MS,
    loopSeconds: LOOP_SECONDS,
    importIntervalMinutes: IMPORT_INTERVAL_MINUTES,
    maintenanceIntervalSeconds: MAINTENANCE_INTERVAL_SECONDS,
    targets: {
      openRate: OPEN_RATE_TARGET,
      answerRateFromOpened: ANSWER_RATE_TARGET
    },
    triggers: {
      pendingQueue: PENDING_QUEUE_TRIGGER
    }
  })}\n`);

  let iteration = 0;
  let lastImportAt = 0;
  let lastMaintenanceAt = 0;

  while (true) {
    iteration += 1;
    const startedAt = Date.now();
    const now = Date.now();

    const actions: ActionResult[] = [];
    const funnel = await getFunnel();
    const scorecard = await getScorecard();
    const pendingQueue = await getPendingQueue();

    const needsImport = now - lastImportAt >= IMPORT_INTERVAL_MINUTES * 60 * 1000;
    if (needsImport) {
      actions.push(await runAction('import_sources', () => postAdmin('/api/v1/admin/import/sources/run', { dryRun: false })));
      lastImportAt = now;
    }

    const needsMaintenance = now - lastMaintenanceAt >= MAINTENANCE_INTERVAL_SECONDS * 1000
      || pendingQueue.count >= PENDING_QUEUE_TRIGGER
      || funnel.conversion.openRate < OPEN_RATE_TARGET
      || funnel.conversion.answerRateFromOpened < ANSWER_RATE_TARGET;

    if (needsMaintenance) {
      actions.push(await runAction('delivery_process', () => postAdmin('/api/v1/admin/delivery/process', { limit: DELIVERY_LIMIT })));
      actions.push(await runAction('delivery_requeue_opened_unanswered', () => postAdmin('/api/v1/admin/delivery/requeue-opened-unanswered', { limit: REQUEUE_LIMIT, dryRun: false })));
      actions.push(await runAction('reminders_process', () => postAdmin('/api/v1/admin/reminders/process', { limit: REMINDER_LIMIT })));
      actions.push(await runAction('autoclose_process', () => postAdmin('/api/v1/admin/autoclose/process', { limit: AUTOCLOSE_LIMIT })));
      actions.push(await runAction('subscriptions_prune', () => postAdmin('/api/v1/admin/subscriptions/prune', { limit: PRUNE_LIMIT, dryRun: false })));
      lastMaintenanceAt = now;
    }

    process.stdout.write(`${JSON.stringify({
      iteration,
      startedAt: new Date(startedAt).toISOString(),
      durationMs: Date.now() - startedAt,
      observed: {
        funnelTotals: funnel.totals,
        conversion: funnel.conversion,
        scorecard: scorecard.summary,
        pendingQueueCount: pendingQueue.count,
        topPendingAgents: summarizeQueueByAgent(pendingQueue)
      },
      actions
    })}\n`);

    if (RUN_ONCE) break;
    await sleep(LOOP_SECONDS * 1000);
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
