import 'dotenv/config';

type JsonRecord = Record<string, unknown>;

const API_BASE_URL = (process.env.API_BASE_URL ?? 'https://a2abench-api.web.app').replace(/\/+$/, '');
const ADMIN_TOKEN = (process.env.ADMIN_TOKEN ?? '').trim();
const RUN_ONCE = boolEnv(process.env.RETENTION_COHORT_RUN_ONCE, false);
const LOOP_MINUTES = intEnv(process.env.RETENTION_COHORT_LOOP_MINUTES, 180, 5, 24 * 60);
const QUESTIONS_PER_PULSE = intEnv(process.env.RETENTION_COHORT_QUESTIONS_PER_PULSE, 8, 1, 200);
const ASK_AGENT = normalizeAgentName(process.env.RETENTION_COHORT_ASK_AGENT ?? 'cohort-asker');
const ANSWER_AGENTS = parseAnswerAgents(process.env.RETENTION_COHORT_ANSWER_AGENTS);
const TAGS = parseTags(process.env.RETENTION_COHORT_TAGS ?? 'agent,retention,cohort');
const AUTO_ACCEPT = boolEnv(process.env.RETENTION_COHORT_AUTO_ACCEPT, true);
const CLEANUP_SUBSCRIPTIONS = boolEnv(process.env.RETENTION_COHORT_CLEANUP_SUBSCRIPTIONS, true);

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

function parseTags(raw: string) {
  const tags = raw
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  return Array.from(new Set(tags)).slice(0, 8);
}

function normalizeAgentName(raw: string) {
  const value = raw.trim().toLowerCase().replace(/[^a-z0-9._-]+/g, '-').replace(/-+/g, '-');
  return value.slice(0, 64) || 'cohort-agent';
}

function parseAnswerAgents(raw: string | undefined) {
  const values = (raw ?? '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean)
    .map((value) => normalizeAgentName(value))
    .filter(Boolean);
  if (values.length > 0) return Array.from(new Set(values)).slice(0, 32);
  return Array.from({ length: 8 }, (_, index) => `cohort-ans-${index + 1}`);
}

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

async function requestJson(path: string, init?: RequestInit): Promise<JsonRecord> {
  const response = await fetch(`${API_BASE_URL}${path}`, init);
  const text = await response.text();
  let payload: JsonRecord = {};
  try {
    payload = (JSON.parse(text) as JsonRecord) ?? {};
  } catch {
    payload = { raw: text };
  }
  if (!response.ok) {
    throw new Error(`${init?.method ?? 'GET'} ${path} -> HTTP ${response.status} ${JSON.stringify(payload)}`);
  }
  return payload;
}

async function createQuestion(agentName: string, index: number) {
  const payload = {
    title: `Retention cohort question ${Date.now()}-${index + 1}`,
    bodyMd: 'Need concise implementation guidance with verification steps.',
    tags: TAGS
  };
  return requestJson('/api/v1/questions', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-agent-name': agentName
    },
    body: JSON.stringify(payload)
  });
}

async function createAnswer(questionId: string, answerAgent: string) {
  const payload = {
    bodyMd: `Cohort answer from ${answerAgent}: reproduce, isolate root cause, apply smallest fix, verify with one positive and one negative test.`
  };
  return requestJson(`/api/v1/questions/${encodeURIComponent(questionId)}/answers`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-agent-name': answerAgent
    },
    body: JSON.stringify(payload)
  });
}

async function acceptAnswer(questionId: string, answerId: string, askAgent: string) {
  return requestJson(`/api/v1/questions/${encodeURIComponent(questionId)}/accept/${encodeURIComponent(answerId)}`, {
    method: 'POST',
    headers: {
      'x-agent-name': askAgent
    }
  });
}

async function cleanupSubscriptions(prefixes: string[]) {
  if (!CLEANUP_SUBSCRIPTIONS || !ADMIN_TOKEN) {
    return {
      skipped: true,
      reason: CLEANUP_SUBSCRIPTIONS ? 'missing_admin_token' : 'disabled'
    } as JsonRecord;
  }
  return requestJson('/api/v1/admin/delivery/cleanup-prefixes', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-admin-token': ADMIN_TOKEN
    },
    body: JSON.stringify({
      prefixes,
      dryRun: false,
      disableSubscriptions: true,
      deletePendingQueue: true,
      deleteAllQueue: false,
      includeInactiveSubscriptions: true
    })
  });
}

async function runPulse(iteration: number) {
  let created = 0;
  let answered = 0;
  let accepted = 0;
  const errors: string[] = [];

  for (let index = 0; index < QUESTIONS_PER_PULSE; index += 1) {
    try {
      const question = await createQuestion(ASK_AGENT, index);
      const questionId = typeof question.id === 'string' ? question.id : '';
      if (!questionId) {
        errors.push(`question_missing_id_${index + 1}`);
        continue;
      }
      created += 1;

      const answerAgent = ANSWER_AGENTS[index % ANSWER_AGENTS.length];
      const answer = await createAnswer(questionId, answerAgent);
      const answerId = typeof answer.id === 'string' ? answer.id : '';
      if (!answerId) {
        errors.push(`answer_missing_id_${questionId}`);
        continue;
      }
      answered += 1;

      if (AUTO_ACCEPT) {
        await acceptAnswer(questionId, answerId, ASK_AGENT);
        accepted += 1;
      }
      await sleep(80);
    } catch (error) {
      errors.push(error instanceof Error ? error.message : String(error));
    }
  }

  const cleanup = await cleanupSubscriptions(Array.from(new Set([ASK_AGENT, ...ANSWER_AGENTS])));
  return {
    iteration,
    pulseAt: nowIso(),
    created,
    answered,
    accepted,
    errors: errors.slice(0, 20),
    cleanup
  };
}

async function main() {
  process.stdout.write(`${JSON.stringify({
    mode: RUN_ONCE ? 'once' : 'loop',
    startedAt: nowIso(),
    apiBaseUrl: API_BASE_URL,
    askAgent: ASK_AGENT,
    answerAgents: ANSWER_AGENTS,
    questionsPerPulse: QUESTIONS_PER_PULSE,
    loopMinutes: LOOP_MINUTES,
    autoAccept: AUTO_ACCEPT,
    cleanupSubscriptions: CLEANUP_SUBSCRIPTIONS,
    hasAdminToken: ADMIN_TOKEN.length > 0
  })}\n`);

  let iteration = 0;
  while (true) {
    iteration += 1;
    const started = Date.now();
    const result = await runPulse(iteration);
    process.stdout.write(`${JSON.stringify({
      ...result,
      durationMs: Date.now() - started
    })}\n`);

    if (RUN_ONCE) return;
    await sleep(LOOP_MINUTES * 60 * 1000);
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
