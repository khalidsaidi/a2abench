import 'dotenv/config';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import rateLimit from '@fastify/rate-limit';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { PrismaClient, Prisma } from '@prisma/client';
import { markdownToText } from './markdown.js';
import { ANSWER_REQUEST_SCHEMA, runAnswer, createDefaultLlmFromEnv, createLlmFromByok } from './answer.js';
import { z } from 'zod';
import crypto from 'crypto';

const prisma = new PrismaClient();
const fastify = Fastify({ logger: true, trustProxy: true, ignoreTrailingSlash: true });

const PORT = Number(process.env.PORT ?? 3000);
const ADMIN_TOKEN = process.env.ADMIN_TOKEN ?? '';
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL ?? (process.env.NODE_ENV === 'production' ? 'https://a2abench-api.web.app' : '')).trim();
const ADMIN_DASH_USER = process.env.ADMIN_DASH_USER ?? '';
const ADMIN_DASH_PASS = process.env.ADMIN_DASH_PASS ?? '';
const AGENT_OPEN_MODE = (process.env.AGENT_OPEN_MODE ?? 'true').toLowerCase() === 'true';
const TRIAL_KEY_TTL_HOURS = Number(process.env.TRIAL_KEY_TTL_HOURS ?? 24);
const TRIAL_DAILY_WRITE_LIMIT = Number(process.env.TRIAL_DAILY_WRITE_LIMIT ?? (AGENT_OPEN_MODE ? 400 : 20));
const TRIAL_DAILY_QUESTION_LIMIT = Number(process.env.TRIAL_DAILY_QUESTION_LIMIT ?? (AGENT_OPEN_MODE ? 120 : 5));
const TRIAL_DAILY_ANSWER_LIMIT = Number(process.env.TRIAL_DAILY_ANSWER_LIMIT ?? (AGENT_OPEN_MODE ? 400 : 20));
const TRIAL_KEY_RATE_LIMIT_MAX = Number(process.env.TRIAL_KEY_RATE_LIMIT_MAX ?? (AGENT_OPEN_MODE ? 100 : 5));
const TRIAL_KEY_RATE_LIMIT_WINDOW = process.env.TRIAL_KEY_RATE_LIMIT_WINDOW ?? '1 day';
const TRIAL_KEY_ACTOR_TYPE = process.env.TRIAL_KEY_ACTOR_TYPE ?? 'unknown';
const TRIAL_AUTO_SUBSCRIBE = (process.env.TRIAL_AUTO_SUBSCRIBE ?? 'true').toLowerCase() === 'true';
const TRIAL_AUTO_SUBSCRIBE_EVENTS_RAW = (process.env.TRIAL_AUTO_SUBSCRIBE_EVENTS
  ?? 'question.created,question.needs_acceptance,question.accepted')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const TRIAL_AUTO_SUBSCRIBE_TAGS_RAW = (process.env.TRIAL_AUTO_SUBSCRIBE_TAGS ?? '')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const KEYLESS_AUTH_ENABLED = (process.env.KEYLESS_AUTH_ENABLED ?? 'true').toLowerCase() === 'true';
const KEYLESS_AUTH_ALLOW_ANONYMOUS = (process.env.KEYLESS_AUTH_ALLOW_ANONYMOUS ?? (AGENT_OPEN_MODE ? 'true' : 'false')).toLowerCase() === 'true';
const KEYLESS_AUTH_ACTOR_TYPE = normalizeActorType(process.env.KEYLESS_AUTH_ACTOR_TYPE ?? 'public_external');
const AUTH_INVALID_BEARER_FALLBACK_TO_KEYLESS = (process.env.AUTH_INVALID_BEARER_FALLBACK_TO_KEYLESS ?? (AGENT_OPEN_MODE ? 'true' : 'false')).toLowerCase() === 'true';
const KEYLESS_DAILY_WRITE_LIMIT = Number(process.env.KEYLESS_DAILY_WRITE_LIMIT ?? (AGENT_OPEN_MODE ? 5000 : 200));
const KEYLESS_DAILY_QUESTION_LIMIT = Number(process.env.KEYLESS_DAILY_QUESTION_LIMIT ?? (AGENT_OPEN_MODE ? 1200 : 80));
const KEYLESS_DAILY_ANSWER_LIMIT = Number(process.env.KEYLESS_DAILY_ANSWER_LIMIT ?? (AGENT_OPEN_MODE ? 5000 : 200));
const KEYLESS_MAX_IDENTITIES_PER_IP_PER_DAY = Number(process.env.KEYLESS_MAX_IDENTITIES_PER_IP_PER_DAY ?? 0);
const KEYLESS_AUTO_SUBSCRIBE = (process.env.KEYLESS_AUTO_SUBSCRIBE ?? 'true').toLowerCase() === 'true';
const AGENT_QUICKSTART_CANDIDATES = Math.max(10, Number(process.env.AGENT_QUICKSTART_CANDIDATES ?? 200));
const AUTO_CLOSE_ENABLED = (process.env.AUTO_CLOSE_ENABLED ?? 'true').toLowerCase() === 'true';
const AUTO_CLOSE_AFTER_HOURS = Math.max(1, Number(process.env.AUTO_CLOSE_AFTER_HOURS ?? (AGENT_OPEN_MODE ? 6 : 72)));
const AUTO_CLOSE_MIN_ANSWER_AGE_HOURS = Math.max(1, Number(process.env.AUTO_CLOSE_MIN_ANSWER_AGE_HOURS ?? (AGENT_OPEN_MODE ? 1 : 24)));
const AUTO_CLOSE_AFTER_MINUTES = Math.max(1, Number(process.env.AUTO_CLOSE_AFTER_MINUTES ?? (AGENT_OPEN_MODE ? 5 : AUTO_CLOSE_AFTER_HOURS * 60)));
const AUTO_CLOSE_MIN_ANSWER_AGE_MINUTES = Math.max(1, Number(process.env.AUTO_CLOSE_MIN_ANSWER_AGE_MINUTES ?? (AGENT_OPEN_MODE ? 2 : AUTO_CLOSE_MIN_ANSWER_AGE_HOURS * 60)));
const AUTO_CLOSE_PROCESS_LIMIT = Math.max(1, Number(process.env.AUTO_CLOSE_PROCESS_LIMIT ?? 100));
const AUTO_CLOSE_LOOP_INTERVAL_MS = Math.max(10_000, Number(process.env.AUTO_CLOSE_LOOP_INTERVAL_MS ?? (AGENT_OPEN_MODE ? 60_000 : 300_000)));
const AUTO_CLOSE_AGENT_NAME = normalizeAgentOrNull(process.env.AUTO_CLOSE_AGENT_NAME) ?? 'system-autoclose';
const IMPORT_QUALITY_GATE_ENABLED = (process.env.IMPORT_QUALITY_GATE_ENABLED ?? 'true').toLowerCase() === 'true';
const CAPTURE_AGENT_PAYLOADS = (process.env.CAPTURE_AGENT_PAYLOADS ?? '').toLowerCase() === 'true';
const AGENT_PAYLOAD_TTL_HOURS = Number(process.env.AGENT_PAYLOAD_TTL_HOURS ?? 24);
const AGENT_PAYLOAD_MAX_EVENTS = Number(process.env.AGENT_PAYLOAD_MAX_EVENTS ?? 1000);
const AGENT_PAYLOAD_MAX_BYTES = Number(process.env.AGENT_PAYLOAD_MAX_BYTES ?? 16_384);
const AGENT_EVENT_TOKEN = process.env.AGENT_EVENT_TOKEN ?? '';
const LLM_CLIENT = createDefaultLlmFromEnv();
const LLM_ENABLED = (process.env.LLM_ENABLED ?? '').toLowerCase() === 'true';
const LLM_ALLOW_BYOK = (process.env.LLM_ALLOW_BYOK ?? '').toLowerCase() === 'true';
const LLM_REQUIRE_API_KEY = (process.env.LLM_REQUIRE_API_KEY ?? 'true').toLowerCase() === 'true';
const LLM_AGENT_ALLOWLIST = new Set(
  (process.env.LLM_AGENT_ALLOWLIST ?? '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
);
const LLM_DAILY_LIMIT = Number(process.env.LLM_DAILY_LIMIT ?? 50);
const llmUsage = new Map<string, { dateKey: string; count: number }>();
const USAGE_SUMMARY_FRESH_MS = Number(process.env.USAGE_SUMMARY_FRESH_MS ?? 60_000);
const USAGE_LOG_BUFFER_MAX = Math.max(200, Number(process.env.USAGE_LOG_BUFFER_MAX ?? 5000));
const USAGE_LOG_FLUSH_BATCH_SIZE = Math.max(25, Number(process.env.USAGE_LOG_FLUSH_BATCH_SIZE ?? 250));
const USAGE_LOG_FLUSH_INTERVAL_MS = Math.max(250, Number(process.env.USAGE_LOG_FLUSH_INTERVAL_MS ?? 1000));
const usageSummaryCache = new Map<string, { updatedAt: number; value: unknown }>();
const usageSummaryInflight = new Map<string, Promise<unknown>>();
const QUESTION_CLAIM_TTL_MINUTES = Number(process.env.QUESTION_CLAIM_TTL_MINUTES ?? 30);
const QUESTION_CLAIM_MIN_MINUTES = Number(process.env.QUESTION_CLAIM_MIN_MINUTES ?? 5);
const QUESTION_CLAIM_MAX_MINUTES = Number(process.env.QUESTION_CLAIM_MAX_MINUTES ?? 240);
const DELIVERY_MAX_ATTEMPTS = Math.max(1, Number(process.env.DELIVERY_MAX_ATTEMPTS ?? 6));
const DELIVERY_RETRY_BASE_MS = Math.max(1000, Number(process.env.DELIVERY_RETRY_BASE_MS ?? 15_000));
const DELIVERY_RETRY_MAX_MS = Math.max(DELIVERY_RETRY_BASE_MS, Number(process.env.DELIVERY_RETRY_MAX_MS ?? 3_600_000));
const DELIVERY_PROCESS_LIMIT = Math.max(1, Number(process.env.DELIVERY_PROCESS_LIMIT ?? 100));
const DELIVERY_REQUIRE_RECENT_ACTIVITY = (process.env.DELIVERY_REQUIRE_RECENT_ACTIVITY ?? 'true').toLowerCase() === 'true';
const DELIVERY_ACTIVE_WEBHOOK_WINDOW_HOURS = Math.max(1, Number(process.env.DELIVERY_ACTIVE_WEBHOOK_WINDOW_HOURS ?? 24));
const DELIVERY_ACTIVE_INBOX_WINDOW_MINUTES = Math.max(1, Number(process.env.DELIVERY_ACTIVE_INBOX_WINDOW_MINUTES ?? (AGENT_OPEN_MODE ? 5 : 15)));
const DELIVERY_NEW_SUBSCRIPTION_GRACE_MINUTES = Math.max(1, Number(process.env.DELIVERY_NEW_SUBSCRIPTION_GRACE_MINUTES ?? (AGENT_OPEN_MODE ? 10 : 120)));
const DELIVERY_MAX_PENDING_PER_SUBSCRIPTION = Math.max(0, Number(process.env.DELIVERY_MAX_PENDING_PER_SUBSCRIPTION ?? (AGENT_OPEN_MODE ? 12 : 200)));
const DELIVERY_REQUEUE_OPENED_ENABLED = (process.env.DELIVERY_REQUEUE_OPENED_ENABLED ?? 'true').toLowerCase() === 'true';
const DELIVERY_REQUEUE_AFTER_MINUTES = Math.max(1, Number(process.env.DELIVERY_REQUEUE_AFTER_MINUTES ?? 6));
const DELIVERY_REQUEUE_MAX_PER_QUESTION_SUBSCRIPTION = Math.max(1, Number(process.env.DELIVERY_REQUEUE_MAX_PER_QUESTION_SUBSCRIPTION ?? (AGENT_OPEN_MODE ? 2 : 5)));
const DELIVERY_REQUEUE_SCAN_LIMIT = Math.max(1, Math.min(2000, Number(process.env.DELIVERY_REQUEUE_SCAN_LIMIT ?? (AGENT_OPEN_MODE ? 1200 : 400))));
const DELIVERY_REQUEUE_LOOP_INTERVAL_MS = Math.max(15_000, Number(process.env.DELIVERY_REQUEUE_LOOP_INTERVAL_MS ?? 60_000));
const JOB_DISCOVERY_AUTO_SUBSCRIBE = (process.env.JOB_DISCOVERY_AUTO_SUBSCRIBE ?? (AGENT_OPEN_MODE ? 'true' : 'false')).toLowerCase() === 'true';
const ACCEPTANCE_REMINDER_STAGES_HOURS = (process.env.ACCEPTANCE_REMINDER_STAGES_HOURS ?? (AGENT_OPEN_MODE ? '1,6,24' : '1,24,72'))
  .split(',')
  .map((value) => Number(value.trim()))
  .filter((value) => Number.isFinite(value) && value > 0)
  .map((value) => Math.round(value));
const ACCEPTANCE_REMINDER_LIMIT = Math.max(1, Number(process.env.ACCEPTANCE_REMINDER_LIMIT ?? 200));
const ACCEPT_LINK_SECRET = process.env.ACCEPT_LINK_SECRET ?? ADMIN_TOKEN;
const ACCEPT_LINK_TTL_MINUTES = Math.max(5, Number(process.env.ACCEPT_LINK_TTL_MINUTES ?? 7 * 24 * 60));
const STARTER_BONUS_CREDITS = Math.max(0, Number(process.env.STARTER_BONUS_CREDITS ?? 30));
const AGENT_IDENTITY_REQUIRE_HEADER_FOR_WRITES = (process.env.AGENT_IDENTITY_REQUIRE_HEADER_FOR_WRITES ?? 'false').toLowerCase() === 'true';
const AGENT_IDENTITY_ENFORCE_BOUND_MATCH = (process.env.AGENT_IDENTITY_ENFORCE_BOUND_MATCH ?? 'true').toLowerCase() === 'true';
const AGENT_IDENTITY_AUTO_BIND_ON_FIRST_WRITE = (process.env.AGENT_IDENTITY_AUTO_BIND_ON_FIRST_WRITE ?? 'true').toLowerCase() === 'true';
const AGENT_SIGNATURE_ENFORCE_WRITES = (process.env.AGENT_SIGNATURE_ENFORCE_WRITES ?? 'false').toLowerCase() === 'true';
const AGENT_SIGNATURE_MAX_SKEW_SECONDS = Math.max(10, Number(process.env.AGENT_SIGNATURE_MAX_SKEW_SECONDS ?? 300));
const EXTERNAL_TRACTION_ACTOR_TYPES = new Set(
  (process.env.EXTERNAL_TRACTION_ACTOR_TYPES ?? 'pilot_external,public_external')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
);
const DELIVERY_LOOP_ENABLED = (process.env.DELIVERY_LOOP_ENABLED ?? 'true').toLowerCase() === 'true';
const DELIVERY_LOOP_INTERVAL_MS = Math.max(1000, Number(process.env.DELIVERY_LOOP_INTERVAL_MS ?? 5000));
const REMINDER_LOOP_ENABLED = (process.env.REMINDER_LOOP_ENABLED ?? 'true').toLowerCase() === 'true';
const REMINDER_LOOP_INTERVAL_MS = Math.max(5000, Number(process.env.REMINDER_LOOP_INTERVAL_MS ?? 60_000));
const PUSH_SOLVABILITY_FILTER_ENABLED = (process.env.PUSH_SOLVABILITY_FILTER_ENABLED ?? 'true').toLowerCase() === 'true';
const PUSH_SOLVABILITY_MIN_SCORE = Math.max(0, Math.min(100, Number(process.env.PUSH_SOLVABILITY_MIN_SCORE ?? (AGENT_OPEN_MODE ? 42 : 56))));
const PUSH_SOLVABILITY_UNSCOPED_MIN_SCORE = Math.max(
  PUSH_SOLVABILITY_MIN_SCORE,
  Math.min(100, Number(process.env.PUSH_SOLVABILITY_UNSCOPED_MIN_SCORE ?? (AGENT_OPEN_MODE ? 58 : 64)))
);
const NEXT_BEST_JOB_MIN_SOLVABILITY = Math.max(0, Math.min(100, Number(process.env.NEXT_BEST_JOB_MIN_SOLVABILITY ?? 40)));
const NEXT_JOB_FIRST_TOUCH_MODE_ENABLED = (process.env.NEXT_JOB_FIRST_TOUCH_MODE_ENABLED ?? 'true').toLowerCase() === 'true';
const NEXT_JOB_FIRST_TOUCH_MAX_ANSWERS = Math.max(0, Number(process.env.NEXT_JOB_FIRST_TOUCH_MAX_ANSWERS ?? 2));
const NEXT_JOB_FIRST_TOUCH_MAX_WRITES_30D = Math.max(0, Number(process.env.NEXT_JOB_FIRST_TOUCH_MAX_WRITES_30D ?? 12));
const NEXT_JOB_FIRST_TOUCH_MIN_SOLVABILITY = Math.max(
  0,
  Math.min(NEXT_BEST_JOB_MIN_SOLVABILITY, Number(process.env.NEXT_JOB_FIRST_TOUCH_MIN_SOLVABILITY ?? 28))
);
const NEXT_JOB_GUARDRAIL_ENABLED = (process.env.NEXT_JOB_GUARDRAIL_ENABLED ?? 'true').toLowerCase() === 'true';
const NEXT_JOB_GUARDRAIL_INTERVAL_MS = Math.max(60_000, Number(process.env.NEXT_JOB_GUARDRAIL_INTERVAL_MS ?? 900_000));
const NEXT_JOB_GUARDRAIL_WINDOW_MINUTES = Math.max(5, Number(process.env.NEXT_JOB_GUARDRAIL_WINDOW_MINUTES ?? 15));
const NEXT_JOB_GUARDRAIL_MIN_STRICT_WRITES = Math.max(1, Number(process.env.NEXT_JOB_GUARDRAIL_MIN_STRICT_WRITES ?? 20));
const NEXT_JOB_GUARDRAIL_TRIGGER_ANSWER_RATE = Math.max(
  0,
  Math.min(1, Number(process.env.NEXT_JOB_GUARDRAIL_TRIGGER_ANSWER_RATE ?? 0.2))
);
const NEXT_JOB_GUARDRAIL_RECOVER_ANSWER_RATE = Math.max(
  NEXT_JOB_GUARDRAIL_TRIGGER_ANSWER_RATE,
  Math.min(1, Number(process.env.NEXT_JOB_GUARDRAIL_RECOVER_ANSWER_RATE ?? 0.35))
);
const NEXT_JOB_GUARDRAIL_STICKY_MINUTES = Math.max(1, Number(process.env.NEXT_JOB_GUARDRAIL_STICKY_MINUTES ?? 30));
const NEXT_JOB_GUARDRAIL_EASY_MIN_SOLVABILITY = Math.max(
  0,
  Math.min(NEXT_BEST_JOB_MIN_SOLVABILITY, Number(process.env.NEXT_JOB_GUARDRAIL_EASY_MIN_SOLVABILITY ?? 25))
);
const SUBSCRIPTION_PRUNE_ENABLED = (process.env.SUBSCRIPTION_PRUNE_ENABLED ?? 'true').toLowerCase() === 'true';
const SUBSCRIPTION_PRUNE_INTERVAL_MS = Math.max(30_000, Number(process.env.SUBSCRIPTION_PRUNE_INTERVAL_MS ?? (AGENT_OPEN_MODE ? 60_000 : 300_000)));
const SUBSCRIPTION_PRUNE_WINDOW_HOURS = Math.max(1, Number(process.env.SUBSCRIPTION_PRUNE_WINDOW_HOURS ?? (AGENT_OPEN_MODE ? 6 : 24)));
const SUBSCRIPTION_PRUNE_STALE_HOURS = Math.max(1, Number(process.env.SUBSCRIPTION_PRUNE_STALE_HOURS ?? (AGENT_OPEN_MODE ? 1 : 6)));
const SUBSCRIPTION_PRUNE_MIN_AGE_HOURS = Math.max(1, Number(process.env.SUBSCRIPTION_PRUNE_MIN_AGE_HOURS ?? (AGENT_OPEN_MODE ? 1 : 2)));
const SUBSCRIPTION_PRUNE_WINDOW_MINUTES = Math.max(5, Number(process.env.SUBSCRIPTION_PRUNE_WINDOW_MINUTES ?? (AGENT_OPEN_MODE ? 60 : SUBSCRIPTION_PRUNE_WINDOW_HOURS * 60)));
const SUBSCRIPTION_PRUNE_STALE_MINUTES = Math.max(1, Number(process.env.SUBSCRIPTION_PRUNE_STALE_MINUTES ?? (AGENT_OPEN_MODE ? 15 : SUBSCRIPTION_PRUNE_STALE_HOURS * 60)));
const SUBSCRIPTION_PRUNE_MIN_AGE_MINUTES = Math.max(1, Number(process.env.SUBSCRIPTION_PRUNE_MIN_AGE_MINUTES ?? (AGENT_OPEN_MODE ? 15 : SUBSCRIPTION_PRUNE_MIN_AGE_HOURS * 60)));
const SUBSCRIPTION_PRUNE_MIN_QUEUED = Math.max(1, Number(process.env.SUBSCRIPTION_PRUNE_MIN_QUEUED ?? (AGENT_OPEN_MODE ? 1 : 12)));
const SUBSCRIPTION_PRUNE_MAX_FAILED = Math.max(1, Number(process.env.SUBSCRIPTION_PRUNE_MAX_FAILED ?? (AGENT_OPEN_MODE ? 3 : 8)));
const SUBSCRIPTION_PRUNE_MIN_OPEN_RATE = Math.max(0, Math.min(1, Number(process.env.SUBSCRIPTION_PRUNE_MIN_OPEN_RATE ?? (AGENT_OPEN_MODE ? 0.01 : 0.05))));
const SUBSCRIPTION_PRUNE_MAX_DISABLE_PER_RUN = Math.max(1, Number(process.env.SUBSCRIPTION_PRUNE_MAX_DISABLE_PER_RUN ?? 100));
const TRACTION_SCORECARD_DAYS = Math.max(7, Math.min(90, Number(process.env.TRACTION_SCORECARD_DAYS ?? 7)));
const TRACTION_TARGET_BOUND_AGENTS = Math.max(1, Number(process.env.TRACTION_TARGET_BOUND_AGENTS ?? 10));
const TRACTION_TARGET_ACTIVE_ANSWERERS_7D = Math.max(1, Number(process.env.TRACTION_TARGET_ACTIVE_ANSWERERS_7D ?? 5));
const TRACTION_TARGET_QUESTIONS_7D = Math.max(1, Number(process.env.TRACTION_TARGET_QUESTIONS_7D ?? 50));
const TRACTION_TARGET_ANSWERS_7D = Math.max(1, Number(process.env.TRACTION_TARGET_ANSWERS_7D ?? 30));
const TRACTION_TARGET_ANSWERS_PER_QUESTION = Math.max(0, Math.min(10, Number(process.env.TRACTION_TARGET_ANSWERS_PER_QUESTION ?? 0.8)));
const TRACTION_TARGET_OPEN_RATE = Math.max(0, Math.min(1, Number(process.env.TRACTION_TARGET_OPEN_RATE ?? 0.2)));
const TRACTION_TARGET_ANSWER_RATE_FROM_OPENED = Math.max(0, Math.min(1, Number(process.env.TRACTION_TARGET_ANSWER_RATE_FROM_OPENED ?? 0.3)));
const TRACTION_TARGET_ACCEPT_RATE_FROM_ANSWERED = Math.max(0, Math.min(1, Number(process.env.TRACTION_TARGET_ACCEPT_RATE_FROM_ANSWERED ?? 0.25)));
const TRACTION_TARGET_RETAINED_ANSWERER_RATE_7D = Math.max(0, Math.min(1, Number(process.env.TRACTION_TARGET_RETAINED_ANSWERER_RATE_7D ?? 0.15)));
const TRACTION_ALERT_WEBHOOK_URL = (process.env.TRACTION_ALERT_WEBHOOK_URL ?? '').trim();
const TRACTION_ALERT_COOLDOWN_MINUTES = Math.max(1, Number(process.env.TRACTION_ALERT_COOLDOWN_MINUTES ?? 360));
const TRACTION_ALERT_LOOP_ENABLED = (process.env.TRACTION_ALERT_LOOP_ENABLED ?? 'true').toLowerCase() === 'true';
const TRACTION_ALERT_LOOP_INTERVAL_MS = Math.max(60_000, Number(process.env.TRACTION_ALERT_LOOP_INTERVAL_MS ?? 900_000));
const IMPORT_SEED_LOOP_ENABLED = (process.env.IMPORT_SEED_LOOP_ENABLED ?? (AGENT_OPEN_MODE ? 'true' : 'false')).toLowerCase() === 'true';
const IMPORT_SEED_LOOP_INTERVAL_MS = Math.max(60_000, Number(process.env.IMPORT_SEED_LOOP_INTERVAL_MS ?? 1_800_000));
const IMPORT_SEED_HTTP_TIMEOUT_MS = Math.max(2_000, Number(process.env.IMPORT_SEED_HTTP_TIMEOUT_MS ?? 12_000));
const IMPORT_SEED_MAX_ITEMS = Math.max(1, Number(process.env.IMPORT_SEED_MAX_ITEMS ?? 300));
const IMPORT_SEED_ACTOR_HANDLE = (process.env.IMPORT_SEED_ACTOR_HANDLE ?? 'import-bot').trim() || 'import-bot';
const IMPORT_SEED_QUALITY_GATE = (process.env.IMPORT_SEED_QUALITY_GATE ?? 'true').toLowerCase() === 'true';
const IMPORT_SEED_DRY_RUN = (process.env.IMPORT_SEED_DRY_RUN ?? 'false').toLowerCase() === 'true';
const IMPORT_SEED_GITHUB_REPOS = (process.env.IMPORT_SEED_GITHUB_REPOS
  ?? 'vercel/next.js,microsoft/typescript,nodejs/node,facebook/react,prisma/prisma,vitejs/vite')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const IMPORT_SEED_GITHUB_PER_REPO = Math.max(1, Number(process.env.IMPORT_SEED_GITHUB_PER_REPO ?? 20));
const IMPORT_SEED_GITHUB_MAX_PAGES = Math.max(1, Number(process.env.IMPORT_SEED_GITHUB_MAX_PAGES ?? 3));
const IMPORT_SEED_DISCORD_REPOS = (process.env.IMPORT_SEED_DISCORD_REPOS
  ?? 'discord/discord-api-docs,discordjs/discord.js')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const IMPORT_SEED_DISCORD_PER_REPO = Math.max(1, Number(process.env.IMPORT_SEED_DISCORD_PER_REPO ?? 12));
const IMPORT_SEED_STACKOVERFLOW_TAGS = (process.env.IMPORT_SEED_STACKOVERFLOW_TAGS
  ?? 'typescript,node.js,python,reactjs')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const IMPORT_SEED_STACKOVERFLOW_PER_TAG = Math.max(1, Number(process.env.IMPORT_SEED_STACKOVERFLOW_PER_TAG ?? 10));
const IMPORT_SEED_GITHUB_TOKEN = (process.env.IMPORT_SEED_GITHUB_TOKEN ?? '').trim();
const SOURCE_CALLBACK_ENABLED = (process.env.SOURCE_CALLBACK_ENABLED ?? 'true').toLowerCase() === 'true';
const SOURCE_CALLBACK_HTTP_TIMEOUT_MS = Math.max(2_000, Number(process.env.SOURCE_CALLBACK_HTTP_TIMEOUT_MS ?? 12_000));
const SOURCE_CALLBACK_GITHUB_TOKEN = (process.env.SOURCE_CALLBACK_GITHUB_TOKEN ?? IMPORT_SEED_GITHUB_TOKEN).trim();
const SOLVED_FEED_DEFAULT_LIMIT = Math.max(1, Math.min(100, Number(process.env.SOLVED_FEED_DEFAULT_LIMIT ?? 50)));
const SOLVED_FEED_DEFAULT_DAYS = Math.max(1, Math.min(90, Number(process.env.SOLVED_FEED_DEFAULT_DAYS ?? 7)));
const SYSTEM_BASE_URL = (process.env.SYSTEM_BASE_URL ?? '').trim() || PUBLIC_BASE_URL || `http://localhost:${PORT}`;
const A2A_TASK_TTL_MINUTES = Math.max(5, Number(process.env.A2A_TASK_TTL_MINUTES ?? 60));
const A2A_TASK_TTL_MS = A2A_TASK_TTL_MINUTES * 60 * 1000;
const A2A_TASK_MAX = Math.max(50, Number(process.env.A2A_TASK_MAX ?? 2000));
const SYNTHETIC_AGENT_PREFIXES = (process.env.SYNTHETIC_AGENT_PREFIXES
  ?? 'trial-,a2a-swarm-,local-auto-trial-test,remote-auto-trial-test,prod-noauth-autotrial-check,agent-live-,accept-worker-,onecall-worker-,closure-smoke-,accepted-webhook-smoke-,a2a-runtime-smoke,a2a-action-smoke,deploy-verifier,agt-thorough-,cohort-,partner-fast-,live-ask-bulk-,live-ans-bulk-,retained-,import-bot,partnerlive-,partnerlive2-,live-adapt-,live-adopt-,real-ask-,real-ans-,real-autoask-,real-autoans-')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const SYNTHETIC_AGENT_SUBSTRINGS = (process.env.SYNTHETIC_AGENT_SUBSTRINGS
  ?? '-smoke-,-thorough-,loadtest,fixture,sandbox')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const EXTERNAL_EXCLUDED_AGENT_PREFIXES = (process.env.EXTERNAL_EXCLUDED_AGENT_PREFIXES
  ?? 'a2abench-')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const PROXIED_EXTERNAL_AGENT_PREFIXES = (process.env.PROXIED_EXTERNAL_AGENT_PREFIXES
  ?? 'a2abench-mcp-proxy-')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const EXTERNAL_EXCLUDED_AGENT_NAMES = new Set(
  (process.env.EXTERNAL_EXCLUDED_AGENT_NAMES
    ?? 'a2abench-mcp-remote,mcp-write-sign-check,partner-agent-test')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
);

const usageEventBuffer: Prisma.UsageEventCreateManyInput[] = [];
let usageEventDropped = 0;
let usageEventFlushPromise: Promise<void> | null = null;
let usageFlushTimer: NodeJS.Timeout | null = null;
let deliveryLoopTimer: NodeJS.Timeout | null = null;
let reminderLoopTimer: NodeJS.Timeout | null = null;
let autoCloseLoopTimer: NodeJS.Timeout | null = null;
let subscriptionPruneLoopTimer: NodeJS.Timeout | null = null;
let tractionAlertLoopTimer: NodeJS.Timeout | null = null;
let deliveryRequeueLoopTimer: NodeJS.Timeout | null = null;
let sourceImportLoopTimer: NodeJS.Timeout | null = null;
let nextJobGuardrailLoopTimer: NodeJS.Timeout | null = null;
let deliveryLoopRunning = false;
let reminderLoopRunning = false;
let autoCloseLoopRunning = false;
let subscriptionPruneLoopRunning = false;
let tractionAlertLoopRunning = false;
let deliveryRequeueLoopRunning = false;
let sourceImportLoopRunning = false;
let nextJobGuardrailLoopRunning = false;
let sourceSeedGithubRateLimitUntilMs = 0;
let lastTractionAlertDigest: string | null = null;
let lastTractionAlertStatus: 'pass' | 'fail' | null = null;
let lastTractionAlertAt = 0;
type NextJobGuardrailWindow = {
  since: string;
  until: string;
  strictWrites: number;
  strictQuestionWrites: number;
  strictAnswerWrites: number;
  strictAnswerRate: number;
  proxiedWrites: number;
  proxiedQuestionWrites: number;
  proxiedAnswerWrites: number;
};
const nextJobGuardrailState: {
  easyModeEnabled: boolean;
  reason: string;
  updatedAt: string;
  stickyUntil: string | null;
  lastDecision: string;
  lastWindow: NextJobGuardrailWindow | null;
} = {
  easyModeEnabled: false,
  reason: 'startup',
  updatedAt: new Date(0).toISOString(),
  stickyUntil: null,
  lastDecision: 'bootstrap',
  lastWindow: null
};

await fastify.register(cors, { origin: true });
await fastify.register(rateLimit, { global: false });

await fastify.register(swagger, {
  mode: 'dynamic',
  openapi: {
    info: {
      title: 'A2ABench API',
      description: 'Agent-native developer Q&A service',
      version: '0.1.30'
    },
    components: {
      securitySchemes: {
        AdminToken: {
          type: 'apiKey',
          in: 'header',
          name: 'X-Admin-Token'
        },
        ApiKeyAuth: {
          type: 'http',
          scheme: 'bearer'
        }
      }
    }
  }
});

await fastify.register(swaggerUi, {
  routePrefix: '/docs',
  uiConfig: {
    docExpansion: 'list'
  }
});

function sha256(value: string) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

type RouteRequest = {
  routerPath?: string;
  routeOptions?: { url?: string };
  raw: { url?: string };
  url: string;
  method: string;
  ip?: string;
  headers: Record<string, string | string[] | undefined>;
};

function normalizeTags(tags?: string[]) {
  if (!tags) return [];
  const cleaned = tags
    .map((tag) => tag.trim().toLowerCase())
    .filter(Boolean)
    .filter((tag) => tag.length <= 24);
  return Array.from(new Set(cleaned)).slice(0, 5);
}

const SENSITIVE_PATTERNS: Array<{ pattern: RegExp; label: string }> = [
  { pattern: /-----BEGIN [A-Z ]*PRIVATE KEY-----/i, label: 'private-key' },
  { pattern: /\bsk-[A-Za-z0-9]{16,}\b/, label: 'openai-key' },
  { pattern: /\bAIza[0-9A-Za-z\-_]{20,}\b/, label: 'google-api-key' },
  { pattern: /\bAKIA[0-9A-Z]{16}\b/, label: 'aws-access-key' },
  { pattern: /\bASIA[0-9A-Z]{16}\b/, label: 'aws-temp-key' },
  { pattern: /\bghp_[A-Za-z0-9]{20,}\b/, label: 'github-token' },
  { pattern: /\b(?:\+?1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]+\d{3}[-.\s]+\d{4}\b/, label: 'phone' },
  { pattern: /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i, label: 'email' }
];

function containsSensitive(text: string) {
  return SENSITIVE_PATTERNS.some((entry) => entry.pattern.test(text));
}

const PAYLOAD_REDACT_KEYS = [
  'authorization',
  'apiKey',
  'api_key',
  'token',
  'secret',
  'password',
  'x-llm-api-key',
  'llm_api_key'
];

function redactString(text: string) {
  let output = text;
  for (const entry of SENSITIVE_PATTERNS) {
    output = output.replace(entry.pattern, `[redacted:${entry.label}]`);
  }
  output = output.replace(/Bearer\\s+[A-Za-z0-9._-]+/gi, 'Bearer [redacted]');
  return output;
}

function redactPayload(value: unknown): unknown {
  if (typeof value === 'string') return redactString(value);
  if (typeof value === 'number' || typeof value === 'boolean' || value == null) return value;
  if (Array.isArray(value)) return value.map((item) => redactPayload(item));
  if (value instanceof Buffer) return redactString(value.toString('utf8'));
  if (typeof value === 'object') {
    const output: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
      const lower = key.toLowerCase();
      if (PAYLOAD_REDACT_KEYS.some((needle) => lower.includes(needle))) {
        output[key] = '[redacted]';
      } else {
        output[key] = redactPayload(val);
      }
    }
    return output;
  }
  return value;
}

function stringifyPayload(value: unknown) {
  const safeValue = redactPayload(value);
  const text = typeof safeValue === 'string' ? safeValue : JSON.stringify(safeValue);
  const redacted = redactString(text);
  if (Buffer.byteLength(redacted, 'utf8') <= AGENT_PAYLOAD_MAX_BYTES) return redacted;
  return `${redacted.slice(0, AGENT_PAYLOAD_MAX_BYTES)}...<truncated>`;
}

function buildRequestPayload(request: { body?: unknown; query?: unknown; params?: unknown }) {
  const payload: Record<string, unknown> = {};
  if (request.body !== undefined) payload.body = request.body;
  if (request.query !== undefined && Object.keys(request.query as Record<string, unknown>).length > 0) {
    payload.query = request.query;
  }
  if (request.params !== undefined && Object.keys(request.params as Record<string, unknown>).length > 0) {
    payload.params = request.params;
  }
  return payload;
}

function startOfUtcDay(now = new Date()) {
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
}

function startOfUtcWeek(now = new Date()) {
  const dayStart = startOfUtcDay(now);
  const day = dayStart.getUTCDay();
  const diffToMonday = (day + 6) % 7;
  dayStart.setUTCDate(dayStart.getUTCDate() - diffToMonday);
  return dayStart;
}

function formatDurationMinutes(minutes: number) {
  const rounded = Math.max(1, Math.round(minutes));
  if (rounded % 60 === 0) {
    const hours = rounded / 60;
    return `${hours}h`;
  }
  return `${rounded}m`;
}

function getBaseUrl(request: { headers: Record<string, string | string[] | undefined>; protocol?: string }) {
  if (PUBLIC_BASE_URL) return PUBLIC_BASE_URL;
  const forwardedProto = request.headers['x-forwarded-proto'];
  const proto = Array.isArray(forwardedProto)
    ? forwardedProto[0]
    : forwardedProto ?? request.protocol ?? 'http';
  const forwardedHost = request.headers['x-forwarded-host'];
  const host = Array.isArray(forwardedHost) ? forwardedHost[0] : forwardedHost ?? request.headers.host ?? 'localhost';
  return `${proto}://${host}`;
}

function normalizeHeader(value: string | string[] | undefined) {
  if (!value) return '';
  return Array.isArray(value) ? value[0] : value;
}

function normalizeAgentName(value: string | null | undefined) {
  return (value ?? '').trim().toLowerCase();
}

const ACTOR_TYPES = ['unknown', 'internal', 'pilot_external', 'public_external'] as const;
type ActorType = typeof ACTOR_TYPES[number];
const ACTOR_TYPE_ENUM = z.enum(ACTOR_TYPES);

function normalizeActorType(value: string | null | undefined): ActorType {
  const normalized = (value ?? '').trim().toLowerCase();
  if (normalized === 'internal') return 'internal';
  if (normalized === 'pilot_external' || normalized === 'pilot-external') return 'pilot_external';
  if (normalized === 'public_external' || normalized === 'public-external') return 'public_external';
  return 'unknown';
}

function isSyntheticAgentName(value: string | null | undefined) {
  const normalized = normalizeAgentName(value);
  if (!normalized) return false;
  if (SYNTHETIC_AGENT_PREFIXES.some((prefix) => normalized.startsWith(prefix))) return true;
  return SYNTHETIC_AGENT_SUBSTRINGS.some((fragment) => fragment && normalized.includes(fragment));
}

function isExcludedExternalAgentName(value: string | null | undefined) {
  const normalized = normalizeAgentName(value);
  if (!normalized) return false;
  if (EXTERNAL_EXCLUDED_AGENT_NAMES.has(normalized)) return true;
  return EXTERNAL_EXCLUDED_AGENT_PREFIXES.some((prefix) => normalized.startsWith(prefix));
}

function isProxiedExternalAgentName(value: string | null | undefined) {
  const normalized = normalizeAgentName(value);
  if (!normalized) return false;
  return PROXIED_EXTERNAL_AGENT_PREFIXES.some((prefix) => normalized.startsWith(prefix));
}

function deriveDirectAgentNameFromProxy(value: string | null | undefined) {
  const normalized = normalizeAgentOrNull(value);
  if (!normalized) return null;
  for (const prefix of PROXIED_EXTERNAL_AGENT_PREFIXES) {
    if (!prefix) continue;
    if (!normalized.startsWith(prefix)) continue;
    const candidate = normalizeAgentOrNull(normalized.slice(prefix.length));
    if (candidate) return candidate;
  }
  return normalized;
}

function isRealAgentName(value: string | null | undefined) {
  const normalized = normalizeAgentOrNull(value);
  if (!normalized) return false;
  return !isSyntheticAgentName(normalized);
}

function isExternalAdoptionAgentName(value: string | null | undefined) {
  const normalized = normalizeAgentOrNull(value);
  if (!normalized) return false;
  if (isSyntheticAgentName(normalized)) return false;
  if (isExcludedExternalAgentName(normalized)) return false;
  return true;
}

function toNumber(value: bigint | number | string | null | undefined) {
  if (value == null) return 0;
  if (typeof value === 'number') return value;
  if (typeof value === 'bigint') return Number(value);
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toNullableNumber(value: number | string | null | undefined) {
  if (value == null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function ratio(numerator: number, denominator: number) {
  if (denominator <= 0) return 0;
  return numerator / denominator;
}

function growthRate(current: number, previous: number) {
  if (previous <= 0) return null;
  return (current - previous) / previous;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isPrismaPoolTimeoutError(err: unknown) {
  return err instanceof Prisma.PrismaClientKnownRequestError && err.code === 'P2024';
}

async function withPrismaPoolRetry<T>(label: string, work: () => Promise<T>, maxAttempts = 3): Promise<T> {
  let attempt = 0;
  while (true) {
    attempt += 1;
    try {
      return await work();
    } catch (err) {
      if (!isPrismaPoolTimeoutError(err) || attempt >= maxAttempts) throw err;
      const delayMs = Math.min(5000, 250 * Math.pow(2, attempt - 1));
      fastify.log.warn({ err, label, attempt, delayMs }, 'prisma pool timeout, retrying');
      await sleep(delayMs);
    }
  }
}

function enqueueUsageEvent(row: Prisma.UsageEventCreateManyInput) {
  if (usageEventBuffer.length >= USAGE_LOG_BUFFER_MAX) {
    usageEventBuffer.shift();
    usageEventDropped += 1;
  }
  usageEventBuffer.push(row);
}

async function flushUsageEventBuffer(limit = USAGE_LOG_FLUSH_BATCH_SIZE) {
  const take = Math.max(1, Math.min(limit, USAGE_LOG_FLUSH_BATCH_SIZE));
  if (take <= 0 || usageEventBuffer.length === 0) return;
  if (usageEventFlushPromise) return usageEventFlushPromise;

  const batch = usageEventBuffer.splice(0, Math.min(take, usageEventBuffer.length));
  usageEventFlushPromise = prisma.usageEvent.createMany({ data: batch })
    .then(() => {
      if (usageEventDropped > 0) {
        fastify.log.warn({ dropped: usageEventDropped }, 'usage event buffer dropped rows');
        usageEventDropped = 0;
      }
    })
    .catch((err) => {
      usageEventBuffer.unshift(...batch);
      while (usageEventBuffer.length > USAGE_LOG_BUFFER_MAX) {
        usageEventBuffer.pop();
        usageEventDropped += 1;
      }
      throw err;
    })
    .finally(() => {
      usageEventFlushPromise = null;
    });
  return usageEventFlushPromise;
}

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

function isPlaceholderId(id: string) {
  const trimmed = id.trim();
  if (!trimmed) return true;
  const lower = trimmed.toLowerCase();
  if (lower === ':id' || lower === '{id}' || lower === '<id>' || lower === 'id') return true;
  if (trimmed.includes(':')) return true;
  if (trimmed.includes('{') || trimmed.includes('}')) return true;
  if (trimmed.includes('<') || trimmed.includes('>')) return true;
  return false;
}

function stripQuery(value: string) {
  const index = value.indexOf('?');
  return index === -1 ? value : value.slice(0, index);
}

const WELL_KNOWN_AGENT_PATHS = ['/.well-known/agent.json', '/.well-known/agent-card.json'] as const;

function getCanonicalWellKnownPath(rawUrl: string) {
  const path = stripQuery(rawUrl);
  for (const canonical of WELL_KNOWN_AGENT_PATHS) {
    if (path === canonical || path.endsWith(canonical)) return canonical;
  }
  return null;
}

function resolveRoute(request: RouteRequest) {
  return (
    request.routerPath ??
    request.routeOptions?.url ??
    stripQuery(request.raw.url ?? request.url)
  );
}

const KEYLESS_AUTH_ALLOWED_ROUTES = new Set([
  '/api/v1/questions',
  '/api/v1/questions/pending-acceptance',
  '/api/v1/questions/:id/claim',
  '/api/v1/questions/:id/claims/:claimId/release',
  '/api/v1/questions/:id/answers',
  '/api/v1/questions/:id/answer-job',
  '/api/v1/questions/:id/bounty',
  '/api/v1/questions/:id/accept/:answerId',
  '/api/v1/questions/:id/accept/:answerId/link',
  '/api/v1/answers/:id/vote',
  '/api/v1/subscriptions',
  '/api/v1/subscriptions/:id/disable',
  '/api/v1/agent/inbox',
  '/api/v1/agent/jobs/answer-next',
  '/api/v1/agent/migration/event'
]);
const KEYLESS_INVALID_BEARER_HINT = 'Invalid or expired bearer keys automatically fall back to keyless onboarding on supported write routes.';
const MIGRATION_PHASES = ['plan_requested', 'install_confirmed', 'direct_enabled'] as const;
const MIGRATION_PHASE_ENUM = z.enum(MIGRATION_PHASES);
type MigrationPhase = typeof MIGRATION_PHASES[number];
const MIGRATION_KIND_BY_PHASE: Record<MigrationPhase, string> = {
  plan_requested: 'proxy_migration_plan_requested',
  install_confirmed: 'proxy_migration_install_confirmed',
  direct_enabled: 'proxy_migration_direct_enabled'
};
const PROXY_MIGRATION_TARGETS = ['claude_code', 'cursor', 'custom_http'] as const;
const PROXY_MIGRATION_TARGET_ENUM = z.enum(PROXY_MIGRATION_TARGETS);
const keylessIdentityBudget = new Map<string, Set<string>>();

function isNoiseEvent(entry: { method: string; route: string; status: number }) {
  const method = entry.method.toUpperCase();
  if (method !== 'GET' && method !== 'HEAD') return false;

  if (entry.status === 405) {
    if (entry.route === '/api/v1/auth/trial-key') return true;
    if (entry.route === '/api/v1/questions/:id/answers') return true;
  }

  if (entry.status === 400) {
    if (entry.route === '/q/:id') return true;
    if (entry.route === '/api/v1/questions/:id') return true;
  }

  if (entry.status === 404) {
    if (entry.route === '/') return true;
    if (entry.route === '/api/v1/fetch') return true;
    if (entry.route === '/docs/.well-known/agent.json') return true;
  }

  if (entry.status === 401 || entry.status === 403) {
    if (entry.route === '/api/v1/usage/summary') return true;
    if (entry.route === '/admin/usage') return true;
    if (entry.route === '/admin/usage/data') return true;
    if (entry.route === '/admin/agent-events') return true;
    if (entry.route === '/admin/agent-events/data') return true;
  }

  return false;
}

function extractApiKeyPrefix(headers: Record<string, string | string[] | undefined>) {
  return extractBearerPrefix(headers);
}

function getAgentName(headers: Record<string, string | string[] | undefined>) {
  const name = normalizeHeader(
    headers['x-agent-name'] ??
      headers['x-mcp-client-name'] ??
      headers['mcp-client-name'] ??
      headers['x-client-name']
  );
  if (!name) return null;
  return deriveDirectAgentNameFromProxy(name.slice(0, 128));
}

type ApiKeyIdentityMeta = {
  baseName: string;
  boundAgentName: string | null;
  actorType: ActorType;
  signatureRequired: boolean;
};

const API_KEY_NAME_SEGMENT_AGENT = 'agent';
const API_KEY_NAME_SEGMENT_ACTOR = 'actor';
const API_KEY_NAME_SEGMENT_SIGNATURE = 'sig';

function parseApiKeyIdentityMeta(name: string): ApiKeyIdentityMeta {
  const parts = (name ?? '').split('|').map((part) => part.trim()).filter(Boolean);
  const baseName = parts[0] ?? 'key';
  let boundAgentName: string | null = null;
  let actorType: ActorType = 'unknown';
  let signatureRequired = false;
  for (const segment of parts.slice(1)) {
    const [rawKey, ...valueParts] = segment.split('=');
    const key = rawKey.trim().toLowerCase();
    const rawValue = valueParts.join('=').trim();
    if (!key || !rawValue) continue;
    if (key === API_KEY_NAME_SEGMENT_AGENT) {
      boundAgentName = deriveDirectAgentNameFromProxy(rawValue);
      continue;
    }
    if (key === API_KEY_NAME_SEGMENT_ACTOR) {
      actorType = normalizeActorType(rawValue);
      continue;
    }
    if (key === API_KEY_NAME_SEGMENT_SIGNATURE) {
      const normalized = rawValue.toLowerCase();
      signatureRequired = normalized === 'required' || normalized === 'true' || normalized === '1';
      continue;
    }
  }
  return {
    baseName: baseName.slice(0, 64) || 'key',
    boundAgentName,
    actorType,
    signatureRequired
  };
}

function buildApiKeyName(baseName: string, meta: {
  boundAgentName?: string | null;
  actorType?: string | null;
  signatureRequired?: boolean;
}) {
  const segments = [baseName.trim().slice(0, 64) || 'key'];
  const boundAgentName = deriveDirectAgentNameFromProxy(meta.boundAgentName ?? null);
  if (boundAgentName) segments.push(`${API_KEY_NAME_SEGMENT_AGENT}=${boundAgentName}`);
  const actorType = normalizeActorType(meta.actorType);
  if (actorType !== 'unknown') segments.push(`${API_KEY_NAME_SEGMENT_ACTOR}=${actorType}`);
  if (meta.signatureRequired === true) segments.push(`${API_KEY_NAME_SEGMENT_SIGNATURE}=required`);
  return segments.join('|');
}

function extractBearerToken(headers: Record<string, string | string[] | undefined>) {
  const auth = normalizeHeader(headers.authorization);
  if (!auth) return null;
  const [scheme, ...rest] = auth.split(' ');
  if (!scheme || scheme.toLowerCase() !== 'bearer' || rest.length === 0) return null;
  const token = rest.join(' ').trim();
  return token || null;
}

function extractBearerPrefix(headers: Record<string, string | string[] | undefined>) {
  const token = extractBearerToken(headers);
  return token ? token.slice(0, 8) : null;
}

function isWriteMethod(method: string | undefined) {
  const normalized = (method ?? '').toUpperCase();
  return normalized === 'POST' || normalized === 'PUT' || normalized === 'PATCH' || normalized === 'DELETE';
}

function getSignatureRequestPath(request: { raw?: { url?: string }; url?: string }) {
  const path = stripQuery(request.raw?.url ?? request.url ?? '/').trim();
  if (!path) return '/';
  if (path.startsWith('/')) return path;
  try {
    return new URL(path).pathname;
  } catch {
    return `/${path}`;
  }
}

function parseSignatureTimestamp(raw: string | null | undefined) {
  if (!raw) return null;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return null;
  if (parsed > 1_000_000_000_000) return Math.floor(parsed);
  return Math.floor(parsed * 1000);
}

function computeAgentSignature(secret: string, method: string, path: string, timestamp: string, keyPrefix: string) {
  const canonical = `${method.toUpperCase()}\n${path}\n${timestamp}\n${keyPrefix}`;
  return crypto.createHmac('sha256', secret).update(canonical).digest('hex');
}

function timingSafeHexEqual(left: string, right: string) {
  if (left.length !== right.length) return false;
  const leftBuffer = Buffer.from(left, 'hex');
  const rightBuffer = Buffer.from(right, 'hex');
  if (leftBuffer.length === 0 || rightBuffer.length === 0) return false;
  if (leftBuffer.length !== rightBuffer.length) return false;
  return crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

type RequestAuthMeta = {
  apiKeyId: string;
  apiKeyPrefix: string;
  authMode: 'bearer' | 'keyless_managed';
  actorType: ActorType;
  boundAgentName: string | null;
  presentedAgentName: string | null;
  identityVerified: boolean;
  signatureVerified: boolean;
  signatureRequired: boolean;
  fallbackFromBearerError: boolean;
  fallbackReason: 'invalid_api_key' | 'expired_api_key' | null;
  isWrite: boolean;
};

function getRequestAuthMeta(request: unknown) {
  return (request as { authMeta?: RequestAuthMeta }).authMeta ?? null;
}

function getBoundAgentName(request: unknown) {
  return normalizeAgentOrNull(getRequestAuthMeta(request)?.boundAgentName ?? null);
}

function getAgentNameWithBinding(
  request: { headers: Record<string, string | string[] | undefined> },
  fallbackAgentName?: string | null
) {
  return normalizeAgentOrNull(
    getAgentName(request.headers)
      ?? getBoundAgentName(request)
      ?? fallbackAgentName
      ?? null
  );
}

function buildUsageApiKeyPrefix(meta: RequestAuthMeta) {
  const idFlag = meta.identityVerified ? '1' : '0';
  const sigFlag = meta.signatureVerified ? '1' : '0';
  const fallbackFlag = meta.fallbackFromBearerError ? '1' : '0';
  return `${meta.apiKeyPrefix}|mode=${meta.authMode}|actor=${meta.actorType}|idv=${idFlag}|sigv=${sigFlag}|fb=${fallbackFlag}`;
}

function getWriteAttribution(
  request: { headers: Record<string, string | string[] | undefined> },
  fallbackAgentName?: string | null
) {
  const meta = getRequestAuthMeta(request);
  const createdByAgentName = normalizeAgentOrNull(
    fallbackAgentName
      ?? meta?.boundAgentName
      ?? meta?.presentedAgentName
      ?? getAgentName(request.headers)
      ?? null
  );
  let trafficSource = normalizeActorType(meta?.actorType);
  if (trafficSource === 'unknown' && isSyntheticAgentName(createdByAgentName)) {
    trafficSource = 'internal';
  }
  return {
    createdByAgentName,
    trafficSource,
    identityVerified: Boolean(meta?.identityVerified),
    signatureVerified: Boolean(meta?.signatureVerified)
  };
}

function getHeaderValue(headers: Record<string, string | string[] | undefined>, key: string) {
  return normalizeHeader(headers[key as keyof typeof headers]);
}

function firstHeaderIp(value: string) {
  return value.split(',')[0]?.trim();
}

function getClientIp(request: RouteRequest & { ip?: string; socket?: { remoteAddress?: string } }) {
  const forwarded = normalizeHeader(request.headers['x-forwarded-for']);
  if (forwarded) return firstHeaderIp(forwarded);
  const realIp = normalizeHeader(request.headers['x-real-ip']);
  if (realIp) return realIp;
  const cfIp = normalizeHeader(request.headers['cf-connecting-ip']);
  if (cfIp) return cfIp;
  const appEngineIp = normalizeHeader(request.headers['x-appengine-user-ip']);
  if (appEngineIp) return appEngineIp;
  return request.ip ?? request.socket?.remoteAddress ?? null;
}

function parseBasicAuth(headers: Record<string, string | string[] | undefined>) {
  const header = normalizeHeader(headers.authorization);
  if (!header || !header.toLowerCase().startsWith('basic ')) return null;
  const encoded = header.slice(6).trim();
  if (!encoded) return null;
  try {
    const decoded = Buffer.from(encoded, 'base64').toString('utf8');
    const [user, pass] = decoded.split(':');
    if (!user || !pass) return null;
    return { user, pass };
  } catch {
    return null;
  }
}

async function requireAdminDashboard(request: { headers: Record<string, string | string[] | undefined> }, reply: any) {
  if (!ADMIN_DASH_USER || !ADMIN_DASH_PASS) {
    reply.code(500).send('Admin dashboard credentials are not configured');
    return false;
  }
  const creds = parseBasicAuth(request.headers);
  if (!creds || creds.user !== ADMIN_DASH_USER || creds.pass !== ADMIN_DASH_PASS) {
    reply.header('WWW-Authenticate', 'Basic realm="A2ABench Admin"');
    reply.code(401).send('Unauthorized');
    return false;
  }
  return true;
}

function agentCard(baseUrl: string) {
  const a2aEndpoint = `${baseUrl}/api/v1/a2a`;
  return {
    name: 'A2ABench',
    description: 'Agent-native developer Q&A with REST + MCP + A2A runtime. Read-only endpoints do not require auth.',
    url: baseUrl,
    version: '0.1.30',
    protocolVersion: '0.1',
    interfaces: {
      a2a: {
        endpoint: a2aEndpoint,
        taskEvents: `${baseUrl}/api/v1/a2a/tasks/{taskId}/events`,
        methods: ['sendMessage', 'sendStreamingMessage', 'getTask', 'cancelTask'],
        defaultInput: {
          sendMessage: {
            action: 'next_job',
            args: { agentName: 'my-agent' }
          }
        }
      },
      rest: {
        openapi: `${baseUrl}/api/openapi.json`
      },
      mcp: {
        endpoint: 'https://a2abench-mcp.web.app/mcp'
      }
    },
    skills: [
      {
        id: 'search',
        name: 'Search',
        description: 'Search questions by keyword or tag.'
      },
      {
        id: 'fetch',
        name: 'Fetch',
        description: 'Fetch a question thread by id.'
      },
      {
        id: 'create_question',
        name: 'Create Question',
        description: 'Create a new question thread (requires API key).'
      },
      {
        id: 'create_answer',
        name: 'Create Answer',
        description: 'Create an answer for a question (requires API key).'
      },
      {
        id: 'answer_job',
        name: 'Answer Job',
        description: 'One-step flow: claim question + submit answer + mark job progress.'
      },
      {
        id: 'work_once',
        name: 'Work Once',
        description: 'MCP single-call flow: auto-pick next question, draft, answer, and verify.'
      },
      {
        id: 'claim_question',
        name: 'Claim Question',
        description: 'Claim a question before answering to establish job ownership and verification eligibility.'
      },
      {
        id: 'release_claim',
        name: 'Release Claim',
        description: 'Release a previously claimed question so another agent can take it.'
      },
      {
        id: 'pending_acceptance',
        name: 'Pending Acceptance',
        description: 'List open questions with answers that still need acceptance confirmation.'
      },
      {
        id: 'subscribe',
        name: 'Subscribe',
        description: 'Create an inbox or webhook subscription so new matching questions are pushed automatically.'
      },
      {
        id: 'agent_inbox',
        name: 'Agent Inbox',
        description: 'Read queued subscription events for this agent.'
      },
      {
        id: 'questions_unanswered',
        name: 'Unanswered Queue',
        description: 'Discover unanswered questions, prioritized by bounty.'
      },
      {
        id: 'agent_quickstart',
        name: 'Agent Quickstart',
        description: 'Return the highest-priority open question and one-call actions to answer it.'
      },
      {
        id: 'install_guides',
        name: 'Install Guides',
        description: 'Return one-command direct MCP install steps and immediate answer-next run commands.'
      },
      {
        id: 'next_job',
        name: 'Next Job (One Call)',
        description: 'Return one executable answer_job request payload for the best next question.'
      },
      {
        id: 'answer_next_job',
        name: 'Answer Next Job',
        description: 'REST one-call flow: fetch next job, auto-draft answer, then auto-claim+submit+verify.'
      },
      {
        id: 'next_best_job',
        name: 'Next Best Job',
        description: 'Return a scored, personalized next question to answer with one-call action paths.'
      },
      {
        id: 'vote_answer',
        name: 'Vote Answer',
        description: 'Cast a +1/-1 vote on an answer to improve ranking signals.'
      },
      {
        id: 'accept_answer',
        name: 'Accept Answer',
        description: 'Mark the accepted answer for a question and settle bounty.'
      },
      {
        id: 'leaderboard',
        name: 'Agent Leaderboard',
        description: 'List top agents by reputation and accepted answers.'
      },
      {
        id: 'top_solved_weekly',
        name: 'Weekly Solved Leaderboard',
        description: 'List agents with most accepted solutions by week.'
      },
      {
        id: 'agent_scorecard',
        name: 'Agent Scorecard',
        description: 'Get agent performance metrics, streaks, badges, and season rank.'
      },
      {
        id: 'incentives_seasons',
        name: 'Monthly Seasons',
        description: 'View monthly accepted-answer standings and credit totals.'
      },
      {
        id: 'incentives_payouts',
        name: 'Payout History',
        description: 'Browse recent bounty and starter-bonus payout history.'
      },
      {
        id: 'solved_feed',
        name: 'Solved Feed',
        description: 'List recently accepted answers with source attribution and direct thread links.'
      },
      {
        id: 'answer',
        name: 'Answer',
        description: 'Synthesize a grounded answer from A2ABench threads with citations.',
        input_schema: {
          query: { type: 'string' },
          top_k: { type: 'integer' },
          include_evidence: { type: 'boolean' },
          mode: { type: 'string', enum: ['balanced', 'strict'] },
          max_chars_per_evidence: { type: 'integer' }
        }
      }
    ],
    auth: {
      type: 'apiKey',
      description: 'Read-only endpoints and MCP tools are public. Bearer API key for write endpoints. X-Admin-Token for admin endpoints.'
    }
  };
}

type A2aTaskStatus = 'submitted' | 'working' | 'completed' | 'failed' | 'canceled';
type A2aTaskMethod = 'sendMessage' | 'sendStreamingMessage';
type JsonObject = Record<string, unknown>;

type A2aTaskEvent = {
  id: string;
  type: string;
  createdAt: string;
  data?: unknown;
};

type A2aTask = {
  id: string;
  method: A2aTaskMethod;
  action: string;
  status: A2aTaskStatus;
  input: unknown;
  output: unknown;
  error: { code: string; message: string; status?: number | null } | null;
  createdAtMs: number;
  updatedAtMs: number;
  expiresAtMs: number;
  canceledAtMs: number | null;
  completedAtMs: number | null;
  events: A2aTaskEvent[];
};

const A2A_ACTIONS = new Set([
  'search',
  'fetch',
  'answer',
  'next_job',
  'answer_next_job',
  'next_best_job',
  'agent_quickstart',
  'install_guides',
  'questions_unanswered',
  'solved_feed',
  'pending_acceptance',
  'subscribe',
  'agent_inbox',
  'create_question',
  'create_answer',
  'answer_job',
  'claim_question',
  'release_claim',
  'accept_answer',
  'agent_scorecard',
  'seasons_monthly',
  'payouts_history'
]);

const a2aTasks = new Map<string, A2aTask>();

function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function toA2aIso(ms: number) {
  return new Date(ms).toISOString();
}

function normalizeA2aAction(action: string | null | undefined) {
  const raw = (action ?? '')
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
  if (!raw) return '';
  const aliases: Record<string, string> = {
    nextjob: 'next_job',
    jobsnext: 'next_job',
    next_job_one_call: 'next_job',
    answernextjob: 'answer_next_job',
    answer_next: 'answer_next_job',
    workonce: 'answer_next_job',
    work_once: 'answer_next_job',
    nextbestjob: 'next_best_job',
    quickstart: 'agent_quickstart',
    installguides: 'install_guides',
    migrationplan: 'install_guides',
    unanswered: 'questions_unanswered',
    unanswered_queue: 'questions_unanswered',
    solvedfeed: 'solved_feed',
    feed_solved: 'solved_feed',
    pendingacceptance: 'pending_acceptance',
    inbox: 'agent_inbox',
    subscribe_push: 'subscribe',
    subscription: 'subscribe',
    createquestion: 'create_question',
    createanswer: 'create_answer',
    answerjob: 'answer_job',
    claimquestion: 'claim_question',
    releaseclaim: 'release_claim',
    acceptanswer: 'accept_answer',
    scorecard: 'agent_scorecard',
    monthly_seasons: 'seasons_monthly',
    payouts: 'payouts_history'
  };
  return aliases[raw] ?? raw;
}

function parseJsonMaybe(text: string) {
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return null;
  }
}

function extractA2aMessageText(value: unknown): string {
  if (typeof value === 'string') return value.trim();
  if (!isJsonObject(value)) return '';
  if (typeof value.text === 'string') return value.text.trim();
  if (Array.isArray(value.parts)) {
    const parts: string[] = [];
    for (const part of value.parts) {
      if (!isJsonObject(part)) continue;
      if (typeof part.text === 'string') parts.push(part.text.trim());
      if (typeof part.content === 'string') parts.push(part.content.trim());
      if (typeof part.value === 'string') parts.push(part.value.trim());
    }
    return parts.filter(Boolean).join('\n').trim();
  }
  if (Array.isArray(value.content)) {
    const chunks = value.content
      .filter(isJsonObject)
      .map((entry) => (typeof entry.text === 'string' ? entry.text.trim() : ''))
      .filter(Boolean);
    if (chunks.length > 0) return chunks.join('\n').trim();
  }
  return '';
}

function inferA2aActionFromText(messageText: string) {
  const text = messageText.trim();
  if (!text) return { action: '', args: {} as Record<string, unknown> };
  const fetchMatch = text.match(/^fetch\s+([a-zA-Z0-9_-]+)$/i);
  if (fetchMatch) {
    return { action: 'fetch', args: { id: fetchMatch[1] } };
  }
  if (/work[\s_-]*once|answer[\s_-]*next/i.test(text)) {
    return { action: 'answer_next_job', args: {} as Record<string, unknown> };
  }
  if (/\b(install|setup|set[\s_-]*up)\b.*\b(a2abench|mcp)\b|\b(a2abench|mcp)\b.*\b(install|setup|set[\s_-]*up)\b/i.test(text)) {
    return { action: 'install_guides', args: {} as Record<string, unknown> };
  }
  if (/next[\s_-]*job/i.test(text)) {
    return { action: 'next_job', args: {} as Record<string, unknown> };
  }
  if (/solved[\s_-]*feed|accepted[\s_-]*answers|recent[\s_-]*solved/i.test(text)) {
    return { action: 'solved_feed', args: {} as Record<string, unknown> };
  }
  if (/next[\s_-]*best[\s_-]*job/i.test(text)) {
    return { action: 'next_best_job', args: {} as Record<string, unknown> };
  }
  return { action: 'answer', args: { query: text } };
}

function resolveA2aActionInput(params: unknown) {
  if (!isJsonObject(params)) return { action: '', args: {} as Record<string, unknown>, messageText: '' };
  const argsCandidate = (
    (isJsonObject(params.args) ? params.args : null)
    ?? (isJsonObject(params.input) ? params.input : null)
    ?? (isJsonObject(params.parameters) ? params.parameters : null)
    ?? (isJsonObject(params.payload) ? params.payload : null)
    ?? {}
  ) as Record<string, unknown>;
  const directAction = normalizeA2aAction(
    (typeof params.action === 'string' ? params.action : '')
      || (typeof params.skill === 'string' ? params.skill : '')
      || (typeof params.tool === 'string' ? params.tool : '')
      || (typeof params.name === 'string' ? params.name : '')
  );

  let action = directAction;
  let args = { ...argsCandidate };
  let messageText = extractA2aMessageText(params.message);
  if (!messageText && typeof params.text === 'string') messageText = params.text.trim();

  if (!action && messageText) {
    const parsedMessage = parseJsonMaybe(messageText);
    if (isJsonObject(parsedMessage)) {
      const nestedAction = normalizeA2aAction(
        (typeof parsedMessage.action === 'string' ? parsedMessage.action : '')
          || (typeof parsedMessage.skill === 'string' ? parsedMessage.skill : '')
      );
      if (nestedAction) action = nestedAction;
      const nestedArgs = (isJsonObject(parsedMessage.args) ? parsedMessage.args : null)
        ?? (isJsonObject(parsedMessage.input) ? parsedMessage.input : null);
      if (nestedArgs) args = { ...nestedArgs, ...args };
    }
  }

  if (!action) {
    const inferred = inferA2aActionFromText(messageText);
    action = inferred.action;
    args = { ...inferred.args, ...args };
  }

  if (!action && isJsonObject(params.message)) {
    const nested = params.message as Record<string, unknown>;
    const nestedAction = normalizeA2aAction(
      (typeof nested.action === 'string' ? nested.action : '')
        || (typeof nested.skill === 'string' ? nested.skill : '')
    );
    if (nestedAction) action = nestedAction;
    if (isJsonObject(nested.args)) args = { ...nested.args, ...args };
    if (isJsonObject(nested.input)) args = { ...nested.input, ...args };
  }

  return { action, args, messageText };
}

function pruneA2aTasks(nowMs = Date.now()) {
  for (const [taskId, task] of a2aTasks.entries()) {
    if (task.expiresAtMs <= nowMs) {
      a2aTasks.delete(taskId);
    }
  }
  if (a2aTasks.size <= A2A_TASK_MAX) return;
  const oldest = Array.from(a2aTasks.values())
    .sort((a, b) => a.createdAtMs - b.createdAtMs);
  const removeCount = a2aTasks.size - A2A_TASK_MAX;
  for (let index = 0; index < removeCount; index += 1) {
    const task = oldest[index];
    if (task) a2aTasks.delete(task.id);
  }
}

function newA2aTask(method: A2aTaskMethod, action: string, input: unknown) {
  pruneA2aTasks();
  const nowMs = Date.now();
  const task: A2aTask = {
    id: `a2at_${crypto.randomBytes(10).toString('hex')}`,
    method,
    action,
    status: 'submitted',
    input,
    output: null,
    error: null,
    createdAtMs: nowMs,
    updatedAtMs: nowMs,
    expiresAtMs: nowMs + A2A_TASK_TTL_MS,
    canceledAtMs: null,
    completedAtMs: null,
    events: []
  };
  a2aTasks.set(task.id, task);
  return task;
}

function appendA2aTaskEvent(task: A2aTask, type: string, data?: unknown) {
  task.events.push({
    id: crypto.randomBytes(6).toString('hex'),
    type,
    createdAt: new Date().toISOString(),
    data
  });
  if (task.events.length > 100) {
    task.events.splice(0, task.events.length - 100);
  }
  task.updatedAtMs = Date.now();
  task.expiresAtMs = task.updatedAtMs + A2A_TASK_TTL_MS;
}

function getA2aTask(taskId: string) {
  pruneA2aTasks();
  return a2aTasks.get(taskId) ?? null;
}

function serializeA2aTask(task: A2aTask, baseUrl: string) {
  return {
    id: task.id,
    method: task.method,
    action: task.action,
    status: task.status,
    createdAt: toA2aIso(task.createdAtMs),
    updatedAt: toA2aIso(task.updatedAtMs),
    completedAt: task.completedAtMs ? toA2aIso(task.completedAtMs) : null,
    canceledAt: task.canceledAtMs ? toA2aIso(task.canceledAtMs) : null,
    expiresAt: toA2aIso(task.expiresAtMs),
    input: task.input,
    output: task.output,
    error: task.error,
    links: {
      events: `${baseUrl}/api/v1/a2a/tasks/${task.id}/events`
    },
    events: task.events
  };
}

function makeJsonRpcResponse(id: unknown, result: unknown) {
  return {
    jsonrpc: '2.0',
    id: id ?? null,
    result
  };
}

function makeJsonRpcError(id: unknown, code: number, message: string, data?: unknown) {
  return {
    jsonrpc: '2.0',
    id: id ?? null,
    error: {
      code,
      message,
      ...(data === undefined ? {} : { data })
    }
  };
}

function firstString(...values: unknown[]) {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  return '';
}

function optionalNumber(value: unknown) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
}

function optionalBoolean(value: unknown) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true' || normalized === '1') return true;
    if (normalized === 'false' || normalized === '0') return false;
  }
  return undefined;
}

function optionalStringArray(value: unknown) {
  if (!Array.isArray(value)) return undefined;
  const cleaned = value
    .map((entry) => (typeof entry === 'string' ? entry.trim() : ''))
    .filter(Boolean);
  return cleaned.length > 0 ? cleaned : undefined;
}

function encodeQuery(values: Record<string, unknown>) {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value === undefined || value === null || value === '') continue;
    search.set(key, String(value));
  }
  const encoded = search.toString();
  return encoded ? `?${encoded}` : '';
}

function buildA2aActionRequest(action: string, args: Record<string, unknown>, fallbackAgentName: string | null) {
  switch (action) {
    case 'search': {
      const query = firstString(args.q, args.query);
      const tag = firstString(args.tag);
      const sortRaw = firstString(args.sort).toLowerCase();
      const sort = sortRaw === 'recent' ? 'recent' : sortRaw === 'quality' ? 'quality' : undefined;
      const page = optionalNumber(args.page);
      return {
        method: 'GET' as const,
        url: `/api/v1/search${encodeQuery({ q: query || undefined, tag: tag || undefined, sort, page })}`
      };
    }
    case 'fetch': {
      const id = firstString(args.id, args.questionId);
      if (!id) throw new Error('fetch requires id or questionId');
      return { method: 'GET' as const, url: `/api/v1/questions/${encodeURIComponent(id)}` };
    }
    case 'answer': {
      const query = firstString(args.query, args.q);
      if (!query) throw new Error('answer requires query');
      return {
        method: 'POST' as const,
        url: '/answer',
        payload: {
          query,
          top_k: optionalNumber(args.top_k ?? args.topK),
          include_evidence: optionalBoolean(args.include_evidence ?? args.includeEvidence),
          mode: firstString(args.mode) || undefined,
          max_chars_per_evidence: optionalNumber(args.max_chars_per_evidence ?? args.maxCharsPerEvidence)
        }
      };
    }
    case 'next_best_job': {
      const agentName = firstString(args.agentName, args.agent, fallbackAgentName);
      return {
        method: 'GET' as const,
        url: `/api/v1/agent/next-best-job${encodeQuery({ agentName: agentName || undefined })}`
      };
    }
    case 'next_job': {
      const agentName = firstString(args.agentName, args.agent, fallbackAgentName);
      return {
        method: 'GET' as const,
        url: `/api/v1/agent/jobs/next${encodeQuery({ agentName: agentName || undefined })}`
      };
    }
    case 'answer_next_job': {
      const agentName = firstString(args.agentName, args.agent, fallbackAgentName);
      return {
        method: 'POST' as const,
        url: `/api/v1/agent/jobs/answer-next${encodeQuery({ agentName: agentName || undefined })}`,
        payload: {
          bodyMd: firstString(args.bodyMd, args.body, args.markdown) || undefined,
          mode: firstString(args.mode) || undefined,
          topK: optionalNumber(args.topK ?? args.top_k),
          includeEvidence: optionalBoolean(args.includeEvidence ?? args.include_evidence),
          ttlMinutes: optionalNumber(args.ttlMinutes),
          forceTakeover: optionalBoolean(args.forceTakeover),
          acceptToken: firstString(args.acceptToken) || undefined,
          acceptIfOwner: optionalBoolean(args.acceptIfOwner),
          autoVerify: optionalBoolean(args.autoVerify)
        }
      };
    }
    case 'agent_quickstart': {
      const agentName = firstString(args.agentName, args.agent, fallbackAgentName);
      return {
        method: 'GET' as const,
        url: `/api/v1/agent/quickstart${encodeQuery({ agentName: agentName || undefined })}`
      };
    }
    case 'install_guides': {
      const agentName = firstString(args.agentName, args.agent, fallbackAgentName);
      const target = firstString(args.target).toLowerCase();
      const selectedTarget = target === 'claude_code' || target === 'cursor' || target === 'custom_http'
        ? target
        : undefined;
      return {
        method: 'GET' as const,
        url: `/api/v1/agent/install-guides${encodeQuery({
          agentName: agentName || undefined,
          target: selectedTarget
        })}`
      };
    }
    case 'questions_unanswered': {
      const tag = firstString(args.tag);
      return {
        method: 'GET' as const,
        url: `/api/v1/questions/unanswered${encodeQuery({
          tag: tag || undefined,
          page: optionalNumber(args.page),
          limit: optionalNumber(args.limit)
        })}`
      };
    }
    case 'solved_feed': {
      const sourceType = firstString(args.sourceType, args.source);
      return {
        method: 'GET' as const,
        url: `/api/v1/feed/solved${encodeQuery({
          since: firstString(args.since) || undefined,
          days: optionalNumber(args.days),
          limit: optionalNumber(args.limit),
          includeSynthetic: optionalBoolean(args.includeSynthetic),
          sourceType: sourceType || undefined
        })}`
      };
    }
    case 'pending_acceptance': {
      const agentName = firstString(args.agentName, args.agent, fallbackAgentName);
      return {
        method: 'GET' as const,
        url: `/api/v1/questions/pending-acceptance${encodeQuery({
          agentName: agentName || undefined,
          limit: optionalNumber(args.limit),
          minAnswerAgeMinutes: optionalNumber(args.minAnswerAgeMinutes)
        })}`
      };
    }
    case 'subscribe': {
      const agentName = firstString(args.agentName, args.agent, fallbackAgentName);
      if (!agentName) throw new Error('subscribe requires agentName or X-Agent-Name');
      return {
        method: 'POST' as const,
        url: '/api/v1/subscriptions',
        payload: {
          agentName,
          tags: optionalStringArray(args.tags),
          events: optionalStringArray(args.events),
          webhookUrl: firstString(args.webhookUrl) || undefined,
          webhookSecret: firstString(args.webhookSecret) || undefined,
          active: optionalBoolean(args.active)
        }
      };
    }
    case 'agent_inbox': {
      const agentName = firstString(args.agentName, args.agent, fallbackAgentName);
      return {
        method: 'GET' as const,
        url: `/api/v1/agent/inbox${encodeQuery({
          agentName: agentName || undefined,
          limit: optionalNumber(args.limit),
          markDelivered: optionalBoolean(args.markDelivered)
        })}`
      };
    }
    case 'create_question': {
      const title = firstString(args.title);
      const bodyMd = firstString(args.bodyMd, args.body, args.markdown);
      if (!title || !bodyMd) throw new Error('create_question requires title and bodyMd');
      return {
        method: 'POST' as const,
        url: '/api/v1/questions',
        payload: {
          title,
          bodyMd,
          tags: optionalStringArray(args.tags),
          force: optionalBoolean(args.force)
        }
      };
    }
    case 'create_answer': {
      const id = firstString(args.id, args.questionId);
      const bodyMd = firstString(args.bodyMd, args.body, args.markdown);
      if (!id || !bodyMd) throw new Error('create_answer requires question id and bodyMd');
      return {
        method: 'POST' as const,
        url: `/api/v1/questions/${encodeURIComponent(id)}/answers`,
        payload: { bodyMd }
      };
    }
    case 'answer_job': {
      const id = firstString(args.id, args.questionId);
      const bodyMd = firstString(args.bodyMd, args.body, args.markdown);
      if (!id || !bodyMd) throw new Error('answer_job requires question id and bodyMd');
      return {
        method: 'POST' as const,
        url: `/api/v1/questions/${encodeURIComponent(id)}/answer-job`,
        payload: {
          bodyMd,
          ttlMinutes: optionalNumber(args.ttlMinutes),
          forceTakeover: optionalBoolean(args.forceTakeover),
          acceptToken: firstString(args.acceptToken) || undefined,
          acceptIfOwner: optionalBoolean(args.acceptIfOwner),
          autoVerify: optionalBoolean(args.autoVerify)
        }
      };
    }
    case 'claim_question': {
      const id = firstString(args.id, args.questionId);
      if (!id) throw new Error('claim_question requires question id');
      return {
        method: 'POST' as const,
        url: `/api/v1/questions/${encodeURIComponent(id)}/claim`,
        payload: {
          ttlMinutes: optionalNumber(args.ttlMinutes),
          agentName: firstString(args.agentName, args.agent, fallbackAgentName) || undefined
        }
      };
    }
    case 'release_claim': {
      const id = firstString(args.id, args.questionId);
      const claimId = firstString(args.claimId);
      if (!id || !claimId) throw new Error('release_claim requires question id and claimId');
      return {
        method: 'POST' as const,
        url: `/api/v1/questions/${encodeURIComponent(id)}/claims/${encodeURIComponent(claimId)}/release`
      };
    }
    case 'accept_answer': {
      const id = firstString(args.id, args.questionId);
      const answerId = firstString(args.answerId);
      if (!id || !answerId) throw new Error('accept_answer requires question id and answerId');
      return {
        method: 'POST' as const,
        url: `/api/v1/questions/${encodeURIComponent(id)}/accept/${encodeURIComponent(answerId)}`
      };
    }
    case 'agent_scorecard': {
      const agentName = firstString(args.agentName, args.agent, fallbackAgentName);
      if (!agentName) throw new Error('agent_scorecard requires agentName');
      return {
        method: 'GET' as const,
        url: `/api/v1/agents/${encodeURIComponent(agentName)}/scorecard${encodeQuery({
          days: optionalNumber(args.days)
        })}`
      };
    }
    case 'seasons_monthly': {
      return {
        method: 'GET' as const,
        url: `/api/v1/incentives/seasons/monthly${encodeQuery({
          months: optionalNumber(args.months),
          limit: optionalNumber(args.limit),
          includeSynthetic: optionalBoolean(args.includeSynthetic)
        })}`
      };
    }
    case 'payouts_history': {
      return {
        method: 'GET' as const,
        url: `/api/v1/incentives/payouts/history${encodeQuery({
          page: optionalNumber(args.page),
          limit: optionalNumber(args.limit),
          agentName: firstString(args.agentName, args.agent) || undefined,
          reason: firstString(args.reason) || undefined
        })}`
      };
    }
    default:
      throw new Error(`Unsupported action: ${action}`);
  }
}

function parseInjectedBody(responseBody: string, contentType: string | undefined) {
  const trimmed = responseBody.trim();
  if (!trimmed) return null;
  const shouldParseJson = (contentType ?? '').includes('application/json') || trimmed.startsWith('{') || trimmed.startsWith('[');
  if (!shouldParseJson) return trimmed;
  return parseJsonMaybe(trimmed) ?? trimmed;
}

async function runA2aActionViaInject(request: {
  headers: Record<string, string | string[] | undefined>;
}, action: string, args: Record<string, unknown>, fallbackAgentName: string | null) {
  const call = buildA2aActionRequest(action, args, fallbackAgentName);
  const passThroughHeaders = [
    'authorization',
    'x-agent-name',
    'x-agent-signature',
    'x-agent-timestamp',
    'x-client-name',
    'x-mcp-client-name',
    'mcp-client-name',
    'host',
    'x-forwarded-host',
    'x-forwarded-proto',
    'x-forwarded-port',
    'user-agent'
  ];
  const headers: Record<string, string> = {};
  for (const key of passThroughHeaders) {
    const value = normalizeHeader(request.headers[key]);
    if (value) headers[key] = value;
  }
  headers['x-a2a-proxy'] = '1';
  const injected = await fastify.inject({
    method: call.method,
    url: call.url,
    headers,
    ...(call.payload !== undefined ? { payload: call.payload } : {})
  });
  const payload = parseInjectedBody(injected.body, injected.headers['content-type']);
  return {
    statusCode: injected.statusCode,
    ok: injected.statusCode >= 200 && injected.statusCode < 300,
    payload,
    route: call.url,
    method: call.method
  };
}

function markA2aTaskTerminal(task: A2aTask, status: Extract<A2aTaskStatus, 'completed' | 'failed' | 'canceled'>) {
  task.status = status;
  task.updatedAtMs = Date.now();
  task.completedAtMs = task.updatedAtMs;
  task.expiresAtMs = task.updatedAtMs + A2A_TASK_TTL_MS;
}

function isTerminalA2aStatus(status: A2aTaskStatus) {
  return status === 'completed' || status === 'failed' || status === 'canceled';
}


function parse<T>(schema: z.ZodSchema<T>, input: unknown, reply: { code: (code: number) => { send: (payload: unknown) => void } }) {
  const result = schema.safeParse(input);
  if (!result.success) {
    reply.code(400).send({ error: 'Invalid request', issues: result.error.flatten() });
    return null;
  }
  return result.data;
}

async function requireAdmin(request: { headers: Record<string, string | string[] | undefined> }, reply: any) {
  if (!ADMIN_TOKEN) {
    reply.code(500).send({ error: 'ADMIN_TOKEN is not configured' });
    return false;
  }
  const token = request.headers['x-admin-token'];
  const value = Array.isArray(token) ? token[0] : token;
  if (!value || value !== ADMIN_TOKEN) {
    reply.code(401).send({ error: 'Unauthorized' });
    return false;
  }
  return true;
}

async function requireAgentEventToken(request: { headers: Record<string, string | string[] | undefined> }, reply: any) {
  if (!AGENT_EVENT_TOKEN) {
    reply.code(500).send({ error: 'AGENT_EVENT_TOKEN is not configured' });
    return false;
  }
  const token = request.headers['x-agent-event-token'];
  const value = Array.isArray(token) ? token[0] : token;
  if (!value || value !== AGENT_EVENT_TOKEN) {
    reply.code(401).send({ error: 'Unauthorized' });
    return false;
  }
  return true;
}

type ApiKeyWithUser = Prisma.ApiKeyGetPayload<{ include: { user: true } }>;

function toNullableDailyLimit(value: number) {
  if (!Number.isFinite(value)) return null;
  if (value <= 0) return null;
  return Math.round(value);
}

function allowKeylessIdentityForIp(ip: string, identityHash: string) {
  if (KEYLESS_MAX_IDENTITIES_PER_IP_PER_DAY <= 0) return true;
  const today = getUtcDateKey();
  const prefix = `${today}|`;
  for (const key of keylessIdentityBudget.keys()) {
    if (!key.startsWith(prefix)) {
      keylessIdentityBudget.delete(key);
    }
  }
  const bucketKey = `${today}|${ip || 'unknown'}`;
  let bucket = keylessIdentityBudget.get(bucketKey);
  if (!bucket) {
    bucket = new Set<string>();
    keylessIdentityBudget.set(bucketKey, bucket);
  }
  if (bucket.has(identityHash)) return true;
  if (bucket.size >= KEYLESS_MAX_IDENTITIES_PER_IP_PER_DAY) return false;
  bucket.add(identityHash);
  return true;
}

async function resolveKeylessManagedApiKey(
  request: {
    headers: Record<string, string | string[] | undefined>;
    method?: string;
    raw?: { url?: string };
    url?: string;
    routerPath?: string;
    routeOptions?: { url?: string };
    ip?: string;
    socket?: { remoteAddress?: string };
  },
  reply: any
): Promise<
  | { status: 'ok'; apiKey: ApiKeyWithUser; boundAgentName: string; actorType: ActorType }
  | { status: 'ineligible' }
  | { status: 'failed' }
> {
  if (!KEYLESS_AUTH_ENABLED) return { status: 'ineligible' };
  const route = resolveRoute(request as RouteRequest);
  if (!KEYLESS_AUTH_ALLOWED_ROUTES.has(route)) return { status: 'ineligible' };

  const presentedAgentName = normalizeAgentOrNull(getAgentName(request.headers));
  if (!presentedAgentName && !KEYLESS_AUTH_ALLOW_ANONYMOUS) {
    reply.code(401).send({
      error: 'Missing API key',
      hint: 'Send X-Agent-Name for keyless onboarding, or Authorization: Bearer <api-key>.'
    });
    return { status: 'failed' };
  }

  const ip = getClientIp(request as RouteRequest & { ip?: string; socket?: { remoteAddress?: string } }) ?? 'unknown';
  const userAgent = normalizeHeader(request.headers['user-agent']).trim().toLowerCase().slice(0, 256);
  const identitySeed = presentedAgentName
    ? `agent:${presentedAgentName}`
    : `anon:${ip}|${userAgent || 'unknown'}`;
  const identityHash = sha256(`keyless:${identitySeed}`);
  if (!allowKeylessIdentityForIp(ip, identityHash)) {
    reply.code(429).send({
      error: 'Too many keyless identities from this IP today.',
      limit: KEYLESS_MAX_IDENTITIES_PER_IP_PER_DAY
    });
    return { status: 'failed' };
  }

  const boundAgentName = normalizeAgentOrNull(
    presentedAgentName ?? `anon-${identityHash.slice(0, 12)}`
  );
  if (!boundAgentName) {
    reply.code(400).send({ error: 'Unable to resolve agent identity for keyless onboarding.' });
    return { status: 'failed' };
  }

  const actorType = KEYLESS_AUTH_ACTOR_TYPE;
  const userHandle = `keyless-${identityHash.slice(0, 24)}`;
  const user = await prisma.user.upsert({
    where: { handle: userHandle },
    update: {},
    create: { handle: userHandle }
  });
  await ensureAgentProfile(boundAgentName);

  const keyName = buildApiKeyName('keyless', {
    boundAgentName,
    actorType,
    signatureRequired: false
  });
  const dailyWriteLimit = toNullableDailyLimit(KEYLESS_DAILY_WRITE_LIMIT);
  const dailyQuestionLimit = toNullableDailyLimit(KEYLESS_DAILY_QUESTION_LIMIT);
  const dailyAnswerLimit = toNullableDailyLimit(KEYLESS_DAILY_ANSWER_LIMIT);

  let apiKey = await prisma.apiKey.findFirst({
    where: {
      userId: user.id,
      name: keyName,
      revokedAt: null,
      OR: [{ expiresAt: null }, { expiresAt: { gt: new Date() } }]
    },
    include: { user: true },
    orderBy: { createdAt: 'desc' }
  });

  let keyCreated = false;
  if (!apiKey) {
    const key = `a2a_${crypto.randomBytes(24).toString('hex')}`;
    const keyPrefix = key.slice(0, 8);
    const keyHash = sha256(key);
    apiKey = await prisma.apiKey.create({
      data: {
        userId: user.id,
        name: keyName,
        keyPrefix,
        keyHash,
        scopes: ['write:questions', 'write:answers'],
        expiresAt: null,
        dailyWriteLimit,
        dailyQuestionLimit,
        dailyAnswerLimit
      },
      include: { user: true }
    });
    keyCreated = true;
  } else {
    const hasQuestionScope = apiKey.scopes.includes('write:questions') || apiKey.scopes.includes('write:question');
    const hasAnswerScope = apiKey.scopes.includes('write:answers') || apiKey.scopes.includes('write:answer');
    const needsScopePatch = !hasQuestionScope || !hasAnswerScope;
    const needsLimitPatch =
      apiKey.dailyWriteLimit !== dailyWriteLimit
      || apiKey.dailyQuestionLimit !== dailyQuestionLimit
      || apiKey.dailyAnswerLimit !== dailyAnswerLimit;
    if (needsScopePatch || needsLimitPatch) {
      apiKey = await prisma.apiKey.update({
        where: { id: apiKey.id },
        data: {
          scopes: ['write:questions', 'write:answers'],
          dailyWriteLimit,
          dailyQuestionLimit,
          dailyAnswerLimit
        },
        include: { user: true }
      });
    }
  }

  if (keyCreated && KEYLESS_AUTO_SUBSCRIBE) {
    await ensureTrialAutoSubscription(boundAgentName);
  }

  reply.header('X-A2ABench-Auth-Mode', 'keyless-managed');
  reply.header('X-A2ABench-Agent-Name', boundAgentName.slice(0, 128));

  return {
    status: 'ok',
    apiKey,
    boundAgentName,
    actorType
  };
}

async function requireApiKey(
  request: {
    headers: Record<string, string | string[] | undefined>;
    method?: string;
    raw?: { url?: string };
    url?: string;
    authMeta?: RequestAuthMeta;
  },
  reply: any,
  scope?: string
) {
  const isWrite = isWriteMethod(request.method) || Boolean(scope && scope.startsWith('write:'));
  const key = extractBearerToken(request.headers);
  let apiKey: ApiKeyWithUser | null = null;
  let authMode: RequestAuthMeta['authMode'] = 'bearer';
  let fallbackReason: RequestAuthMeta['fallbackReason'] = null;

  if (key) {
    const keyPrefix = key.slice(0, 8);
    const keyHash = sha256(key);
    apiKey = await prisma.apiKey.findFirst({
      where: { keyPrefix, keyHash, revokedAt: null },
      include: { user: true }
    });
    const invalidKey = !apiKey;
    const expiredKey = Boolean(apiKey?.expiresAt && apiKey.expiresAt.getTime() < Date.now());
    if (invalidKey || expiredKey) {
      const failedReason: RequestAuthMeta['fallbackReason'] = invalidKey ? 'invalid_api_key' : 'expired_api_key';
      if (AUTH_INVALID_BEARER_FALLBACK_TO_KEYLESS) {
        const keyless = await resolveKeylessManagedApiKey(request, reply);
        if (keyless.status === 'ok') {
          apiKey = keyless.apiKey;
          authMode = 'keyless_managed';
          fallbackReason = failedReason;
          reply.header('X-A2ABench-Auth-Fallback', fallbackReason);
        } else if (keyless.status === 'failed') {
          return null;
        }
      }
      const stillInvalid = !apiKey || Boolean(apiKey.expiresAt && apiKey.expiresAt.getTime() < Date.now());
      if (stillInvalid) {
        reply.code(401).send({ error: failedReason === 'expired_api_key' ? 'API key expired' : 'Invalid API key' });
        return null;
      }
    }
  } else {
    const keyless = await resolveKeylessManagedApiKey(request, reply);
    if (keyless.status === 'ok') {
      apiKey = keyless.apiKey;
      authMode = 'keyless_managed';
    } else if (keyless.status === 'failed') {
      return null;
    } else {
      reply.code(401).send({
        error: 'Missing API key',
        hint: 'Send X-Agent-Name for keyless onboarding, or Authorization: Bearer <api-key>.'
      });
      return null;
    }
  }

  if (!apiKey) {
    reply.code(401).send({ error: 'Invalid API key' });
    return null;
  }

  if (scope && apiKey.scopes.length > 0) {
    const aliases: Record<string, string[]> = {
      'write:questions': ['write:questions', 'write:question'],
      'write:answers': ['write:answers', 'write:answer']
    };
    const allowed = aliases[scope] ?? [scope];
    const hasScope = apiKey.scopes.some((value) => allowed.includes(value));
    if (!hasScope) {
      reply.code(403).send({ error: 'Insufficient scope' });
      return null;
    }
  }

  const identityMeta = parseApiKeyIdentityMeta(apiKey.name);
  let boundAgentName = deriveDirectAgentNameFromProxy(identityMeta.boundAgentName);
  const presentedAgentName = normalizeAgentOrNull(getAgentName(request.headers));
  const actorType = normalizeActorType(identityMeta.actorType);

  if (isWrite) {
    if (AGENT_IDENTITY_REQUIRE_HEADER_FOR_WRITES && !presentedAgentName) {
      reply.code(400).send({
        error: 'X-Agent-Name is required for authenticated writes.'
      });
      return null;
    }

    if (!boundAgentName && presentedAgentName && AGENT_IDENTITY_AUTO_BIND_ON_FIRST_WRITE) {
      const updatedName = buildApiKeyName(identityMeta.baseName, {
        boundAgentName: presentedAgentName,
        actorType,
        signatureRequired: identityMeta.signatureRequired
      });
      if (updatedName !== apiKey.name) {
        apiKey = await prisma.apiKey.update({
          where: { id: apiKey.id },
          data: { name: updatedName },
          include: { user: true }
        });
        boundAgentName = presentedAgentName;
      }
    }

    if (AGENT_IDENTITY_ENFORCE_BOUND_MATCH && boundAgentName && presentedAgentName && boundAgentName !== presentedAgentName) {
      reply.code(403).send({
        error: 'Agent identity mismatch for API key.',
        expectedAgentName: boundAgentName
      });
      return null;
    }
  }

  const signatureRequired = isWrite
    && authMode === 'bearer'
    && (AGENT_SIGNATURE_ENFORCE_WRITES || identityMeta.signatureRequired);
  let signatureVerified = false;
  const signature = normalizeHeader(request.headers['x-agent-signature']).trim().toLowerCase();
  const timestampRaw = normalizeHeader(request.headers['x-agent-timestamp']).trim();
  const signatureProvided = Boolean(signature) || Boolean(timestampRaw);
  const canVerifySignature = Boolean(key);
  if (signatureRequired || signatureProvided) {
    if (!signature || !timestampRaw) {
      if (signatureRequired) {
        reply.code(401).send({
          error: 'Missing request signature headers.',
          requiredHeaders: ['X-Agent-Timestamp', 'X-Agent-Signature']
        });
        return null;
      }
    } else {
      const timestampMs = parseSignatureTimestamp(timestampRaw);
      if (!timestampMs) {
        if (signatureRequired) {
          reply.code(401).send({ error: 'Invalid X-Agent-Timestamp.' });
          return null;
        }
      } else {
        const skewMs = Math.abs(Date.now() - timestampMs);
        if (skewMs > AGENT_SIGNATURE_MAX_SKEW_SECONDS * 1000) {
          if (signatureRequired) {
            reply.code(401).send({ error: 'Request signature timestamp skew too large.' });
            return null;
          }
        } else if (canVerifySignature) {
          const path = getSignatureRequestPath(request);
          const expected = computeAgentSignature(key!, request.method ?? 'POST', path, timestampRaw, apiKey.keyPrefix);
          signatureVerified = timingSafeHexEqual(expected, signature);
          if (!signatureVerified && signatureRequired) {
            reply.code(401).send({ error: 'Invalid request signature.' });
            return null;
          }
        } else if (signatureRequired) {
          reply.code(401).send({ error: 'Request signatures require bearer API keys.' });
          return null;
        }
      }
    }
  }

  const identityVerified = Boolean(
    boundAgentName && (!presentedAgentName || boundAgentName === presentedAgentName)
  );
  request.authMeta = {
    apiKeyId: apiKey.id,
    apiKeyPrefix: apiKey.keyPrefix,
    authMode,
    actorType,
    boundAgentName,
    presentedAgentName,
    identityVerified,
    signatureVerified,
    signatureRequired: Boolean(signatureRequired),
    fallbackFromBearerError: Boolean(fallbackReason),
    fallbackReason,
    isWrite
  };
  return apiKey;
}

async function validateApiKey(request: { headers: Record<string, string | string[] | undefined> }) {
  const key = extractBearerToken(request.headers);
  if (!key) return { ok: false, reason: 'Missing API key' };
  const keyPrefix = key.slice(0, 8);
  const keyHash = sha256(key);
  const apiKey = await prisma.apiKey.findFirst({
    where: { keyPrefix, keyHash, revokedAt: null }
  });
  if (!apiKey) return { ok: false, reason: 'Invalid API key' };
  if (apiKey.expiresAt && apiKey.expiresAt.getTime() < Date.now()) {
    return { ok: false, reason: 'API key expired' };
  }
  return { ok: true, keyPrefix };
}

function getLlmQuotaKey(request: RouteRequest, agentName: string | null) {
  const keyPrefix = extractApiKeyPrefix(request.headers);
  if (keyPrefix) return `key:${keyPrefix}`;
  if (agentName) return `agent:${normalizeAgentName(agentName)}`;
  return `ip:${request.ip ?? 'unknown'}`;
}

function getUtcDateKey(now = new Date()) {
  return now.toISOString().slice(0, 10);
}

function allowLlmForRequest(request: RouteRequest, agentName: string | null) {
  if (!LLM_ENABLED) {
    return {
      allowed: false,
      message: 'LLM disabled; returning retrieved evidence only.',
      warnings: ['LLM disabled by policy.']
    };
  }
  if (LLM_AGENT_ALLOWLIST.size > 0) {
    const normalized = normalizeAgentName(agentName);
    if (!normalized || !LLM_AGENT_ALLOWLIST.has(normalized)) {
      return {
        allowed: false,
        message: 'LLM disabled for this agent; returning retrieved evidence only.',
        warnings: ['LLM disabled for this agent.']
      };
    }
  }
  return { allowed: true, message: '', warnings: [] };
}

function allowLlmByQuota(request: RouteRequest, agentName: string | null) {
  if (LLM_DAILY_LIMIT <= 0) return { allowed: true };
  const key = getLlmQuotaKey(request, agentName);
  const today = getUtcDateKey();
  const entry = llmUsage.get(key);
  if (!entry || entry.dateKey !== today) {
    llmUsage.set(key, { dateKey: today, count: 1 });
    return { allowed: true };
  }
  if (entry.count >= LLM_DAILY_LIMIT) {
    return { allowed: false };
  }
  entry.count += 1;
  return { allowed: true };
}

const CAPTURED_ROUTES = new Set([
  '/api/v1/a2a',
  '/api/v1/a2a/tasks/:id/events',
  '/api/v1/auth/trial-key',
  '/api/v1/questions',
  '/api/v1/questions/:id',
  '/api/v1/questions/pending-acceptance',
  '/api/v1/questions/:id/answer-job',
  '/api/v1/questions/:id/claim',
  '/api/v1/questions/:id/claims',
  '/api/v1/questions/:id/claims/:claimId/release',
  '/api/v1/questions/:id/answers',
  '/api/v1/questions/:id/bounty',
  '/api/v1/questions/:id/accept/:answerId',
  '/api/v1/questions/:id/accept/:answerId/link',
  '/api/v1/accept-links/:token',
  '/api/v1/accept-links',
  '/api/v1/answers/:id/vote',
  '/api/v1/agent/inbox',
  '/api/v1/subscriptions',
  '/api/v1/subscriptions/:id/disable',
  '/api/v1/agent/jobs/next',
  '/api/v1/agent/jobs/answer-next',
  '/api/v1/agent/next-best-job',
  '/api/v1/search',
  '/api/v1/questions/unanswered',
  '/api/v1/feed/unanswered',
  '/api/v1/feed/solved',
  '/api/v1/agent/quickstart',
  '/api/v1/agent/install-guides',
  '/api/v1/agents/leaderboard',
  '/api/v1/agents/top-solved-weekly',
  '/api/v1/agents/:agentName/credits',
  '/api/v1/agents/:agentName/scorecard',
  '/api/v1/incentives/rules',
  '/api/v1/incentives/payouts/history',
  '/api/v1/incentives/seasons/monthly',
  '/api/v1/admin/traction/funnel',
  '/api/v1/admin/traction/scorecard',
  '/api/v1/admin/traction/alerts/send',
  '/api/v1/admin/retention/weekly',
  '/api/v1/admin/delivery/process',
  '/api/v1/admin/delivery/queue',
  '/api/v1/admin/delivery/requeue-opened-unanswered',
  '/api/v1/admin/subscriptions/health',
  '/api/v1/admin/subscriptions/prune',
  '/api/v1/admin/reminders/process',
  '/api/v1/admin/autoclose/process',
  '/api/v1/admin/import/questions',
  '/api/v1/admin/import/sources/run',
  '/api/v1/admin/source-callbacks/process',
  '/api/v1/admin/partners/teams',
  '/api/v1/admin/partners/teams/:id/members',
  '/api/v1/admin/partners/teams/:id/metrics/weekly',
  '/api/v1/bounties'
]);

function isAgentTraffic(agentName: string | null, userAgent: string | null) {
  if (agentName) return true;
  if (!userAgent) return false;
  return /(chatgpt|claude|agent|mcp|bot)/i.test(userAgent);
}

async function pruneAgentPayloadEvents() {
  const ttlMs = AGENT_PAYLOAD_TTL_HOURS * 60 * 60 * 1000;
  if (ttlMs > 0) {
    const cutoff = new Date(Date.now() - ttlMs);
    await prisma.agentPayloadEvent.deleteMany({ where: { createdAt: { lt: cutoff } } });
  }
  if (AGENT_PAYLOAD_MAX_EVENTS > 0) {
    const total = await prisma.agentPayloadEvent.count();
    if (total > AGENT_PAYLOAD_MAX_EVENTS) {
      const removeCount = total - AGENT_PAYLOAD_MAX_EVENTS;
      const oldest: Array<{ id: string }> = await prisma.agentPayloadEvent.findMany({
        select: { id: true },
        orderBy: { createdAt: 'asc' },
        take: removeCount
      });
      if (oldest.length > 0) {
        await prisma.agentPayloadEvent.deleteMany({ where: { id: { in: oldest.map((row) => row.id) } } });
      }
    }
  }
}

async function storeAgentPayloadEvent(entry: {
  source: string;
  kind: string;
  method?: string | null;
  route?: string | null;
  status?: number | null;
  durationMs?: number | null;
  tool?: string | null;
  requestId?: string | null;
  agentName?: string | null;
  userAgent?: string | null;
  ip?: string | null;
  apiKeyPrefix?: string | null;
  requestBody?: unknown;
  responseBody?: unknown;
}) {
  if (!CAPTURE_AGENT_PAYLOADS) return;
  const requestBody = entry.requestBody !== undefined ? stringifyPayload(entry.requestBody) : null;
  const responseBody = entry.responseBody !== undefined ? stringifyPayload(entry.responseBody) : null;

  await prisma.agentPayloadEvent.create({
    data: {
      source: entry.source,
      kind: entry.kind,
      method: entry.method ?? null,
      route: entry.route ?? null,
      status: entry.status ?? null,
      durationMs: entry.durationMs ?? null,
      tool: entry.tool ?? null,
      requestId: entry.requestId ?? null,
      agentName: entry.agentName ?? null,
      userAgent: entry.userAgent ?? null,
      ip: entry.ip ?? null,
      apiKeyPrefix: entry.apiKeyPrefix ?? null,
      requestBody,
      responseBody
    }
  });

  void pruneAgentPayloadEvents().catch(() => undefined);
}

async function storeExplicitAgentTelemetryEvent(entry: {
  source: string;
  kind: string;
  method?: string | null;
  route?: string | null;
  status?: number | null;
  durationMs?: number | null;
  tool?: string | null;
  requestId?: string | null;
  agentName?: string | null;
  userAgent?: string | null;
  ip?: string | null;
  apiKeyPrefix?: string | null;
  requestBody?: unknown;
  responseBody?: unknown;
}) {
  const requestBody = entry.requestBody !== undefined ? stringifyPayload(entry.requestBody) : null;
  const responseBody = entry.responseBody !== undefined ? stringifyPayload(entry.responseBody) : null;

  await prisma.agentPayloadEvent.create({
    data: {
      source: entry.source,
      kind: entry.kind,
      method: entry.method ?? null,
      route: entry.route ?? null,
      status: entry.status ?? null,
      durationMs: entry.durationMs ?? null,
      tool: entry.tool ?? null,
      requestId: entry.requestId ?? null,
      agentName: entry.agentName ?? null,
      userAgent: entry.userAgent ?? null,
      ip: entry.ip ?? null,
      apiKeyPrefix: entry.apiKeyPrefix ?? null,
      requestBody,
      responseBody
    }
  });

  void pruneAgentPayloadEvents().catch(() => undefined);
}

async function enforceWriteLimits(
  apiKey: { id: string; dailyWriteLimit: number | null; dailyQuestionLimit: number | null; dailyAnswerLimit: number | null },
  kind: 'question' | 'answer',
  reply: any
) {
  const limits = {
    dailyWrites: apiKey.dailyWriteLimit ?? null,
    dailyQuestions: apiKey.dailyQuestionLimit ?? null,
    dailyAnswers: apiKey.dailyAnswerLimit ?? null
  };
  if (!limits.dailyWrites && !limits.dailyQuestions && !limits.dailyAnswers) return true;
  const bucket = startOfUtcDay();
  const existing = await prisma.apiKeyUsage.findUnique({
    where: { apiKeyId_date: { apiKeyId: apiKey.id, date: bucket } }
  });
  const writeCount = existing?.writeCount ?? 0;
  const questionCount = existing?.questionCount ?? 0;
  const answerCount = existing?.answerCount ?? 0;
  const wouldWrite = writeCount + 1;
  const wouldQuestion = questionCount + (kind === 'question' ? 1 : 0);
  const wouldAnswer = answerCount + (kind === 'answer' ? 1 : 0);

  if (limits.dailyWrites !== null && wouldWrite > limits.dailyWrites) {
    reply.code(429).send({ error: 'Daily write limit reached', limits, resetAt: bucket.toISOString() });
    return false;
  }
  if (limits.dailyQuestions !== null && wouldQuestion > limits.dailyQuestions) {
    reply.code(429).send({ error: 'Daily question limit reached', limits, resetAt: bucket.toISOString() });
    return false;
  }
  if (limits.dailyAnswers !== null && wouldAnswer > limits.dailyAnswers) {
    reply.code(429).send({ error: 'Daily answer limit reached', limits, resetAt: bucket.toISOString() });
    return false;
  }

  await prisma.apiKeyUsage.upsert({
    where: { apiKeyId_date: { apiKeyId: apiKey.id, date: bucket } },
    update: {
      writeCount: { increment: 1 },
      questionCount: { increment: kind === 'question' ? 1 : 0 },
      answerCount: { increment: kind === 'answer' ? 1 : 0 }
    },
    create: {
      apiKeyId: apiKey.id,
      date: bucket,
      writeCount: 1,
      questionCount: kind === 'question' ? 1 : 0,
      answerCount: kind === 'answer' ? 1 : 0
    }
  });

  return true;
}

const ACCEPT_REPUTATION_REWARD = 15;

function normalizeAgentOrNull(value: string | null | undefined) {
  const normalized = normalizeAgentName(value);
  return normalized || null;
}

async function ensureAgentProfile(agentName: string | null | undefined) {
  const normalized = normalizeAgentOrNull(agentName);
  if (!normalized) return null;
  return prisma.agentProfile.upsert({
    where: { name: normalized },
    update: {},
    create: { name: normalized }
  });
}

async function addAgentReputation(agentName: string | null | undefined, delta: number, voteDelta = 0) {
  const normalized = normalizeAgentOrNull(agentName);
  if (!normalized || (!delta && !voteDelta)) return;
  await prisma.agentProfile.upsert({
    where: { name: normalized },
    update: {
      reputation: { increment: delta },
      voteScore: { increment: voteDelta }
    },
    create: {
      name: normalized,
      reputation: delta,
      voteScore: voteDelta
    }
  });
}

async function addAgentCredits(
  agentName: string | null | undefined,
  delta: number,
  reason: string,
  refs?: { questionId?: string | null; answerId?: string | null }
) {
  const normalized = normalizeAgentOrNull(agentName);
  if (!normalized || delta === 0) return;
  await prisma.$transaction([
    prisma.agentProfile.upsert({
      where: { name: normalized },
      update: { credits: { increment: delta } },
      create: { name: normalized, credits: delta }
    }),
    prisma.agentCreditLedger.create({
      data: {
        agentName: normalized,
        delta,
        reason,
        questionId: refs?.questionId ?? null,
        answerId: refs?.answerId ?? null
      }
    })
  ]);
}

async function incrementAgentAnswerCount(agentName: string | null | undefined) {
  const normalized = normalizeAgentOrNull(agentName);
  if (!normalized) return;
  await prisma.agentProfile.upsert({
    where: { name: normalized },
    update: { answersCount: { increment: 1 } },
    create: { name: normalized, answersCount: 1 }
  });
}

async function incrementAcceptedCount(agentName: string | null | undefined, delta: 1 | -1) {
  const normalized = normalizeAgentOrNull(agentName);
  if (!normalized) return;
  await prisma.agentProfile.upsert({
    where: { name: normalized },
    update: { acceptedCount: { increment: delta } },
    create: { name: normalized, acceptedCount: delta }
  });
}

async function getAnswerVoteMap(answerIds: string[]) {
  if (!answerIds.length) return new Map<string, number>();
  const rows = await prisma.answerVote.groupBy({
    by: ['answerId'],
    where: { answerId: { in: answerIds } },
    _sum: { value: true }
  });
  const map = new Map<string, number>();
  for (const row of rows) {
    map.set(row.answerId, row._sum.value ?? 0);
  }
  return map;
}

function getActiveBountyAmount(entry: { active: boolean; expiresAt: Date | null; amount: number } | null) {
  if (!entry || !entry.active) return 0;
  if (entry.expiresAt && entry.expiresAt.getTime() < Date.now()) return 0;
  return Math.max(0, entry.amount);
}

function subscriptionMatches(tags: string[], questionTags: string[]) {
  if (tags.length === 0) return true;
  const qset = new Set(questionTags.map((tag) => tag.toLowerCase()));
  return tags.some((tag) => qset.has(tag.toLowerCase()));
}

const SUBSCRIPTION_EVENT_TYPES = [
  'question.created',
  'question.accepted',
  'question.needs_acceptance',
  'question.acceptance_reminder'
] as const;
const SUBSCRIPTION_DEFAULT_EVENTS = [...SUBSCRIPTION_EVENT_TYPES] as const;

function subscriptionWantsEvent(events: string[] | null | undefined, eventName: string) {
  const normalized = (events ?? [])
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  const effective = normalized.length ? normalized : [...SUBSCRIPTION_DEFAULT_EVENTS];
  return effective.includes(eventName.toLowerCase());
}

type QuestionWebhookEventName = typeof SUBSCRIPTION_EVENT_TYPES[number];

type QuestionWebhookInput = {
  event: QuestionWebhookEventName;
  question: {
    id: string;
    title: string;
    bodyText: string;
    createdAt: Date;
    tags: string[];
    url: string;
    source?: {
      type: string | null;
      url: string | null;
      externalId: string | null;
      title: string | null;
    };
  };
  answer?: {
    id: string;
    agentName: string | null;
    createdAt: Date;
  };
  acceptance?: {
    acceptedAt: Date;
    acceptedAnswerId: string;
    acceptedAgentName: string | null;
    acceptedByAgentName: string | null;
    bountyPaid: number;
    starterBonusPaid: number;
  };
  prompt?: {
    message: string;
    suggestedAction: string;
    acceptUrl?: string | null;
    reminderStageHours?: number | null;
  };
};

function computeDeliveryBackoffMs(attemptCount: number) {
  const exponent = Math.max(0, attemptCount - 1);
  const ms = DELIVERY_RETRY_BASE_MS * Math.pow(2, exponent);
  return Math.min(DELIVERY_RETRY_MAX_MS, Math.max(DELIVERY_RETRY_BASE_MS, ms));
}

async function processDeliveryQueue(limit = DELIVERY_PROCESS_LIMIT) {
  const now = new Date();
  const take = Math.max(1, Math.min(500, limit));
  const due = await prisma.deliveryQueue.findMany({
    where: {
      deliveredAt: null,
      webhookUrl: { not: null },
      nextAttemptAt: { lte: now },
      attemptCount: { lt: DELIVERY_MAX_ATTEMPTS }
    },
    orderBy: [
      { nextAttemptAt: 'asc' },
      { createdAt: 'asc' }
    ],
    take
  });
  if (due.length === 0) return { processed: 0, delivered: 0, failed: 0, pending: 0 };

  let delivered = 0;
  let failed = 0;
  let pending = 0;

  for (const job of due) {
    const payloadText = JSON.stringify(job.payload);
    const headers: Record<string, string> = {
      'content-type': 'application/json',
      'x-a2abench-event': job.event
    };
    if (job.webhookSecret) {
      const signature = crypto.createHmac('sha256', job.webhookSecret).update(payloadText).digest('hex');
      headers['x-a2abench-signature'] = `sha256=${signature}`;
    }

    const attempts = job.attemptCount + 1;
    const maxAttempts = Math.max(1, job.maxAttempts || DELIVERY_MAX_ATTEMPTS);
    const attemptAt = new Date();
    try {
      const response = await fetch(job.webhookUrl!, {
        method: 'POST',
        headers,
        body: payloadText
      });
      if (response.ok) {
        await prisma.deliveryQueue.update({
          where: { id: job.id },
          data: {
            attemptCount: attempts,
            lastAttemptAt: attemptAt,
            lastStatus: response.status,
            lastError: null,
            deliveredAt: new Date()
          }
        });
        delivered += 1;
        continue;
      }

      const bodyText = (await response.text()).slice(0, 1000);
      await prisma.deliveryQueue.update({
        where: { id: job.id },
        data: {
          attemptCount: attempts,
          lastAttemptAt: attemptAt,
          lastStatus: response.status,
          lastError: bodyText || `HTTP ${response.status}`,
          nextAttemptAt: attempts >= maxAttempts
            ? new Date(Date.now() + DELIVERY_RETRY_MAX_MS)
            : new Date(Date.now() + computeDeliveryBackoffMs(attempts))
        }
      });
      if (attempts >= maxAttempts) failed += 1;
      else pending += 1;
    } catch (err) {
      await prisma.deliveryQueue.update({
        where: { id: job.id },
        data: {
          attemptCount: attempts,
          lastAttemptAt: attemptAt,
          lastStatus: null,
          lastError: err instanceof Error ? err.message.slice(0, 1000) : 'delivery_failed',
          nextAttemptAt: attempts >= maxAttempts
            ? new Date(Date.now() + DELIVERY_RETRY_MAX_MS)
            : new Date(Date.now() + computeDeliveryBackoffMs(attempts))
        }
      });
      if (attempts >= maxAttempts) failed += 1;
      else pending += 1;
    }
  }

  return { processed: due.length, delivered, failed, pending };
}

async function markAgentPullDeliveryOpened(
  agentName: string,
  preferredQuestionId?: string | null,
  options?: { fallbackToAny?: boolean; createIfMissing?: boolean }
) {
  const normalizedAgent = normalizeAgentOrNull(agentName);
  if (!normalizedAgent) return null;
  const now = new Date();
  const fallbackToAny = options?.fallbackToAny !== false;
  const createIfMissing = options?.createIfMissing === true;
  const baseWhere: Prisma.DeliveryQueueWhereInput = {
    agentName: normalizedAgent,
    event: 'question.created',
    deliveredAt: null,
    attemptCount: { lt: DELIVERY_MAX_ATTEMPTS }
  };

  const preferredQuestion = preferredQuestionId?.trim() ?? null;
  const target = preferredQuestion
    ? await prisma.deliveryQueue.findFirst({
        where: {
          ...baseWhere,
          questionId: preferredQuestion
        },
        orderBy: { createdAt: 'asc' }
      })
    : null;

  const fallback = target ?? (fallbackToAny
    ? await prisma.deliveryQueue.findFirst({
        where: baseWhere,
        orderBy: { createdAt: 'asc' }
      })
    : null);
  if (!fallback) {
    if (!createIfMissing || !preferredQuestion) return null;
    const existingOpened = await prisma.deliveryQueue.findFirst({
      where: {
        agentName: normalizedAgent,
        event: 'question.created',
        questionId: preferredQuestion,
        deliveredAt: { not: null }
      },
      orderBy: { deliveredAt: 'desc' }
    });
    if (existingOpened) {
      return {
        id: existingOpened.id,
        questionId: existingOpened.questionId ?? null,
        openedAt: existingOpened.deliveredAt?.toISOString() ?? now.toISOString(),
        via: 'pull_discovery'
      };
    }

    const subscription = await prisma.questionSubscription.findFirst({
      where: {
        agentName: normalizedAgent,
        active: true,
        webhookUrl: null
      },
      select: {
        id: true,
        events: true
      },
      orderBy: { createdAt: 'desc' }
    });
    if (!subscription || !subscriptionWantsEvent(subscription.events, 'question.created')) return null;

    const synthetic = await prisma.deliveryQueue.create({
      data: {
        subscriptionId: subscription.id,
        agentName: normalizedAgent,
        event: 'question.created',
        payload: {
          event: 'question.created',
          question: {
            id: preferredQuestion
          },
          meta: {
            openedVia: 'pull_discovery_backfill',
            openedAt: now.toISOString()
          }
        },
        questionId: preferredQuestion,
        answerId: null,
        webhookUrl: null,
        webhookSecret: null,
        attemptCount: 1,
        maxAttempts: DELIVERY_MAX_ATTEMPTS,
        nextAttemptAt: now,
        lastAttemptAt: now,
        deliveredAt: now,
        lastStatus: 200,
        lastError: 'opened_via_pull_discovery'
      }
    });

    return {
      id: synthetic.id,
      questionId: synthetic.questionId ?? null,
      openedAt: now.toISOString(),
      via: 'pull_discovery'
    };
  }

  await prisma.deliveryQueue.update({
    where: { id: fallback.id },
    data: {
      deliveredAt: now,
      lastAttemptAt: now,
      lastStatus: 200,
      lastError: 'opened_via_pull_discovery',
      attemptCount: { increment: 1 }
    }
  });

  return {
    id: fallback.id,
    questionId: fallback.questionId ?? null,
    openedAt: now.toISOString(),
    via: 'pull_discovery'
  };
}

async function markAgentPullDeliveriesOpened(
  agentName: string | null | undefined,
  questionIds: Array<string | null | undefined>,
  options?: { limit?: number }
) {
  const normalizedAgent = normalizeAgentOrNull(agentName);
  if (!normalizedAgent) return [];
  const limit = Math.max(1, Math.min(20, Number(options?.limit ?? 8)));
  const uniqueQuestionIds = Array.from(new Set(
    questionIds
      .map((value) => (value ?? '').trim())
      .filter(Boolean)
  )).slice(0, limit);
  if (uniqueQuestionIds.length === 0) return [];

  const opened: Array<{ id: string; questionId: string | null; openedAt: string; via: string }> = [];
  for (const questionId of uniqueQuestionIds) {
    const signal = await markAgentPullDeliveryOpened(normalizedAgent, questionId, { fallbackToAny: false });
    if (signal) opened.push(signal);
  }
  return opened;
}

async function processOpenedUnansweredRequeue(options?: { limit?: number; dryRun?: boolean }) {
  const enabled = DELIVERY_REQUEUE_OPENED_ENABLED;
  if (!enabled) {
    return {
      enabled: false,
      dryRun: options?.dryRun === true,
      scanned: 0,
      eligible: 0,
      requeued: 0,
      skipped: {
        answeredAfterOpen: 0,
        alreadyPending: 0,
        maxRequeuesReached: 0,
        inactiveSubscription: 0,
        eventNotEnabled: 0
      },
      windowMinutes: DELIVERY_REQUEUE_AFTER_MINUTES,
      maxPerQuestionSubscription: DELIVERY_REQUEUE_MAX_PER_QUESTION_SUBSCRIPTION
    };
  }

  const dryRun = options?.dryRun === true;
  const now = new Date();
  const staleBefore = new Date(now.getTime() - DELIVERY_REQUEUE_AFTER_MINUTES * 60 * 1000);
  const scanLimit = Math.max(1, Math.min(2000, options?.limit ?? DELIVERY_REQUEUE_SCAN_LIMIT));

  const openedRows = await prisma.deliveryQueue.findMany({
    where: {
      event: 'question.created',
      questionId: { not: null },
      deliveredAt: { not: null, lte: staleBefore }
    },
    select: {
      id: true,
      subscriptionId: true,
      agentName: true,
      event: true,
      payload: true,
      questionId: true,
      answerId: true,
      webhookUrl: true,
      webhookSecret: true,
      maxAttempts: true,
      createdAt: true,
      deliveredAt: true
    },
    orderBy: { deliveredAt: 'asc' },
    take: scanLimit
  });

  if (openedRows.length === 0) {
    return {
      enabled,
      dryRun,
      scanned: 0,
      eligible: 0,
      requeued: 0,
      skipped: {
        answeredAfterOpen: 0,
        alreadyPending: 0,
        maxRequeuesReached: 0,
        inactiveSubscription: 0,
        eventNotEnabled: 0
      },
      windowMinutes: DELIVERY_REQUEUE_AFTER_MINUTES,
      maxPerQuestionSubscription: DELIVERY_REQUEUE_MAX_PER_QUESTION_SUBSCRIPTION
    };
  }

  const questionIds = Array.from(new Set(
    openedRows
      .map((row) => row.questionId)
      .filter((value): value is string => Boolean(value && value.trim()))
  ));
  const subscriptionIds = Array.from(new Set(openedRows.map((row) => row.subscriptionId)));

  const [latestAnswerRows, pendingRows, queueCountRows, subscriptions] = await Promise.all([
    questionIds.length === 0
      ? Promise.resolve([] as Array<{ questionId: string; _max: { createdAt: Date | null } }>)
      : prisma.answer.groupBy({
          by: ['questionId'],
          where: { questionId: { in: questionIds } },
          _max: { createdAt: true }
        }),
    questionIds.length === 0 || subscriptionIds.length === 0
      ? Promise.resolve([] as Array<{ subscriptionId: string; questionId: string | null }>)
      : prisma.deliveryQueue.findMany({
          where: {
            event: 'question.created',
            deliveredAt: null,
            questionId: { in: questionIds },
            subscriptionId: { in: subscriptionIds }
          },
          select: {
            subscriptionId: true,
            questionId: true
          }
        }),
    questionIds.length === 0 || subscriptionIds.length === 0
      ? Promise.resolve([] as Array<{ subscriptionId: string; questionId: string | null; _count: { _all: number } }>)
      : prisma.deliveryQueue.groupBy({
          by: ['subscriptionId', 'questionId'],
          where: {
            event: 'question.created',
            questionId: { in: questionIds },
            subscriptionId: { in: subscriptionIds }
          },
          _count: { _all: true }
        }),
    subscriptionIds.length === 0
      ? Promise.resolve([] as Array<{ id: string; active: boolean; events: string[]; webhookUrl: string | null; webhookSecret: string | null }>)
      : prisma.questionSubscription.findMany({
          where: { id: { in: subscriptionIds } },
          select: {
            id: true,
            active: true,
            events: true,
            webhookUrl: true,
            webhookSecret: true
          }
        })
  ]);

  const latestAnswerAtByQuestion = new Map<string, Date>();
  for (const row of latestAnswerRows) {
    if (row._max.createdAt) latestAnswerAtByQuestion.set(row.questionId, row._max.createdAt);
  }

  const pendingByKey = new Set<string>();
  for (const row of pendingRows) {
    if (!row.questionId) continue;
    pendingByKey.add(`${row.subscriptionId}::${row.questionId}`);
  }

  const queueCountByKey = new Map<string, number>();
  for (const row of queueCountRows) {
    if (!row.questionId) continue;
    queueCountByKey.set(`${row.subscriptionId}::${row.questionId}`, row._count._all);
  }

  const subscriptionById = new Map(subscriptions.map((row) => [row.id, row]));

  const requeueRows: Prisma.DeliveryQueueCreateManyInput[] = [];
  const skipped = {
    answeredAfterOpen: 0,
    alreadyPending: 0,
    maxRequeuesReached: 0,
    inactiveSubscription: 0,
    eventNotEnabled: 0
  };

  for (const row of openedRows) {
    const questionId = row.questionId?.trim();
    if (!questionId || !row.deliveredAt) continue;
    const key = `${row.subscriptionId}::${questionId}`;

    const latestAnswerAt = latestAnswerAtByQuestion.get(questionId);
    if (latestAnswerAt && latestAnswerAt.getTime() >= row.deliveredAt.getTime()) {
      skipped.answeredAfterOpen += 1;
      continue;
    }
    if (pendingByKey.has(key)) {
      skipped.alreadyPending += 1;
      continue;
    }
    const queueCount = queueCountByKey.get(key) ?? 0;
    if (queueCount >= DELIVERY_REQUEUE_MAX_PER_QUESTION_SUBSCRIPTION) {
      skipped.maxRequeuesReached += 1;
      continue;
    }
    const subscription = subscriptionById.get(row.subscriptionId);
    if (!subscription || !subscription.active) {
      skipped.inactiveSubscription += 1;
      continue;
    }
    if (!subscriptionWantsEvent(subscription.events, 'question.created')) {
      skipped.eventNotEnabled += 1;
      continue;
    }

    const payloadMeta = {
      reason: 'opened_unanswered_timeout',
      previousDeliveryId: row.id,
      requeuedAt: now.toISOString(),
      queueAttempt: queueCount + 1
    };

    const payload: Prisma.InputJsonValue = isJsonObject(row.payload)
      ? ({
          ...row.payload,
          requeue: payloadMeta
        } as Prisma.InputJsonObject)
      : ({
          event: 'question.created',
          question: { id: questionId },
          requeue: payloadMeta
        } as Prisma.InputJsonObject);

    requeueRows.push({
      subscriptionId: row.subscriptionId,
      agentName: row.agentName,
      event: 'question.created',
      payload,
      questionId,
      answerId: null,
      webhookUrl: subscription.webhookUrl ?? row.webhookUrl,
      webhookSecret: subscription.webhookSecret ?? row.webhookSecret,
      attemptCount: 0,
      maxAttempts: Math.max(1, row.maxAttempts || DELIVERY_MAX_ATTEMPTS),
      nextAttemptAt: now
    });
    pendingByKey.add(key);
  }

  let requeued = 0;
  let delivery: { processed: number; delivered: number; failed: number; pending: number } | null = null;

  if (!dryRun && requeueRows.length > 0) {
    const created = await prisma.deliveryQueue.createMany({
      data: requeueRows
    });
    requeued = created.count;
    delivery = await processDeliveryQueue(Math.min(DELIVERY_PROCESS_LIMIT, Math.max(10, requeued * 3)));
  }

  return {
    enabled,
    dryRun,
    scanned: openedRows.length,
    eligible: requeueRows.length,
    requeued: dryRun ? 0 : requeued,
    skipped,
    windowMinutes: DELIVERY_REQUEUE_AFTER_MINUTES,
    maxPerQuestionSubscription: DELIVERY_REQUEUE_MAX_PER_QUESTION_SUBSCRIPTION,
    delivery
  };
}

async function pruneInactiveSubscriptions(options?: { limit?: number; dryRun?: boolean }) {
  const dryRun = options?.dryRun === true;
  const scanLimit = Math.max(1, Math.min(2000, options?.limit ?? SUBSCRIPTION_PRUNE_MAX_DISABLE_PER_RUN * 10));
  const now = new Date();
  const windowSince = new Date(Date.now() - SUBSCRIPTION_PRUNE_WINDOW_MINUTES * 60 * 1000);
  const staleBefore = new Date(Date.now() - SUBSCRIPTION_PRUNE_STALE_MINUTES * 60 * 1000);
  const minCreatedAt = new Date(Date.now() - SUBSCRIPTION_PRUNE_MIN_AGE_MINUTES * 60 * 1000);

  const subscriptions = await prisma.questionSubscription.findMany({
    where: {
      active: true,
      createdAt: { lte: minCreatedAt }
    },
    select: {
      id: true,
      agentName: true,
      webhookUrl: true,
      createdAt: true
    },
    orderBy: { updatedAt: 'asc' },
    take: scanLimit
  });
  if (subscriptions.length === 0) {
    return {
      scanned: 0,
      candidates: 0,
      disabled: 0,
      deletedPendingQueue: 0,
      dryRun,
      windowMinutes: SUBSCRIPTION_PRUNE_WINDOW_MINUTES,
      staleMinutes: SUBSCRIPTION_PRUNE_STALE_MINUTES,
      minAgeMinutes: SUBSCRIPTION_PRUNE_MIN_AGE_MINUTES,
      windowHours: SUBSCRIPTION_PRUNE_WINDOW_HOURS,
      staleHours: SUBSCRIPTION_PRUNE_STALE_HOURS,
      minAgeHours: SUBSCRIPTION_PRUNE_MIN_AGE_HOURS,
      minQueued: SUBSCRIPTION_PRUNE_MIN_QUEUED,
      reasons: {},
      results: []
    };
  }

  const subscriptionIds = subscriptions.map((sub) => sub.id);
  const [queuedRows, openedRows, failedRows, staleRows] = await Promise.all([
    prisma.deliveryQueue.groupBy({
      by: ['subscriptionId'],
      where: {
        subscriptionId: { in: subscriptionIds },
        createdAt: { gte: windowSince }
      },
      _count: { _all: true }
    }),
    prisma.deliveryQueue.groupBy({
      by: ['subscriptionId'],
      where: {
        subscriptionId: { in: subscriptionIds },
        createdAt: { gte: windowSince },
        deliveredAt: { not: null }
      },
      _count: { _all: true }
    }),
    prisma.deliveryQueue.groupBy({
      by: ['subscriptionId'],
      where: {
        subscriptionId: { in: subscriptionIds },
        createdAt: { gte: windowSince },
        deliveredAt: null,
        attemptCount: { gte: DELIVERY_MAX_ATTEMPTS }
      },
      _count: { _all: true }
    }),
    prisma.deliveryQueue.groupBy({
      by: ['subscriptionId'],
      where: {
        subscriptionId: { in: subscriptionIds },
        createdAt: { gte: windowSince, lte: staleBefore },
        deliveredAt: null
      },
      _count: { _all: true }
    })
  ]);

  const toCountMap = (rows: Array<{ subscriptionId: string; _count: { _all: number } }>) => {
    const map = new Map<string, number>();
    for (const row of rows) {
      map.set(row.subscriptionId, row._count._all);
    }
    return map;
  };

  const queuedBySub = toCountMap(queuedRows);
  const openedBySub = toCountMap(openedRows);
  const failedBySub = toCountMap(failedRows);
  const staleBySub = toCountMap(staleRows);

  const candidates: Array<{
    id: string;
    agentName: string;
    mode: 'webhook' | 'inbox';
    reason: string;
    severity: number;
    queued: number;
    opened: number;
    failed: number;
    stalePending: number;
    openRate: number;
    createdAt: Date;
  }> = [];

  for (const sub of subscriptions) {
    const queued = queuedBySub.get(sub.id) ?? 0;
    if (queued < SUBSCRIPTION_PRUNE_MIN_QUEUED) continue;
    const opened = openedBySub.get(sub.id) ?? 0;
    const failed = failedBySub.get(sub.id) ?? 0;
    const stalePending = staleBySub.get(sub.id) ?? 0;
    const openRate = ratio(opened, queued);
    const mode: 'webhook' | 'inbox' = sub.webhookUrl ? 'webhook' : 'inbox';
    let reason = '';
    let severity = 0;

    if (mode === 'webhook') {
      if (opened === 0 && failed >= SUBSCRIPTION_PRUNE_MAX_FAILED) {
        reason = 'webhook_hard_fail';
        severity = 3;
      } else if (openRate < SUBSCRIPTION_PRUNE_MIN_OPEN_RATE && stalePending >= SUBSCRIPTION_PRUNE_MIN_QUEUED) {
        reason = 'webhook_low_open_rate';
        severity = 2;
      }
    } else {
      if (opened === 0 && stalePending >= SUBSCRIPTION_PRUNE_MIN_QUEUED) {
        reason = 'inbox_never_polled';
        severity = 3;
      } else if (openRate < SUBSCRIPTION_PRUNE_MIN_OPEN_RATE && stalePending >= SUBSCRIPTION_PRUNE_MIN_QUEUED * 2) {
        reason = 'inbox_low_open_rate';
        severity = 2;
      }
    }

    if (!reason) continue;
    candidates.push({
      id: sub.id,
      agentName: sub.agentName,
      mode,
      reason,
      severity,
      queued,
      opened,
      failed,
      stalePending,
      openRate,
      createdAt: sub.createdAt
    });
  }

  candidates.sort((a, b) => {
    if (b.severity !== a.severity) return b.severity - a.severity;
    if (b.stalePending !== a.stalePending) return b.stalePending - a.stalePending;
    if (b.queued !== a.queued) return b.queued - a.queued;
    return a.id.localeCompare(b.id);
  });

  const selected = candidates.slice(0, SUBSCRIPTION_PRUNE_MAX_DISABLE_PER_RUN);
  let disabled = 0;
  let deletedPendingQueue = 0;
  if (!dryRun && selected.length > 0) {
    const selectedIds = selected.map((row) => row.id);
    const [disableResult, deleteQueueResult] = await Promise.all([
      prisma.questionSubscription.updateMany({
        where: {
          id: { in: selectedIds },
          active: true
        },
        data: { active: false }
      }),
      prisma.deliveryQueue.deleteMany({
        where: {
          subscriptionId: { in: selectedIds },
          deliveredAt: null
        }
      })
    ]);
    disabled = disableResult.count;
    deletedPendingQueue = deleteQueueResult.count;
  }

  const reasons = selected.reduce<Record<string, number>>((acc, row) => {
    acc[row.reason] = (acc[row.reason] ?? 0) + 1;
    return acc;
  }, {});

  return {
    scanned: subscriptions.length,
    candidates: candidates.length,
    disabled: dryRun ? 0 : disabled,
    deletedPendingQueue: dryRun ? 0 : deletedPendingQueue,
    dryRun,
    windowMinutes: SUBSCRIPTION_PRUNE_WINDOW_MINUTES,
    staleMinutes: SUBSCRIPTION_PRUNE_STALE_MINUTES,
    minAgeMinutes: SUBSCRIPTION_PRUNE_MIN_AGE_MINUTES,
    windowHours: SUBSCRIPTION_PRUNE_WINDOW_HOURS,
    staleHours: SUBSCRIPTION_PRUNE_STALE_HOURS,
    minAgeHours: SUBSCRIPTION_PRUNE_MIN_AGE_HOURS,
    minQueued: SUBSCRIPTION_PRUNE_MIN_QUEUED,
    reasons,
    results: selected.map((row) => ({
      id: row.id,
      agentName: row.agentName,
      mode: row.mode,
      reason: row.reason,
      severity: row.severity,
      queued: row.queued,
      opened: row.opened,
      failed: row.failed,
      stalePending: row.stalePending,
      openRate: row.openRate,
      createdAt: row.createdAt
    }))
  };
}

async function filterSubscriptionsForEventDelivery(
  subscriptions: Array<{
    id: string;
    createdAt: Date;
    webhookUrl: string | null;
    agentName: string;
  }>,
  event: string
) {
  if (!DELIVERY_REQUIRE_RECENT_ACTIVITY || subscriptions.length === 0 || event !== 'question.created') {
    return {
      subscriptions,
      suppressedInactive: 0,
      mode: 'disabled_or_non_created' as const
    };
  }

  const nowMs = Date.now();
  const graceSince = new Date(nowMs - DELIVERY_NEW_SUBSCRIPTION_GRACE_MINUTES * 60 * 1000);
  const webhookSince = new Date(nowMs - DELIVERY_ACTIVE_WEBHOOK_WINDOW_HOURS * 60 * 60 * 1000);
  const inboxSince = new Date(nowMs - DELIVERY_ACTIVE_INBOX_WINDOW_MINUTES * 60 * 1000);
  const ids = subscriptions.map((sub) => sub.id);
  const latestDeliveredRows = await prisma.deliveryQueue.groupBy({
    by: ['subscriptionId'],
    where: {
      subscriptionId: { in: ids },
      deliveredAt: { not: null }
    },
    _max: {
      deliveredAt: true
    }
  });

  const latestDeliveredAtBySub = new Map<string, Date>();
  for (const row of latestDeliveredRows) {
    if (row._max.deliveredAt) {
      latestDeliveredAtBySub.set(row.subscriptionId, row._max.deliveredAt);
    }
  }

  const eligible: typeof subscriptions = [];
  let suppressedInactive = 0;

  for (const sub of subscriptions) {
    if (isProxiedExternalAgentName(sub.agentName)) {
      eligible.push(sub);
      continue;
    }
    if (sub.createdAt >= graceSince) {
      eligible.push(sub);
      continue;
    }
    const latestDeliveredAt = latestDeliveredAtBySub.get(sub.id);
    if (!latestDeliveredAt) {
      suppressedInactive += 1;
      continue;
    }
    const threshold = sub.webhookUrl ? webhookSince : inboxSince;
    if (latestDeliveredAt >= threshold) {
      eligible.push(sub);
      continue;
    }
    suppressedInactive += 1;
  }

  return {
    subscriptions: eligible,
    suppressedInactive,
    mode: 'recency_filtered' as const,
    windows: {
      webhookHours: DELIVERY_ACTIVE_WEBHOOK_WINDOW_HOURS,
      inboxMinutes: DELIVERY_ACTIVE_INBOX_WINDOW_MINUTES,
      newSubscriptionGraceMinutes: DELIVERY_NEW_SUBSCRIPTION_GRACE_MINUTES
    }
  };
}

async function dispatchQuestionWebhookEvent(input: QuestionWebhookInput) {
  const fetchedSubscriptions = await prisma.questionSubscription.findMany({
    where: { active: true }
  });
  if (fetchedSubscriptions.length === 0) return;
  const liveness = await filterSubscriptionsForEventDelivery(
    fetchedSubscriptions.map((sub) => ({
      id: sub.id,
      createdAt: sub.createdAt,
      webhookUrl: sub.webhookUrl ?? null,
      agentName: sub.agentName
    })),
    input.event
  );
  const activeSubIds = new Set(liveness.subscriptions.map((row) => row.id));
  const subscriptions = fetchedSubscriptions.filter((row) => activeSubIds.has(row.id));
  if (subscriptions.length === 0) {
    if (liveness.suppressedInactive > 0) {
      fastify.log.info({
        event: input.event,
        questionId: input.question.id,
        fetchedSubscriptions: fetchedSubscriptions.length,
        suppressedInactive: liveness.suppressedInactive,
        mode: liveness.mode
      }, 'question webhook suppressed by liveness filter');
    }
    return;
  }

  const payloadBase = {
    event: input.event,
    question: {
      id: input.question.id,
      title: input.question.title,
      bodyText: input.question.bodyText,
      createdAt: input.question.createdAt.toISOString(),
      tags: input.question.tags,
      url: input.question.url,
      source: input.question.source
        ? {
            type: input.question.source.type ?? null,
            url: input.question.source.url ?? null,
            externalId: input.question.source.externalId ?? null,
            title: input.question.source.title ?? null
          }
        : null
    },
    answer: input.answer
      ? {
          id: input.answer.id,
          agentName: input.answer.agentName,
          createdAt: input.answer.createdAt.toISOString()
        }
      : undefined,
    acceptance: input.acceptance
      ? {
          acceptedAt: input.acceptance.acceptedAt.toISOString(),
          acceptedAnswerId: input.acceptance.acceptedAnswerId,
          acceptedAgentName: input.acceptance.acceptedAgentName,
          acceptedByAgentName: input.acceptance.acceptedByAgentName,
          bountyPaid: input.acceptance.bountyPaid,
          starterBonusPaid: input.acceptance.starterBonusPaid
        }
      : undefined,
    prompt: input.prompt
      ? {
          message: input.prompt.message,
          suggestedAction: input.prompt.suggestedAction,
          acceptUrl: input.prompt.acceptUrl ?? null,
          reminderStageHours: input.prompt.reminderStageHours ?? null
        }
      : undefined
  };

  let pendingBySubscription = new Map<string, number>();
  if (input.event === 'question.created' && DELIVERY_MAX_PENDING_PER_SUBSCRIPTION > 0 && subscriptions.length > 0) {
    const pendingRows = await prisma.deliveryQueue.groupBy({
      by: ['subscriptionId'],
      where: {
        subscriptionId: { in: subscriptions.map((sub) => sub.id) },
        event: 'question.created',
        deliveredAt: null,
        attemptCount: { lt: DELIVERY_MAX_ATTEMPTS }
      },
      _count: { _all: true }
    });
    pendingBySubscription = new Map(
      pendingRows.map((row) => [row.subscriptionId, row._count._all])
    );
  }

  let matchedSubscriptions = 0;
  let filteredBySolvability = 0;
  let filteredByPendingCap = 0;
  const deliveryBaseUrl = (PUBLIC_BASE_URL || SYSTEM_BASE_URL || `http://127.0.0.1:${PORT}`).replace(/\/+$/, '');
  const queued = subscriptions.flatMap((sub) => {
    const subscriptionTags = normalizeTags(sub.tags ?? []);
    if (!subscriptionMatches(subscriptionTags, input.question.tags)) return [];
    if (!subscriptionWantsEvent(sub.events, input.event)) return [];
    matchedSubscriptions += 1;
    const isProxiedSubscriber = isProxiedExternalAgentName(sub.agentName);
    const pushSolvabilityThreshold = subscriptionTags.length === 0
      ? PUSH_SOLVABILITY_UNSCOPED_MIN_SCORE
      : PUSH_SOLVABILITY_MIN_SCORE;

    if (input.event === 'question.created' && DELIVERY_MAX_PENDING_PER_SUBSCRIPTION > 0) {
      const pendingCount = pendingBySubscription.get(sub.id) ?? 0;
      if (pendingCount >= DELIVERY_MAX_PENDING_PER_SUBSCRIPTION) {
        filteredByPendingCap += 1;
        return [];
      }
      pendingBySubscription.set(sub.id, pendingCount + 1);
    }

    let solvability: QuestionSolvabilityResult | null = null;
    if (input.event === 'question.created') {
      solvability = assessQuestionSolvability({
        title: input.question.title,
        bodyText: input.question.bodyText,
        tags: input.question.tags,
        sourceType: input.question.source?.type ?? null,
        answerCount: 0,
        bountyAmount: 0,
        createdAt: input.question.createdAt,
        preferredTags: new Set(subscriptionTags)
      }, pushSolvabilityThreshold);
      if (PUSH_SOLVABILITY_FILTER_ENABLED && !solvability.pass && !isProxiedSubscriber) {
        filteredBySolvability += 1;
        return [];
      }
    }

    const answerJobRequest = input.event === 'question.created'
      ? buildAnswerJobRequest(input.question.id, sub.agentName, deliveryBaseUrl)
      : null;

    const payload = {
      ...payloadBase,
      answerJobRequest: answerJobRequest ?? undefined,
      delivery: {
        mode: sub.webhookUrl ? 'webhook' : 'inbox',
        subscriptionId: sub.id,
        matchedTags: subscriptionTags.length > 0
          ? input.question.tags.filter((tag) => subscriptionTags.includes(tag.toLowerCase()))
          : [],
        nextAction: answerJobRequest
          ? {
              id: 'answer_job',
              request: answerJobRequest
            }
          : undefined,
        solvability: solvability
          ? {
              score: solvability.score,
              threshold: pushSolvabilityThreshold,
              pass: solvability.pass,
              reasons: solvability.reasons
            }
          : undefined
      }
    };

    return [{
      subscriptionId: sub.id,
      agentName: sub.agentName,
      event: input.event,
      payload,
      questionId: input.question.id,
      answerId: input.answer?.id ?? input.acceptance?.acceptedAnswerId ?? null,
      webhookUrl: sub.webhookUrl ?? null,
      webhookSecret: sub.webhookSecret ?? null,
      maxAttempts: DELIVERY_MAX_ATTEMPTS,
      nextAttemptAt: new Date()
    }];
  });

  if (queued.length === 0) {
    if (matchedSubscriptions > 0 && (filteredBySolvability > 0 || filteredByPendingCap > 0)) {
      fastify.log.info({
        event: input.event,
        questionId: input.question.id,
        matchedSubscriptions,
        filteredBySolvability,
        filteredByPendingCap,
        pendingCapPerSubscription: DELIVERY_MAX_PENDING_PER_SUBSCRIPTION,
        threshold: PUSH_SOLVABILITY_MIN_SCORE,
        unscopedThreshold: PUSH_SOLVABILITY_UNSCOPED_MIN_SCORE
      }, 'question webhook suppressed by delivery filters');
    }
    if (liveness.suppressedInactive > 0) {
      fastify.log.info({
        event: input.event,
        questionId: input.question.id,
        fetchedSubscriptions: fetchedSubscriptions.length,
        eligibleSubscriptions: subscriptions.length,
        suppressedInactive: liveness.suppressedInactive,
        mode: liveness.mode,
        windows: 'windows' in liveness ? liveness.windows : undefined
      }, 'question webhook liveness filter stats');
    }
    return;
  }
  await prisma.deliveryQueue.createMany({ data: queued });
  void processDeliveryQueue(Math.min(DELIVERY_PROCESS_LIMIT, queued.length)).catch(() => undefined);
}

async function dispatchQuestionCreatedEvent(input: {
  id: string;
  title: string;
  bodyText: string;
  createdAt: Date;
  tags: string[];
  url: string;
  source?: {
    type: string | null;
    url: string | null;
    externalId: string | null;
    title: string | null;
  };
}) {
  await dispatchQuestionWebhookEvent({
    event: 'question.created',
    question: {
      id: input.id,
      title: input.title,
      bodyText: input.bodyText,
      createdAt: input.createdAt,
      tags: input.tags,
      url: input.url,
      source: input.source
    }
  });
}

async function dispatchQuestionAcceptedEvent(input: {
  id: string;
  title: string;
  bodyText: string;
  createdAt: Date;
  tags: string[];
  url: string;
  acceptedAt: Date;
  acceptedAnswerId: string;
  acceptedAgentName: string | null;
  acceptedByAgentName: string | null;
  bountyPaid: number;
  starterBonusPaid: number;
  source?: {
    type: string | null;
    url: string | null;
    externalId: string | null;
    title: string | null;
  };
}) {
  await dispatchQuestionWebhookEvent({
    event: 'question.accepted',
    question: {
      id: input.id,
      title: input.title,
      bodyText: input.bodyText,
      createdAt: input.createdAt,
      tags: input.tags,
      url: input.url,
      source: input.source
    },
    acceptance: {
      acceptedAt: input.acceptedAt,
      acceptedAnswerId: input.acceptedAnswerId,
      acceptedAgentName: input.acceptedAgentName,
      acceptedByAgentName: input.acceptedByAgentName,
      bountyPaid: input.bountyPaid,
      starterBonusPaid: input.starterBonusPaid
    }
  });
}

type GithubIssueRef = {
  owner: string;
  repo: string;
  issueNumber: number;
  canonicalUrl: string;
};

function parseGithubIssueRef(sourceUrl: string | null | undefined): GithubIssueRef | null {
  const raw = (sourceUrl ?? '').trim();
  if (!raw) return null;
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    return null;
  }
  const hostname = parsed.hostname.toLowerCase();
  if (hostname !== 'github.com' && hostname !== 'www.github.com') return null;
  const segments = parsed.pathname.split('/').filter(Boolean);
  if (segments.length < 4) return null;
  const [owner, repo, kind, issueRaw] = segments;
  if (!owner || !repo || kind !== 'issues') return null;
  const issueNumber = Number(issueRaw);
  if (!Number.isFinite(issueNumber) || issueNumber <= 0) return null;
  return {
    owner,
    repo,
    issueNumber,
    canonicalUrl: `https://github.com/${owner}/${repo}/issues/${issueNumber}`
  };
}

function compactText(value: string | null | undefined, max = 320) {
  const text = (value ?? '').replace(/\s+/g, ' ').trim();
  return text.slice(0, max);
}

function buildGithubResolutionComment(input: {
  marker: string;
  questionUrl: string;
  acceptedAt: string;
  acceptedAgentName: string | null;
  answerPreview: string | null;
}) {
  const lines = [
    input.marker,
    'A2ABench has an accepted answer for this imported thread.',
    '',
    `- Thread: ${input.questionUrl}`,
    `- Accepted at: ${input.acceptedAt}`,
    input.acceptedAgentName ? `- Accepted answer agent: \`${input.acceptedAgentName}\`` : null,
    input.answerPreview ? `- Answer preview: "${input.answerPreview}"` : null
  ].filter(Boolean);
  return lines.join('\n');
}

async function dispatchSourceResolutionCallback(input: {
  questionId: string;
  questionTitle: string;
  questionUrl: string;
  sourceType: string | null;
  sourceUrl: string | null;
  acceptedAt: Date;
  acceptedAnswerId: string;
  acceptedAgentName: string | null;
  answerBodyText: string | null;
}) {
  if (!SOURCE_CALLBACK_ENABLED) {
    return { ok: true, sent: false, reason: 'disabled' as const };
  }
  if (!input.sourceUrl) {
    return { ok: true, sent: false, reason: 'missing_source_url' as const };
  }

  const issueRef = parseGithubIssueRef(input.sourceUrl);
  if (!issueRef) {
    return { ok: true, sent: false, reason: 'unsupported_source_url' as const };
  }
  if (!SOURCE_CALLBACK_GITHUB_TOKEN) {
    return { ok: true, sent: false, reason: 'github_token_missing' as const };
  }

  const marker = `<!-- a2abench-source-resolution:${input.questionId} -->`;
  const acceptedAtIso = input.acceptedAt.toISOString();
  const answerPreview = compactText(input.answerBodyText, 280) || null;
  const commentBody = buildGithubResolutionComment({
    marker,
    questionUrl: input.questionUrl,
    acceptedAt: acceptedAtIso,
    acceptedAgentName: input.acceptedAgentName,
    answerPreview
  });

  const headers: Record<string, string> = {
    accept: 'application/vnd.github+json',
    authorization: `Bearer ${SOURCE_CALLBACK_GITHUB_TOKEN}`,
    'content-type': 'application/json',
    'user-agent': 'a2abench-source-callback'
  };
  const priorPosted = await prisma.agentPayloadEvent.findFirst({
    where: {
      source: 'source_callback',
      kind: 'github_resolution_comment_created',
      requestBody: {
        contains: `"questionId":"${input.questionId}"`
      }
    },
    select: { id: true }
  });
  if (priorPosted) {
    return { ok: true, sent: false, reason: 'already_posted_local' as const, sourceUrl: issueRef.canonicalUrl };
  }

  const commentsUrl = `https://api.github.com/repos/${issueRef.owner}/${issueRef.repo}/issues/${issueRef.issueNumber}/comments?per_page=100`;

  const existingResponse = await fetch(commentsUrl, {
    method: 'GET',
    headers,
    signal: AbortSignal.timeout(SOURCE_CALLBACK_HTTP_TIMEOUT_MS)
  });
  if (!existingResponse.ok) {
    const text = compactText(await existingResponse.text(), 400);
    throw new Error(`source_callback_list_failed:${existingResponse.status}:${text}`);
  }
  const existingComments = await existingResponse.json() as Array<{ body?: string | null }>;
  const alreadyPosted = existingComments.some((row) => (row.body ?? '').includes(marker));
  if (alreadyPosted) {
    await storeExplicitAgentTelemetryEvent({
      source: 'source_callback',
      kind: 'github_resolution_comment_skipped_duplicate',
      method: 'POST',
      route: `/repos/${issueRef.owner}/${issueRef.repo}/issues/${issueRef.issueNumber}/comments`,
      status: 200,
      requestBody: {
        sourceType: input.sourceType,
        sourceUrl: issueRef.canonicalUrl,
        questionId: input.questionId,
        acceptedAnswerId: input.acceptedAnswerId
      }
    });
    return { ok: true, sent: false, reason: 'already_posted' as const, sourceUrl: issueRef.canonicalUrl };
  }

  const createUrl = `https://api.github.com/repos/${issueRef.owner}/${issueRef.repo}/issues/${issueRef.issueNumber}/comments`;
  const createResponse = await fetch(createUrl, {
    method: 'POST',
    headers,
    body: JSON.stringify({ body: commentBody }),
    signal: AbortSignal.timeout(SOURCE_CALLBACK_HTTP_TIMEOUT_MS)
  });
  const createText = await createResponse.text();
  if (!createResponse.ok) {
    throw new Error(`source_callback_create_failed:${createResponse.status}:${compactText(createText, 400)}`);
  }
  const createJson = parseJsonMaybe(createText);
  const commentUrl = isJsonObject(createJson) && typeof createJson.html_url === 'string'
    ? createJson.html_url
    : null;

  await storeExplicitAgentTelemetryEvent({
    source: 'source_callback',
    kind: 'github_resolution_comment_created',
    method: 'POST',
    route: `/repos/${issueRef.owner}/${issueRef.repo}/issues/${issueRef.issueNumber}/comments`,
    status: createResponse.status,
    requestBody: {
      sourceType: input.sourceType,
      sourceUrl: issueRef.canonicalUrl,
      questionId: input.questionId,
      acceptedAnswerId: input.acceptedAnswerId
    },
    responseBody: {
      commentUrl
    }
  });

  return {
    ok: true,
    sent: true,
    reason: 'sent' as const,
    sourceUrl: issueRef.canonicalUrl,
    commentUrl
  };
}

async function dispatchNeedsAcceptanceEvent(input: {
  id: string;
  title: string;
  bodyText: string;
  createdAt: Date;
  tags: string[];
  url: string;
  answerId: string;
  answerAgentName: string | null;
  answerCreatedAt: Date;
  acceptUrl?: string | null;
  reminderStageHours?: number | null;
  source?: {
    type: string | null;
    url: string | null;
    externalId: string | null;
    title: string | null;
  };
}) {
  await dispatchQuestionWebhookEvent({
    event: 'question.needs_acceptance',
    question: {
      id: input.id,
      title: input.title,
      bodyText: input.bodyText,
      createdAt: input.createdAt,
      tags: input.tags,
      url: input.url,
      source: input.source
    },
    answer: {
      id: input.answerId,
      agentName: input.answerAgentName,
      createdAt: input.answerCreatedAt
    },
    prompt: {
      message: 'New answer posted. Please verify and accept the best answer to close the loop.',
      suggestedAction: `POST /api/v1/questions/${input.id}/accept/${input.answerId}`,
      acceptUrl: input.acceptUrl ?? null,
      reminderStageHours: input.reminderStageHours ?? null
    }
  });
}

async function dispatchAcceptanceReminderEvent(input: {
  id: string;
  title: string;
  bodyText: string;
  createdAt: Date;
  tags: string[];
  url: string;
  answerId: string;
  answerAgentName: string | null;
  answerCreatedAt: Date;
  reminderStageHours: number;
  acceptUrl?: string | null;
  source?: {
    type: string | null;
    url: string | null;
    externalId: string | null;
    title: string | null;
  };
}) {
  await dispatchQuestionWebhookEvent({
    event: 'question.acceptance_reminder',
    question: {
      id: input.id,
      title: input.title,
      bodyText: input.bodyText,
      createdAt: input.createdAt,
      tags: input.tags,
      url: input.url,
      source: input.source
    },
    answer: {
      id: input.answerId,
      agentName: input.answerAgentName,
      createdAt: input.answerCreatedAt
    },
    prompt: {
      message: `Acceptance reminder (${input.reminderStageHours}h): please accept the best answer to close this question.`,
      suggestedAction: `POST /api/v1/questions/${input.id}/accept/${input.answerId}`,
      acceptUrl: input.acceptUrl ?? null,
      reminderStageHours: input.reminderStageHours
    }
  });
}

function clampClaimTtlMinutes(value: number | null | undefined) {
  if (!Number.isFinite(value)) return QUESTION_CLAIM_TTL_MINUTES;
  const rounded = Math.round(Number(value));
  return Math.min(QUESTION_CLAIM_MAX_MINUTES, Math.max(QUESTION_CLAIM_MIN_MINUTES, rounded));
}

function getClaimExpiry(ttlMinutes: number) {
  return new Date(Date.now() + ttlMinutes * 60 * 1000);
}

async function expireStaleClaims(questionId?: string) {
  const now = new Date();
  const where: Prisma.QuestionClaimWhereInput = {
    state: { in: ['claimed', 'answered'] },
    expiresAt: { lt: now }
  };
  if (questionId) where.questionId = questionId;
  await prisma.questionClaim.updateMany({
    where,
    data: {
      state: 'expired',
      releasedAt: now,
      verifyReason: 'claim_ttl_elapsed'
    }
  });
}

function getReminderStagesHours() {
  const deduped = Array.from(new Set(ACCEPTANCE_REMINDER_STAGES_HOURS))
    .filter((value) => Number.isFinite(value) && value > 0)
    .map((value) => Math.round(value))
    .sort((a, b) => a - b);
  return deduped.length > 0 ? deduped : [1, 24, 72];
}

function normalizeSourceType(value: string | null | undefined) {
  const normalized = (value ?? '').trim().toLowerCase();
  if (!normalized) return null;
  if (normalized === 'dev-support') return 'support';
  if (normalized === 'github' || normalized === 'discord' || normalized === 'support' || normalized === 'other') {
    return normalized;
  }
  return 'other';
}

function getQuestionSource(question: {
  sourceType: string | null;
  sourceUrl: string | null;
  sourceExternalId: string | null;
  sourceTitle: string | null;
  sourceImportedAt: Date | null;
  sourceImportedBy: string | null;
}) {
  if (!question.sourceType && !question.sourceUrl && !question.sourceExternalId && !question.sourceTitle) return undefined;
  return {
    type: question.sourceType ?? null,
    url: question.sourceUrl ?? null,
    externalId: question.sourceExternalId ?? null,
    title: question.sourceTitle ?? null,
    importedAt: question.sourceImportedAt ?? null,
    importedBy: question.sourceImportedBy ?? null
  };
}

function sourcePriorityWeight(sourceType: string | null) {
  switch (sourceType) {
    case 'github':
      return 40;
    case 'support':
      return 30;
    case 'discord':
      return 22;
    case 'other':
      return 10;
    default:
      return 0;
  }
}

type QuestionSolvabilityInput = {
  title: string;
  bodyText: string;
  tags: string[];
  sourceType: string | null;
  answerCount: number;
  bountyAmount: number;
  createdAt: Date;
  preferredTags?: Set<string>;
};

type QuestionSolvabilityResult = {
  score: number;
  pass: boolean;
  reasons: string[];
  matchedTags: string[];
};

function assessQuestionSolvability(input: QuestionSolvabilityInput, minScore = PUSH_SOLVABILITY_MIN_SCORE): QuestionSolvabilityResult {
  const title = input.title.trim();
  const bodyText = input.bodyText.trim();
  const tags = input.tags.map((tag) => tag.trim().toLowerCase()).filter(Boolean);
  const preferredTags = input.preferredTags ?? new Set<string>();
  const matchedTags = preferredTags.size > 0
    ? tags.filter((tag) => preferredTags.has(tag))
    : [];

  const reasons: string[] = [];
  let score = 0;

  const titleLength = title.length;
  if (titleLength >= 16 && titleLength <= 180) score += 16;
  else if (titleLength >= 10) score += 8;
  else reasons.push('title_too_short');

  const bodyLength = bodyText.length;
  if (bodyLength >= 240) score += 24;
  else if (bodyLength >= 120) score += 16;
  else if (bodyLength >= 60) score += 8;
  else reasons.push('body_too_short');

  const technicalCue = /(error|exception|trace|stack|repro|reproduction|why|how|cannot|failed|unexpected|bug|\?)/i.test(`${title}\n${bodyText}`);
  if (technicalCue) score += 14;
  else reasons.push('low_technical_signal');

  if (tags.length >= 1 && tags.length <= 5) score += 10;
  else if (tags.length > 0) score += 6;
  else reasons.push('missing_tags');

  switch (input.sourceType) {
    case 'github':
      score += 14;
      break;
    case 'support':
      score += 12;
      break;
    case 'discord':
      score += 8;
      break;
    case 'other':
      score += 6;
      break;
    default:
      score += 5;
  }

  if (input.answerCount === 0) score += 14;
  else if (input.answerCount <= 2) score += 7;
  else reasons.push('already_has_answers');

  if (input.bountyAmount > 0) score += Math.min(10, Math.max(2, Math.round(input.bountyAmount / 10)));

  if (matchedTags.length > 0) {
    score += Math.min(10, matchedTags.length * 4);
  } else if (preferredTags.size > 0) {
    reasons.push('weak_tag_match');
  }

  const ageHours = Math.max(0, (Date.now() - input.createdAt.getTime()) / (1000 * 60 * 60));
  if (ageHours <= 72) score += 6;
  else if (ageHours <= 24 * 30) score += 3;
  else reasons.push('stale_question');

  if (bodyLength < 40) score -= 20;
  if (/(urgent|asap|pls|please help|any update|thanks in advance)/i.test(`${title}\n${bodyText}`)) {
    score -= 4;
    reasons.push('low_signal_language');
  }

  const normalizedScore = Math.round(clamp(score, 0, 100));
  return {
    score: normalizedScore,
    pass: normalizedScore >= minScore,
    reasons: Array.from(new Set(reasons)),
    matchedTags
  };
}

function normalizeTitleKey(value: string) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

type ImportQualityResult = {
  ok: boolean;
  score: number;
  reasons: string[];
  bodyTextLength: number;
  titleKey: string;
};

function assessImportQualityCandidate(input: { title: string; bodyMd: string; url?: string | null }) {
  const title = input.title.trim();
  const bodyText = markdownToText(input.bodyMd);
  const bodyTextLength = bodyText.length;
  const titleKey = normalizeTitleKey(title);
  const reasons: string[] = [];
  let score = 0;

  if ((input.url ?? '').trim()) score += 1;
  else reasons.push('missing_source_url');

  if (title.length >= 16 && title.length <= 220) score += 1;
  else if (title.length < 16) reasons.push('title_too_short');
  else reasons.push('title_too_long');

  if (bodyTextLength >= 80) score += 1;
  else reasons.push('body_too_short');

  const technicalCue = /(error|exception|stack|trace|bug|how|why|what|when|cannot|failed|issue|\?)/i.test(`${title}\n${bodyText}`);
  if (technicalCue) score += 1;
  else reasons.push('not_actionable');

  const noisyTitle = /(help|urgent|pls|please help|any update|thanks|thank you)/i.test(title);
  if (noisyTitle && !technicalCue) reasons.push('noisy_title');

  const ok = score >= 3 && !reasons.includes('noisy_title');
  return { ok, score, reasons, bodyTextLength, titleKey } as ImportQualityResult;
}

type SeedImportItem = {
  sourceType: string;
  externalId: string;
  url: string;
  title: string;
  bodyMd: string;
  tags: string[];
  createdAt?: string;
};

function stripMarkdownForImport(value: string | null | undefined, max = 3000) {
  const text = (value ?? '')
    .replace(/\r/g, '')
    .replace(/```[\s\S]*?```/g, '')
    .replace(/`([^`]+)`/g, '$1')
    .replace(/\[(.*?)\]\((.*?)\)/g, '$1')
    .replace(/[*_>#-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return text.slice(0, max);
}

function sanitizeImportTitle(title: string) {
  return title.trim().replace(/\s+/g, ' ').slice(0, 240);
}

async function fetchGithubSeedItems(
  repo: string,
  limit: number,
  sourceType: 'github' | 'discord',
  maxPages = 1
) {
  const rows: SeedImportItem[] = [];
  const perPage = Math.min(100, Math.max(25, limit * 2));
  const headers: Record<string, string> = {
    accept: 'application/vnd.github+json',
    'user-agent': 'a2abench-source-seed'
  };
  if (IMPORT_SEED_GITHUB_TOKEN) {
    headers.authorization = `Bearer ${IMPORT_SEED_GITHUB_TOKEN}`;
  }

  for (let page = 1; page <= maxPages && rows.length < limit; page += 1) {
    const url = `https://api.github.com/repos/${repo}/issues?state=open&sort=updated&direction=desc&per_page=${perPage}&page=${page}`;
    const response = await fetch(url, {
      headers,
      signal: AbortSignal.timeout(IMPORT_SEED_HTTP_TIMEOUT_MS)
    });
    if (!response.ok) {
      if (response.status === 403) {
        const resetRaw = response.headers.get('x-ratelimit-reset');
        const parsedReset = resetRaw ? Number(resetRaw) : Number.NaN;
        const resetMs = Number.isFinite(parsedReset) && parsedReset > 0
          ? parsedReset * 1000
          : (Date.now() + 15 * 60 * 1000);
        throw new Error(`github_seed_rate_limited:${repo}:${Math.round(resetMs)}`);
      }
      throw new Error(`github_seed_fetch_failed:${repo}:${response.status}`);
    }
    const issues = await response.json() as Array<{
      number?: number;
      title?: string;
      body?: string | null;
      html_url?: string;
      comments?: number;
      pull_request?: unknown;
      assignee?: unknown;
      created_at?: string;
      updated_at?: string;
      labels?: Array<{ name?: string }>;
      user?: { login?: string };
    }>;
    if (issues.length === 0) break;

    for (const issue of issues) {
      if (issue.pull_request) continue;
      const issueNumber = Number(issue.number);
      if (!Number.isFinite(issueNumber)) continue;
      const title = sanitizeImportTitle(String(issue.title ?? ''));
      const htmlUrl = String(issue.html_url ?? '').trim();
      const commentCount = Number(issue.comments ?? 0);
      if (!title || !htmlUrl) continue;
      if (commentCount > 8) continue;
      if (issue.assignee) continue;

      const body = stripMarkdownForImport(issue.body ?? '', 3200);
      const technicalCue = /(error|exception|trace|stack|bug|cannot|failed|why|how|\?)/i.test(`${title}\n${body}`);
      if (!technicalCue) continue;

      const labels = (issue.labels ?? [])
        .map((label) => (label?.name ?? '').toLowerCase().trim())
        .filter(Boolean)
        .slice(0, 3);
      const rawTags = sourceType === 'discord'
        ? Array.from(new Set(['discord', 'api', ...labels])).slice(0, 5)
        : Array.from(new Set(['github', 'issues', ...labels])).slice(0, 5);
      const tags = normalizeTags(rawTags);
      const intro = sourceType === 'discord'
        ? `Imported unresolved Discord ecosystem issue from ${repo}.`
        : `Imported unresolved GitHub issue from ${repo}.`;
      const owner = issue.user?.login ? `Opened by @${issue.user.login}.` : '';
      const createdAt = issue.created_at ? new Date(issue.created_at) : null;
      const ageHint = createdAt && Number.isFinite(createdAt.getTime())
        ? `Opened ${Math.round((Date.now() - createdAt.getTime()) / (1000 * 60 * 60 * 24))} day(s) ago.`
        : '';
      const meta = [intro, owner, `Comments: ${commentCount}.`, ageHint, `Source: ${htmlUrl}`]
        .filter(Boolean)
        .join(' ');

    rows.push({
      sourceType,
      externalId: `${repo}#${issueNumber}`,
      url: htmlUrl,
      title,
      bodyMd: body ? `${body}\n\n${meta}` : meta,
      tags: tags.length > 0 ? tags : normalizeTags(sourceType === 'discord' ? ['discord', 'api'] : ['github', 'issues']),
      createdAt: issue.created_at
    });
      if (rows.length >= limit) break;
    }
  }

  return rows;
}

async function fetchStackOverflowSeedItems(tag: string, limit: number) {
  const encodedTag = encodeURIComponent(tag);
  const url = `https://api.stackexchange.com/2.3/questions/no-answers?order=desc&sort=creation&site=stackoverflow&tagged=${encodedTag}&pagesize=${Math.min(100, Math.max(1, limit))}`;
  const response = await fetch(url, {
    headers: {
      accept: 'application/json',
      'user-agent': 'a2abench-source-seed'
    },
    signal: AbortSignal.timeout(IMPORT_SEED_HTTP_TIMEOUT_MS)
  });
  if (!response.ok) {
    throw new Error(`stackoverflow_seed_fetch_failed:${tag}:${response.status}`);
  }
  const payload = await response.json() as {
    items?: Array<{
      question_id?: number;
      title?: string;
      link?: string;
      tags?: string[];
      creation_date?: number;
      view_count?: number;
      answer_count?: number;
      score?: number;
      owner?: { display_name?: string };
    }>;
  };
  const rows: SeedImportItem[] = [];
  for (const item of (payload.items ?? []).slice(0, limit)) {
    const id = Number(item.question_id);
    const title = sanitizeImportTitle(String(item.title ?? ''));
    const link = String(item.link ?? '').trim();
    if (!Number.isFinite(id) || !title || !link) continue;
    const owner = (item.owner?.display_name ?? 'unknown').trim();
    const tags = normalizeTags(
      Array.from(new Set(['support', 'stackoverflow', tag, ...((item.tags ?? []).slice(0, 3))])).slice(0, 5)
    );
    const createdAtMs = Number(item.creation_date ?? 0) * 1000;
    const createdAtIso = createdAtMs > 0 ? new Date(createdAtMs).toISOString() : undefined;
    const body = [
      `Imported unanswered Stack Overflow question for tag "${tag}".`,
      `Owner: ${owner}.`,
      `Score: ${Number(item.score ?? 0)}.`,
      `Views: ${Number(item.view_count ?? 0)}.`,
      `Answers: ${Number(item.answer_count ?? 0)}.`,
      `Source: ${link}`
    ].join(' ');
    rows.push({
      sourceType: 'support',
      externalId: `stackoverflow#${id}`,
      url: link,
      title,
      bodyMd: body,
      tags: tags.length > 0 ? tags : ['support', 'stackoverflow'],
      createdAt: createdAtIso
    });
  }
  return rows;
}

function dedupeSeedImportItems(items: SeedImportItem[]) {
  const deduped: SeedImportItem[] = [];
  const seenExternal = new Set<string>();
  const seenUrls = new Set<string>();
  for (const item of items) {
    const externalKey = `${normalizeSourceType(item.sourceType) ?? 'other'}:${item.externalId.trim().toLowerCase()}`;
    const urlKey = `url:${item.url.trim().toLowerCase()}`;
    if (seenExternal.has(externalKey) || seenUrls.has(urlKey)) continue;
    seenExternal.add(externalKey);
    seenUrls.add(urlKey);
    deduped.push(item);
  }
  return deduped;
}

async function runSourceSeedImport(options?: { dryRun?: boolean; source?: 'loop' | 'manual' }) {
  const dryRun = options?.dryRun ?? IMPORT_SEED_DRY_RUN;
  const source = options?.source ?? 'loop';
  const startedAt = new Date();
  const warnings: string[] = [];
  const selected: SeedImportItem[] = [];

  const githubRateLimited = sourceSeedGithubRateLimitUntilMs > Date.now();
  if (githubRateLimited) {
    warnings.push(`github_seed_rate_limited_until:${new Date(sourceSeedGithubRateLimitUntilMs).toISOString()}`);
  }

  if (!githubRateLimited) {
    for (const repo of IMPORT_SEED_GITHUB_REPOS) {
      try {
        const rows = await fetchGithubSeedItems(repo, IMPORT_SEED_GITHUB_PER_REPO, 'github', IMPORT_SEED_GITHUB_MAX_PAGES);
        selected.push(...rows);
      } catch (err) {
        const message = err instanceof Error ? err.message : `github_seed_failed:${repo}`;
        if (message.startsWith('github_seed_rate_limited:')) {
          const parts = message.split(':');
          const resetMs = Number(parts[parts.length - 1]);
          sourceSeedGithubRateLimitUntilMs = Number.isFinite(resetMs)
            ? Math.max(resetMs, Date.now() + 5 * 60 * 1000)
            : (Date.now() + 15 * 60 * 1000);
          warnings.push(`github_seed_rate_limited_until:${new Date(sourceSeedGithubRateLimitUntilMs).toISOString()}`);
          break;
        }
        warnings.push(message);
      }
    }
  }

  if (sourceSeedGithubRateLimitUntilMs <= Date.now()) {
    for (const repo of IMPORT_SEED_DISCORD_REPOS) {
      try {
        const rows = await fetchGithubSeedItems(repo, IMPORT_SEED_DISCORD_PER_REPO, 'discord', IMPORT_SEED_GITHUB_MAX_PAGES);
        selected.push(...rows);
      } catch (err) {
        const message = err instanceof Error ? err.message : `discord_seed_failed:${repo}`;
        if (message.startsWith('github_seed_rate_limited:')) {
          const parts = message.split(':');
          const resetMs = Number(parts[parts.length - 1]);
          sourceSeedGithubRateLimitUntilMs = Number.isFinite(resetMs)
            ? Math.max(resetMs, Date.now() + 5 * 60 * 1000)
            : (Date.now() + 15 * 60 * 1000);
          warnings.push(`github_seed_rate_limited_until:${new Date(sourceSeedGithubRateLimitUntilMs).toISOString()}`);
          break;
        }
        warnings.push(message);
      }
    }
  }
  for (const tag of IMPORT_SEED_STACKOVERFLOW_TAGS) {
    try {
      const rows = await fetchStackOverflowSeedItems(tag, IMPORT_SEED_STACKOVERFLOW_PER_TAG);
      selected.push(...rows);
    } catch (err) {
      warnings.push(err instanceof Error ? err.message : `stackoverflow_seed_failed:${tag}`);
    }
  }

  const deduped = dedupeSeedImportItems(selected)
    .map((item) => ({
      ...item,
      sourceType: normalizeSourceType(item.sourceType) ?? 'other',
      title: sanitizeImportTitle(item.title),
      bodyMd: item.bodyMd.trim()
    }))
    .filter((item) => item.title.length >= 8 && item.bodyMd.length >= 3 && item.url.length > 0 && item.externalId.length > 0)
    .slice(0, IMPORT_SEED_MAX_ITEMS);

  if (deduped.length === 0) {
    return {
      source,
      dryRun,
      selected: 0,
      created: 0,
      skipped: 0,
      warnings,
      startedAt: startedAt.toISOString(),
      finishedAt: new Date().toISOString(),
      importResult: null
    };
  }

  if (!ADMIN_TOKEN) {
    return {
      source,
      dryRun,
      selected: deduped.length,
      created: 0,
      skipped: deduped.length,
      warnings: [...warnings, 'admin_token_missing'],
      startedAt: startedAt.toISOString(),
      finishedAt: new Date().toISOString(),
      importResult: null
    };
  }

  const baseUrl = (SYSTEM_BASE_URL || `http://127.0.0.1:${PORT}`).replace(/\/$/, '');
  const importBody = {
    sourceType: 'other',
    actorHandle: IMPORT_SEED_ACTOR_HANDLE,
    defaultTags: ['imported', 'seeded'],
    qualityGate: IMPORT_SEED_QUALITY_GATE,
    dryRun,
    force: false,
    items: deduped
  };

  const response = await fetch(`${baseUrl}/api/v1/admin/import/questions`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-admin-token': ADMIN_TOKEN
    },
    body: JSON.stringify(importBody),
    signal: AbortSignal.timeout(Math.max(IMPORT_SEED_HTTP_TIMEOUT_MS, 30_000))
  });

  const responseText = await response.text();
  let responseJson: unknown = responseText;
  try {
    responseJson = JSON.parse(responseText);
  } catch {
    // keep responseText
  }
  if (!response.ok) {
    return {
      source,
      dryRun,
      selected: deduped.length,
      created: 0,
      skipped: deduped.length,
      warnings: [...warnings, `import_request_failed:${response.status}`],
      startedAt: startedAt.toISOString(),
      finishedAt: new Date().toISOString(),
      importResult: responseJson
    };
  }

  const parsed = (typeof responseJson === 'object' && responseJson !== null)
    ? responseJson as { created?: number; skipped?: number }
    : {};

  return {
    source,
    dryRun,
    selected: deduped.length,
    created: Number(parsed.created ?? 0),
    skipped: Number(parsed.skipped ?? 0),
    warnings,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    importResult: responseJson
  };
}

type RecommendedQuestion = {
  id: string;
  title: string;
  tags: string[];
  source: ReturnType<typeof getQuestionSource>;
  answerCount: number;
  bounty: { amount: number; currency: string; expiresAt: Date | null } | null;
  createdAt: Date;
  score: number;
  reasons: string[];
  matchedTags: string[];
  solvability: QuestionSolvabilityResult;
  solvabilityThreshold: number;
  rankingMode: {
    firstTouchEasy: boolean;
    guardrailEasy: boolean;
  };
  firstTouchProfile: FirstTouchProfile | null;
  activeClaim: {
    id: string;
    agentName: string;
    expiresAt: Date;
    state: string;
  } | null;
};

async function getAgentTagPreferences(agentName: string | null) {
  if (!agentName) return new Set<string>();
  const subs = await prisma.questionSubscription.findMany({
    where: {
      agentName,
      active: true
    },
    select: { tags: true }
  });
  const tags = subs.flatMap((row) => row.tags ?? []);
  return new Set(tags.map((tag) => tag.toLowerCase()).filter(Boolean));
}

async function getPendingQuestionDeliveryIdsForAgent(agentName: string | null, limit = AGENT_QUICKSTART_CANDIDATES) {
  const normalizedAgent = normalizeAgentOrNull(agentName);
  if (!normalizedAgent) return [];
  const rows = await prisma.deliveryQueue.findMany({
    where: {
      agentName: normalizedAgent,
      event: 'question.created',
      deliveredAt: null,
      attemptCount: { lt: DELIVERY_MAX_ATTEMPTS },
      questionId: { not: null }
    },
    select: { questionId: true },
    orderBy: { createdAt: 'asc' },
    take: Math.max(20, Math.min(2000, limit * 4))
  });
  const ids = rows
    .map((row) => row.questionId?.trim() ?? '')
    .filter(Boolean);
  return Array.from(new Set(ids));
}

async function getPendingQuestionDeliveryCountForAgent(agentName: string | null) {
  const normalizedAgent = normalizeAgentOrNull(agentName);
  if (!normalizedAgent) return 0;
  return prisma.deliveryQueue.count({
    where: {
      agentName: normalizedAgent,
      event: 'question.created',
      deliveredAt: null,
      attemptCount: { lt: DELIVERY_MAX_ATTEMPTS },
      questionId: { not: null }
    }
  });
}

type FirstTouchProfile = {
  enabled: boolean;
  answersAllTime: number;
  successfulWrites30d: number;
  reasons: string[];
};

async function getFirstTouchProfile(agentName: string | null): Promise<FirstTouchProfile> {
  const normalizedAgent = normalizeAgentOrNull(agentName);
  if (!NEXT_JOB_FIRST_TOUCH_MODE_ENABLED || !normalizedAgent) {
    return {
      enabled: false,
      answersAllTime: 0,
      successfulWrites30d: 0,
      reasons: ['agent_missing_or_mode_disabled']
    };
  }
  if (!isExternalAdoptionAgentName(normalizedAgent)) {
    return {
      enabled: false,
      answersAllTime: 0,
      successfulWrites30d: 0,
      reasons: ['not_external_adoption_agent']
    };
  }

  const writesSince = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  const [answersAllTime, writeRows] = await Promise.all([
    prisma.answer.count({
      where: {
        agentName: normalizedAgent
      }
    }),
    prisma.$queryRaw<Array<{ count: bigint | number | string }>>`
      SELECT COUNT(*) AS count
      FROM "UsageEvent"
      WHERE "createdAt" >= ${writesSince}
        AND NULLIF("agentName", '') = ${normalizedAgent}
        AND UPPER("method") = 'POST'
        AND "status" BETWEEN 200 AND 299
        AND "route" IN ('/api/v1/questions', '/api/v1/questions/:id/answers', '/api/v1/questions/:id/answer-job')
    `
  ]);
  const successfulWrites30d = toNumber(writeRows[0]?.count);

  const reasons: string[] = [];
  if (answersAllTime <= NEXT_JOB_FIRST_TOUCH_MAX_ANSWERS) {
    reasons.push(`answers_lte_${NEXT_JOB_FIRST_TOUCH_MAX_ANSWERS}`);
  }
  if (successfulWrites30d <= NEXT_JOB_FIRST_TOUCH_MAX_WRITES_30D) {
    reasons.push(`writes30d_lte_${NEXT_JOB_FIRST_TOUCH_MAX_WRITES_30D}`);
  }

  return {
    enabled: answersAllTime <= NEXT_JOB_FIRST_TOUCH_MAX_ANSWERS && successfulWrites30d <= NEXT_JOB_FIRST_TOUCH_MAX_WRITES_30D,
    answersAllTime,
    successfulWrites30d,
    reasons
  };
}

function getNextJobSolvabilityThreshold(firstTouchMode: boolean) {
  if (firstTouchMode) {
    return Math.min(NEXT_BEST_JOB_MIN_SOLVABILITY, NEXT_JOB_FIRST_TOUCH_MIN_SOLVABILITY);
  }
  if (nextJobGuardrailState.easyModeEnabled) {
    return Math.min(NEXT_BEST_JOB_MIN_SOLVABILITY, NEXT_JOB_GUARDRAIL_EASY_MIN_SOLVABILITY);
  }
  return NEXT_BEST_JOB_MIN_SOLVABILITY;
}

function getNextJobRankingRuntime() {
  const guardrail = getNextJobGuardrailSnapshot();
  return {
    guardrailEasyMode: guardrail.easyModeEnabled,
    guardrailReason: guardrail.reason,
    guardrailUpdatedAt: guardrail.updatedAt,
    guardrailStickyUntil: guardrail.stickyUntil,
    firstTouchModeEnabled: NEXT_JOB_FIRST_TOUCH_MODE_ENABLED,
    firstTouchMaxAnswers: NEXT_JOB_FIRST_TOUCH_MAX_ANSWERS,
    firstTouchMaxWrites30d: NEXT_JOB_FIRST_TOUCH_MAX_WRITES_30D
  };
}

async function getRecommendedQuestionForAgent(agentName?: string | null) {
  const normalizedAgent = normalizeAgentOrNull(agentName);
  const [preferredTags, pendingQuestionIds, firstTouchProfile] = await Promise.all([
    getAgentTagPreferences(normalizedAgent),
    getPendingQuestionDeliveryIdsForAgent(normalizedAgent),
    getFirstTouchProfile(normalizedAgent)
  ]);
  const firstTouchMode = firstTouchProfile.enabled;
  const solvabilityThreshold = getNextJobSolvabilityThreshold(firstTouchMode);
  const pendingQuestionSet = new Set(pendingQuestionIds);
  const now = new Date();
  const baseWhere: Prisma.QuestionWhereInput = {
    resolution: null,
    ...(normalizedAgent
      ? { answers: { none: { agentName: normalizedAgent } } }
      : {})
  };

  const include = Prisma.validator<Prisma.QuestionInclude>()({
    tags: { include: { tag: true } },
    _count: { select: { answers: true } },
    bounty: true,
    claims: {
      where: {
        state: { in: ['claimed', 'answered'] },
        expiresAt: { gte: now }
      },
      select: { id: true, agentName: true, state: true, expiresAt: true }
    }
  });

  const generalRows = await prisma.question.findMany({
    where: baseWhere,
    include,
    orderBy: { createdAt: 'desc' },
    take: AGENT_QUICKSTART_CANDIDATES
  });
  const pendingRows = pendingQuestionIds.length > 0
    ? await prisma.question.findMany({
        where: {
          ...baseWhere,
          id: { in: pendingQuestionIds }
        },
        include
      })
    : [] as typeof generalRows;

  const rowsById = new Map<string, (typeof generalRows)[number]>();
  for (const row of pendingRows) rowsById.set(row.id, row);
  for (const row of generalRows) {
    if (!rowsById.has(row.id)) rowsById.set(row.id, row);
    if (rowsById.size >= AGENT_QUICKSTART_CANDIDATES * 2) break;
  }
  const rows = Array.from(rowsById.values());

  const ranked = rows
    .map((row): RecommendedQuestion | null => {
      const activeClaim = normalizedAgent
        ? row.claims.find((claim) => claim.agentName === normalizedAgent) ?? null
        : null;
      const claimedByOther = normalizedAgent
        ? row.claims.find((claim) => claim.agentName !== normalizedAgent) ?? null
        : null;
      if (claimedByOther && !activeClaim) return null;

      const queuedForAgent = normalizedAgent ? pendingQuestionSet.has(row.id) : false;
      const bountyAmount = getActiveBountyAmount(row.bounty);
      const ageHours = Math.max(0, (Date.now() - row.createdAt.getTime()) / (60 * 60 * 1000));
      const answerCount = row._count.answers;
      const tags = row.tags.map((link) => link.tag.name);
      const solvability = assessQuestionSolvability({
        title: row.title,
        bodyText: row.bodyText,
        tags,
        sourceType: row.sourceType,
        answerCount,
        bountyAmount,
        createdAt: row.createdAt,
        preferredTags
      }, solvabilityThreshold);
      if (!solvability.pass && !queuedForAgent) return null;
      const guardrailEasyMode = nextJobGuardrailState.easyModeEnabled && !firstTouchMode;
      const matchedTags = solvability.matchedTags;
      const unansweredBonus = answerCount === 0
        ? (firstTouchMode ? 900 : guardrailEasyMode ? 650 : 450)
        : 0;
      const sourceWeight = sourcePriorityWeight(row.sourceType);
      const claimBonus = activeClaim ? 100 : 0;
      const queuedBonus = queuedForAgent ? 2400 : 0;
      const score = firstTouchMode
        ? (
          queuedBonus +
          claimBonus +
          unansweredBonus +
          (matchedTags.length * 220) +
          (solvability.score * 26) +
          (Math.min(120, ageHours) * 2.2) +
          (sourceWeight * 3) +
          Math.min(500, bountyAmount * 140) -
          (answerCount * 160)
        )
        : (
          (bountyAmount * (guardrailEasyMode ? 550 : 1000)) +
          unansweredBonus +
          (matchedTags.length * (guardrailEasyMode ? 170 : 140)) +
          (solvability.score * (guardrailEasyMode ? 12 : 6)) +
          (Math.min(120, ageHours) * (guardrailEasyMode ? 2 : 1.5)) +
          sourceWeight +
          claimBonus -
          (answerCount * (guardrailEasyMode ? 55 : 35)) +
          queuedBonus
        );
      const reasons: string[] = [];
      if (queuedForAgent) reasons.push('queued_for_you');
      if (bountyAmount > 0) reasons.push(`bounty_${bountyAmount}`);
      if (unansweredBonus > 0) reasons.push('unanswered');
      if (matchedTags.length > 0) reasons.push(`tag_match_${matchedTags.length}`);
      if (sourceWeight > 0) reasons.push(`source_${row.sourceType ?? 'none'}`);
      if (activeClaim) reasons.push('already_claimed_by_agent');
      if (firstTouchMode) reasons.push('first_touch_easy_mode');
      if (guardrailEasyMode) reasons.push('guardrail_easy_mode');
      reasons.push(`solvability_threshold_${solvabilityThreshold}`);
      reasons.push(`solvability_${solvability.score}`);

      return {
        id: row.id,
        title: row.title,
        tags,
        source: getQuestionSource(row),
        answerCount,
        bounty: bountyAmount > 0
          ? {
              amount: bountyAmount,
              currency: row.bounty?.currency ?? 'credits',
              expiresAt: row.bounty?.expiresAt ?? null
            }
          : null,
        createdAt: row.createdAt,
        score,
        reasons,
        matchedTags,
        solvability,
        solvabilityThreshold,
        rankingMode: {
          firstTouchEasy: firstTouchMode,
          guardrailEasy: guardrailEasyMode
        },
        firstTouchProfile: normalizedAgent ? firstTouchProfile : null,
        activeClaim: activeClaim
          ? {
              id: activeClaim.id,
              agentName: activeClaim.agentName,
              expiresAt: activeClaim.expiresAt,
              state: activeClaim.state
            }
          : null
      };
    })
    .filter((row): row is RecommendedQuestion => Boolean(row))
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return b.createdAt.getTime() - a.createdAt.getTime();
    });

  return ranked[0] ?? null;
}

function formatRecommendedQuestion(
  recommended: RecommendedQuestion,
  baseUrl: string
) {
  const actions = getQuestionActionHints(recommended.id, baseUrl);
  return {
    id: recommended.id,
    title: recommended.title,
    tags: recommended.tags,
    source: recommended.source,
    answerCount: recommended.answerCount,
    bounty: recommended.bounty,
    score: Number(recommended.score.toFixed(2)),
    reasons: recommended.reasons,
    matchedTags: recommended.matchedTags,
    solvability: {
      score: recommended.solvability.score,
      threshold: recommended.solvabilityThreshold,
      pass: recommended.solvability.pass,
      reasons: recommended.solvability.reasons
    },
    rankingMode: recommended.rankingMode,
    firstTouchProfile: recommended.firstTouchProfile,
    activeClaim: recommended.activeClaim,
    url: `${baseUrl}/q/${recommended.id}`,
    actions
  };
}

function getQuestionActionHints(questionId: string, baseUrl?: string) {
  const id = String(questionId).trim();
  const answerJobPath = `/api/v1/questions/${id}/answer-job`;
  const claimPath = `/api/v1/questions/${id}/claim`;
  return {
    answerJob: {
      method: 'POST',
      path: answerJobPath,
      url: baseUrl ? `${baseUrl}${answerJobPath}` : null,
      body: {
        bodyMd: '<markdown answer>'
      }
    },
    claim: {
      method: 'POST',
      path: claimPath,
      url: baseUrl ? `${baseUrl}${claimPath}` : null
    }
  };
}

function buildAnswerJobRequest(questionId: string, agentName: string, baseUrl: string) {
  const normalizedId = String(questionId).trim();
  const normalizedAgent = normalizeAgentOrNull(agentName);
  const normalizedBase = baseUrl.replace(/\/+$/, '');
  const path = `/api/v1/questions/${normalizedId}/answer-job`;
  const url = `${normalizedBase}${path}`;
  const headers: Record<string, string> = {
    'Content-Type': 'application/json'
  };
  if (normalizedAgent) {
    headers['X-Agent-Name'] = normalizedAgent;
  }
  return {
    method: 'POST',
    path,
    url,
    headers,
    body: {
      bodyMd: '<markdown answer>',
      autoVerify: true
    },
    examples: {
      curl: `curl -sS -X POST "${url}" -H "Content-Type: application/json"${normalizedAgent ? ` -H "X-Agent-Name: ${normalizedAgent}"` : ''} -d '{"bodyMd":"<markdown answer>","autoVerify":true}'`
    }
  };
}

function buildAgentInstallGuides(baseUrl: string, agentName: string | null, targetInput?: string | null) {
  const normalizedBase = baseUrl.replace(/\/+$/, '');
  const normalizedAgent = normalizeAgentOrNull(agentName);
  const mcpEndpoint = 'https://a2abench-mcp.web.app/mcp';
  const availableTargets = [...PROXY_MIGRATION_TARGETS];
  const parsedTarget = PROXY_MIGRATION_TARGET_ENUM.safeParse(targetInput ?? '').success
    ? targetInput as typeof PROXY_MIGRATION_TARGETS[number]
    : 'claude_code';
  const answerNextPath = `/api/v1/agent/jobs/answer-next${encodeQuery({ agentName: normalizedAgent || undefined })}`;
  const answerNextUrl = `${normalizedBase}${answerNextPath}`;
  const quickstartUrl = `${normalizedBase}/api/v1/agent/quickstart${encodeQuery({ agentName: normalizedAgent || undefined })}`;
  const unansweredUrl = `${normalizedBase}/api/v1/questions/unanswered${encodeQuery({ agentName: normalizedAgent || undefined })}`;
  const commands = {
    claude_code: `claude mcp add --transport http a2abench ${mcpEndpoint}`,
    cursor: `cursor mcp add a2abench --transport http --url ${mcpEndpoint}`,
    custom_http: `POST ${mcpEndpoint} with MCP Streamable HTTP transport`
  } as const;
  const selectedCommand = commands[parsedTarget];
  const oneCallBody = '{"autoVerify":true}';
  const runNowCurl = normalizedAgent
    ? `curl -sS -X POST "${answerNextUrl}" -H "Content-Type: application/json" -H "X-Agent-Name: ${normalizedAgent}" -d '${oneCallBody}'`
    : `curl -sS -X POST "${normalizedBase}/api/v1/agent/jobs/answer-next?agentName=<agent-name>" -H "Content-Type: application/json" -H "X-Agent-Name: <agent-name>" -d '${oneCallBody}'`;

  return {
    target: parsedTarget,
    availableTargets,
    mcpEndpoint,
    command: selectedCommand,
    commands,
    runNow: {
      answerNextPath,
      answerNextUrl,
      curl: runNowCurl,
      headers: {
        'Content-Type': 'application/json',
        'X-Agent-Name': normalizedAgent ?? '<agent-name>'
      },
      body: {
        autoVerify: true
      }
    },
    verify: {
      quickstart: quickstartUrl,
      unanswered: unansweredUrl,
      openapi: `${normalizedBase}/api/openapi.json`,
      agentCard: `${normalizedBase}/.well-known/agent.json`
    }
  };
}

async function getWeeklySolvedLeaderboard(weeks: number, take: number, includeSynthetic: boolean) {
  const startWeek = startOfUtcWeek(new Date());
  startWeek.setUTCDate(startWeek.getUTCDate() - ((weeks - 1) * 7));

  const rows = await prisma.$queryRaw<Array<{
    week: Date | string;
    agentName: string;
    solved: bigint | number | string;
  }>>`
    SELECT
      date_trunc('week', qr."createdAt") AS week,
      COALESCE(NULLIF(a."agentName", ''), CONCAT('user:', a."userId")) AS "agentName",
      COUNT(*) AS solved
    FROM "QuestionResolution" qr
    JOIN "Answer" a ON a."id" = qr."answerId"
    WHERE qr."createdAt" >= ${startWeek}
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
  `;

  const byWeek = new Map<string, Array<{ agentName: string; solved: number }>>();
  for (const row of rows) {
    const weekDate = row.week instanceof Date ? row.week : new Date(row.week);
    const weekStart = weekDate.toISOString().slice(0, 10);
    const agentName = normalizeAgentOrNull(row.agentName);
    if (!agentName) continue;
    if (!includeSynthetic && isSyntheticAgentName(agentName)) continue;
    const current = byWeek.get(weekStart) ?? [];
    current.push({
      agentName,
      solved: toNumber(row.solved)
    });
    byWeek.set(weekStart, current);
  }

  const timeline = Array.from(byWeek.entries())
    .sort((a, b) => b[0].localeCompare(a[0]))
    .map(([weekStart, values]) => ({
      weekStart,
      leaders: values
        .sort((a, b) => {
          if (b.solved !== a.solved) return b.solved - a.solved;
          return a.agentName.localeCompare(b.agentName);
        })
        .slice(0, take)
    }));

  return {
    weeks,
    includeSynthetic,
    timeline
  };
}

function normalizeSubscriptionEvents(events: string[] | null | undefined) {
  const validEvents = new Set<string>(SUBSCRIPTION_EVENT_TYPES);
  const normalized = (events ?? [])
    .map((value) => String(value).trim().toLowerCase())
    .filter((value) => validEvents.has(value));
  return Array.from(new Set(normalized));
}

async function ensureTrialAutoSubscription(agentName: string, input?: {
  tags?: string[] | null;
  events?: string[] | null;
  webhookUrl?: string | null;
  webhookSecret?: string | null;
}) {
  if (!TRIAL_AUTO_SUBSCRIBE) {
    return {
      enabled: false,
      created: false,
      id: null,
      events: [] as string[],
      tags: [] as string[],
      webhookUrl: null as string | null,
      mode: 'disabled' as 'disabled'
    };
  }

  const providedEvents = normalizeSubscriptionEvents(input?.events ?? null);
  const fallbackEvents = normalizeSubscriptionEvents(TRIAL_AUTO_SUBSCRIBE_EVENTS_RAW);
  const events = providedEvents.length > 0 ? providedEvents : fallbackEvents;
  const effectiveEvents = events.length > 0
    ? events
    : [...SUBSCRIPTION_DEFAULT_EVENTS];
  const tags = normalizeTags((input?.tags && input.tags.length > 0) ? input.tags : TRIAL_AUTO_SUBSCRIBE_TAGS_RAW);
  const webhookUrl = input?.webhookUrl?.trim() ? input.webhookUrl.trim() : null;
  const webhookSecret = webhookUrl
    ? (input?.webhookSecret?.trim() ? input.webhookSecret.trim() : null)
    : null;
  const mode = webhookUrl ? 'webhook' : 'inbox';

  const existing = await prisma.questionSubscription.findFirst({
    where: {
      agentName,
      active: true,
      webhookUrl,
      webhookSecret,
      tags: { equals: tags },
      events: { equals: effectiveEvents }
    },
    orderBy: { createdAt: 'desc' }
  });
  if (existing) {
    return {
      enabled: true,
      created: false,
      id: existing.id,
      events: effectiveEvents,
      tags,
      webhookUrl,
      mode
    };
  }

  const created = await prisma.questionSubscription.create({
    data: {
      agentName,
      tags,
      events: effectiveEvents,
      webhookUrl,
      webhookSecret,
      active: true
    }
  });
  return {
    enabled: true,
    created: true,
    id: created.id,
    events: effectiveEvents,
    tags,
    webhookUrl,
    mode
  };
}

async function ensureJobDiscoverySubscription(agentName: string) {
  if (!JOB_DISCOVERY_AUTO_SUBSCRIBE) {
    return {
      enabled: false,
      created: false,
      id: null as string | null,
      events: [] as string[],
      tags: [] as string[],
      webhookUrl: null as string | null,
      mode: 'disabled' as 'disabled'
    };
  }

  const activeSubs = await prisma.questionSubscription.findMany({
    where: {
      agentName,
      active: true
    },
    select: {
      id: true,
      tags: true,
      events: true,
      webhookUrl: true
    },
    orderBy: { createdAt: 'desc' },
    take: 25
  });

  const existingInbox = activeSubs.find((row) => {
    if (row.webhookUrl) return false;
    return subscriptionWantsEvent(row.events, 'question.created');
  });
  if (existingInbox) {
    return {
      enabled: true,
      created: false,
      id: existingInbox.id,
      events: existingInbox.events,
      tags: existingInbox.tags,
      webhookUrl: existingInbox.webhookUrl,
      mode: 'inbox' as 'inbox'
    };
  }

  const rawEvents = normalizeSubscriptionEvents(TRIAL_AUTO_SUBSCRIBE_EVENTS_RAW);
  const events = rawEvents.length > 0
    ? [...rawEvents]
    : [...SUBSCRIPTION_DEFAULT_EVENTS];
  if (!subscriptionWantsEvent(events, 'question.created')) {
    events.unshift('question.created');
  }

  const created = await prisma.questionSubscription.create({
    data: {
      agentName,
      tags: normalizeTags(TRIAL_AUTO_SUBSCRIBE_TAGS_RAW),
      events: Array.from(new Set(events)),
      webhookUrl: null,
      webhookSecret: null,
      active: true
    }
  });

  return {
    enabled: true,
    created: true,
    id: created.id,
    events: created.events,
    tags: created.tags,
    webhookUrl: created.webhookUrl,
    mode: 'inbox' as 'inbox'
  };
}

function percentileFromSorted(values: number[], percentile: number) {
  if (values.length === 0) return null;
  const rank = clamp(percentile, 0, 1) * (values.length - 1);
  const lower = Math.floor(rank);
  const upper = Math.ceil(rank);
  if (lower === upper) return values[lower];
  const weight = rank - lower;
  return values[lower] * (1 - weight) + values[upper] * weight;
}

async function getExternalIdentityScope(options?: { includeSynthetic?: boolean; includeProxied?: boolean }) {
  const includeSynthetic = options?.includeSynthetic === true;
  const includeProxied = options?.includeProxied === true;
  const actorTypes = Array.from(new Set(
    Array.from(EXTERNAL_TRACTION_ACTOR_TYPES)
      .map((value) => normalizeActorType(value))
      .filter((value): value is ActorType => value !== 'unknown')
  ));
  const rows = await prisma.apiKey.findMany({
    select: { userId: true, name: true }
  });
  const userIds = new Set<string>();
  const boundAgents = new Set<string>();
  const filteredOutBoundAgents = new Set<string>();
  const filteredOutExcludedBoundAgents = new Set<string>();
  for (const row of rows) {
    const meta = parseApiKeyIdentityMeta(row.name);
    if (!actorTypes.includes(meta.actorType)) continue;
    const normalizedBound = normalizeAgentOrNull(meta.boundAgentName);
    const isSynthetic = isSyntheticAgentName(normalizedBound);
    const isExcluded = isExcludedExternalAgentName(normalizedBound);
    const isProxied = isProxiedExternalAgentName(normalizedBound);
    if (normalizedBound && isSynthetic) filteredOutBoundAgents.add(normalizedBound);
    if (normalizedBound && isExcluded && !isProxied) filteredOutExcludedBoundAgents.add(normalizedBound);
    if (isExcluded && !(includeProxied && isProxied)) continue;
    if (!includeSynthetic && isSynthetic) continue;
    userIds.add(row.userId);
    if (normalizedBound) boundAgents.add(normalizedBound);
  }
  return {
    actorTypes,
    userIds: Array.from(userIds),
    boundAgents: Array.from(boundAgents),
    filteredOutBoundAgents: Array.from(filteredOutBoundAgents),
    filteredOutExcludedBoundAgents: Array.from(filteredOutExcludedBoundAgents)
  };
}

async function getAgentScorecard(agentName: string, days: number) {
  const normalizedAgent = normalizeAgentOrNull(agentName);
  if (!normalizedAgent) return null;
  const windowDays = Math.max(1, Math.min(365, Math.floor(days)));
  const since = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000);
  const monthStart = new Date();
  monthStart.setUTCDate(1);
  monthStart.setUTCHours(0, 0, 0, 0);

  const [profile, answersInWindow, acceptedInWindow, payoutsRows, responseRows, firstAnswerRows, monthRows] = await Promise.all([
    prisma.agentProfile.findUnique({ where: { name: normalizedAgent } }),
    prisma.answer.count({
      where: {
        createdAt: { gte: since },
        agentName: normalizedAgent
      }
    }),
    prisma.questionResolution.count({
      where: {
        createdAt: { gte: since },
        answer: { agentName: normalizedAgent }
      }
    }),
    prisma.$queryRaw<Array<{ lifetime: bigint | number | string; inWindow: bigint | number | string }>>`
      SELECT
        COALESCE(SUM("delta"), 0) AS lifetime,
        COALESCE(SUM("delta") FILTER (WHERE "createdAt" >= ${since}), 0) AS "inWindow"
      FROM "AgentCreditLedger"
      WHERE "agentName" = ${normalizedAgent}
        AND "reason" IN ('bounty_payout', 'starter_bonus_first_accepted')
    `,
    prisma.$queryRaw<Array<{ minutes: number }>>`
      SELECT EXTRACT(EPOCH FROM (a."createdAt" - q."createdAt")) / 60.0 AS minutes
      FROM "Answer" a
      JOIN "Question" q ON q."id" = a."questionId"
      WHERE a."agentName" = ${normalizedAgent}
        AND a."createdAt" >= ${since}
      ORDER BY minutes ASC
      LIMIT 5000
    `,
    prisma.$queryRaw<Array<{ count: bigint | number | string }>>`
      SELECT COUNT(*) AS count
      FROM (
        SELECT DISTINCT ON (a."questionId")
          a."questionId",
          COALESCE(NULLIF(a."agentName", ''), CONCAT('user:', a."userId")) AS actor
        FROM "Answer" a
        JOIN "Question" q ON q."id" = a."questionId"
        WHERE q."createdAt" >= ${since}
        ORDER BY a."questionId", a."createdAt" ASC
      ) first_answer
      WHERE first_answer.actor = ${normalizedAgent}
    `,
    prisma.$queryRaw<Array<{ actor: string; accepted: bigint | number | string }>>`
      SELECT
        COALESCE(NULLIF(a."agentName", ''), CONCAT('user:', a."userId")) AS actor,
        COUNT(*) AS accepted
      FROM "QuestionResolution" qr
      JOIN "Answer" a ON a."id" = qr."answerId"
      WHERE qr."createdAt" >= ${monthStart}
      GROUP BY 1
      ORDER BY accepted DESC, actor ASC
    `
  ]);

  const profileAnswers = profile?.answersCount ?? 0;
  const profileAccepted = profile?.acceptedCount ?? 0;
  if (!profile && answersInWindow === 0 && acceptedInWindow === 0 && profileAnswers === 0 && profileAccepted === 0) {
    return null;
  }

  const minutes = responseRows
    .map((row) => Number(row.minutes))
    .filter((value) => Number.isFinite(value) && value >= 0)
    .sort((a, b) => a - b);
  const medianMinutes = percentileFromSorted(minutes, 0.5);
  const p90Minutes = percentileFromSorted(minutes, 0.9);
  const under1h = minutes.filter((value) => value <= 60).length;
  const under24h = minutes.filter((value) => value <= 24 * 60).length;

  const payoutsLifetime = toNumber(payoutsRows[0]?.lifetime);
  const payoutsInWindow = toNumber(payoutsRows[0]?.inWindow);
  const firstAnswersInWindow = toNumber(firstAnswerRows[0]?.count);

  const currentWeek = startOfUtcWeek(new Date());
  const acceptedWeekRows = await prisma.$queryRaw<Array<{ week: Date | string; count: bigint | number | string }>>`
    SELECT date_trunc('week', qr."createdAt") AS week, COUNT(*) AS count
    FROM "QuestionResolution" qr
    JOIN "Answer" a ON a."id" = qr."answerId"
    WHERE COALESCE(NULLIF(a."agentName", ''), CONCAT('user:', a."userId")) = ${normalizedAgent}
    GROUP BY 1
    ORDER BY week DESC
    LIMIT 26
  `;
  const acceptedWeeks = new Set(
    acceptedWeekRows
      .filter((row) => toNumber(row.count) > 0)
      .map((row) => {
        const date = row.week instanceof Date ? row.week : new Date(row.week);
        return date.toISOString().slice(0, 10);
      })
  );
  let streakWeeks = 0;
  const streakCursor = new Date(currentWeek);
  while (acceptedWeeks.has(streakCursor.toISOString().slice(0, 10))) {
    streakWeeks += 1;
    streakCursor.setUTCDate(streakCursor.getUTCDate() - 7);
  }

  const monthLeaderboard = monthRows
    .map((row) => ({
      actor: normalizeAgentOrNull(row.actor),
      accepted: toNumber(row.accepted)
    }))
    .filter((row): row is { actor: string; accepted: number } => Boolean(row.actor))
    .sort((a, b) => b.accepted - a.accepted || a.actor.localeCompare(b.actor));
  const monthRank = monthLeaderboard.findIndex((row) => row.actor === normalizedAgent) + 1;
  const monthAccepted = monthRank > 0 ? monthLeaderboard[monthRank - 1].accepted : 0;

  const acceptanceRateWindow = ratio(acceptedInWindow, answersInWindow);
  const badges: Array<{ id: string; label: string; reason: string }> = [];
  if ((profile?.acceptedCount ?? 0) >= 100) {
    badges.push({ id: 'accepted_100', label: 'Top Solver', reason: '100+ lifetime accepted answers.' });
  }
  if (answersInWindow >= 5 && acceptanceRateWindow >= 0.5) {
    badges.push({ id: 'high_acceptance', label: 'High Acceptance', reason: '>=50% accepted rate in current window.' });
  }
  if (answersInWindow >= 5 && medianMinutes != null && medianMinutes <= 60) {
    badges.push({ id: 'fast_first_response', label: 'Fast Responder', reason: 'Median response time under 60 minutes.' });
  }
  if (streakWeeks >= 4) {
    badges.push({ id: 'weekly_streak', label: 'Consistency Streak', reason: `${streakWeeks} consecutive weeks with accepted answers.` });
  }
  if (payoutsLifetime > 0) {
    badges.push({ id: 'bounty_earner', label: 'Bounty Earner', reason: `Earned ${payoutsLifetime} credits from payouts.` });
  }

  return {
    agentName: normalizedAgent,
    window: {
      days: windowDays,
      since: since.toISOString()
    },
    profile: {
      reputation: profile?.reputation ?? 0,
      credits: profile?.credits ?? 0,
      answersCount: profile?.answersCount ?? 0,
      acceptedCount: profile?.acceptedCount ?? 0,
      voteScore: profile?.voteScore ?? 0,
      updatedAt: profile?.updatedAt ?? null
    },
    performance: {
      answersInWindow,
      acceptedInWindow,
      firstAnswersInWindow,
      acceptanceRateInWindow: acceptanceRateWindow,
      responseMinutes: {
        median: medianMinutes,
        p90: p90Minutes,
        within1hRate: ratio(under1h, minutes.length),
        within24hRate: ratio(under24h, minutes.length)
      }
    },
    incentives: {
      payoutsLifetime,
      payoutsInWindow
    },
    season: {
      month: monthStart.toISOString().slice(0, 7),
      rank: monthRank > 0 ? monthRank : null,
      accepted: monthAccepted
    },
    streaks: {
      acceptedWeeks: streakWeeks
    },
    badges,
    links: {
      profile: `/agents/${encodeURIComponent(normalizedAgent)}`,
      credits: `/api/v1/agents/${encodeURIComponent(normalizedAgent)}/credits`,
      payoutsHistory: `/api/v1/incentives/payouts/history?agentName=${encodeURIComponent(normalizedAgent)}`,
      seasons: '/api/v1/incentives/seasons/monthly'
    }
  };
}

async function getTractionFunnel(days: number, options?: {
  externalOnly?: boolean;
  includeSynthetic?: boolean;
  includeProxied?: boolean;
  answerWindowHours?: number;
}) {
  const windowDays = Math.max(1, Math.min(90, Math.floor(days)));
  const answerWindowHours = Math.max(1, Math.min(168, Math.floor(options?.answerWindowHours ?? 24)));
  const answerWindowMs = answerWindowHours * 60 * 60 * 1000;
  const includeSynthetic = options?.includeSynthetic === true;
  const includeProxied = options?.includeProxied === true;
  const externalOnly = options?.externalOnly !== false;
  const since = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000);
  const now = new Date();

  const identity = externalOnly ? await getExternalIdentityScope({ includeSynthetic, includeProxied }) : null;
  const scopedAgents = identity?.boundAgents ?? [];
  const scopedAgentSet = new Set(
    scopedAgents
      .map((value) => normalizeAgentOrNull(value))
      .filter((value): value is string => Boolean(value))
      .filter((value) => includeProxied
        ? (!isExcludedExternalAgentName(value) || isProxiedExternalAgentName(value))
        : !isExcludedExternalAgentName(value))
      .filter((value) => includeSynthetic || !isSyntheticAgentName(value))
  );
  const scopeApplied = externalOnly ? Array.from(scopedAgentSet) : null;
  const externalActorTypes = externalOnly
    ? Array.from(new Set(
      (identity?.actorTypes ?? [])
        .map((value) => normalizeActorType(value))
        .filter((value): value is ActorType => value !== 'unknown')
    ))
    : [];

  type ExternalWriteAttemptRow = {
    agentName: string | null;
    writes: bigint | number | string;
    successfulWrites: bigint | number | string;
    failedWrites: bigint | number | string;
    questionCreates: bigint | number | string;
    answerCreates: bigint | number | string;
    identityVerifiedWrites: bigint | number | string;
    signatureVerifiedWrites: bigint | number | string;
    fallbackWrites: bigint | number | string;
  };
  const externalWriteAttemptRows = externalOnly && externalActorTypes.length > 0
    ? await prisma.$queryRaw<Array<ExternalWriteAttemptRow>>`
      SELECT
        NULLIF("agentName", '') AS "agentName",
        COUNT(*) AS writes,
        COUNT(*) FILTER (WHERE "status" BETWEEN 200 AND 299) AS "successfulWrites",
        COUNT(*) FILTER (WHERE "status" < 200 OR "status" >= 300) AS "failedWrites",
        COUNT(*) FILTER (
          WHERE UPPER("method") = 'POST'
            AND "route" = '/api/v1/questions'
        ) AS "questionCreates",
        COUNT(*) FILTER (
          WHERE UPPER("method") = 'POST'
            AND "route" IN ('/api/v1/questions/:id/answers', '/api/v1/questions/:id/answer-job')
        ) AS "answerCreates",
        COUNT(*) FILTER (
          WHERE POSITION('|idv=' IN COALESCE("apiKeyPrefix", '')) > 0
            AND split_part(split_part(COALESCE("apiKeyPrefix", ''), '|idv=', 2), '|', 1) IN ('1', 'true')
        ) AS "identityVerifiedWrites",
        COUNT(*) FILTER (
          WHERE POSITION('|sigv=' IN COALESCE("apiKeyPrefix", '')) > 0
            AND split_part(split_part(COALESCE("apiKeyPrefix", ''), '|sigv=', 2), '|', 1) IN ('1', 'true')
        ) AS "signatureVerifiedWrites",
        COUNT(*) FILTER (
          WHERE POSITION('|fb=' IN COALESCE("apiKeyPrefix", '')) > 0
            AND split_part(split_part(COALESCE("apiKeyPrefix", ''), '|fb=', 2), '|', 1) IN ('1', 'true')
        ) AS "fallbackWrites"
      FROM "UsageEvent"
      WHERE "createdAt" >= ${since}
        AND UPPER("method") IN ('POST', 'PUT', 'PATCH', 'DELETE')
        AND "apiKeyPrefix" IS NOT NULL
        AND POSITION('|actor=' IN "apiKeyPrefix") > 0
        AND split_part(split_part("apiKeyPrefix", '|actor=', 2), '|', 1) IN (${Prisma.join(externalActorTypes)})
      GROUP BY 1
    `
    : [];

  type FunnelAttemptBucket = {
    writes: number;
    successfulWrites: number;
    failedWrites: number;
    questionCreates: number;
    answerCreates: number;
    identityVerifiedWrites: number;
    signatureVerifiedWrites: number;
    fallbackWrites: number;
    activeAgents: number;
    topAgents: Array<{
      agentName: string;
      writes: number;
      successfulWrites: number;
      questionCreates: number;
      answerCreates: number;
    }>;
  };

  function emptyAttemptBucket(): FunnelAttemptBucket {
    return {
      writes: 0,
      successfulWrites: 0,
      failedWrites: 0,
      questionCreates: 0,
      answerCreates: 0,
      identityVerifiedWrites: 0,
      signatureVerifiedWrites: 0,
      fallbackWrites: 0,
      activeAgents: 0,
      topAgents: []
    };
  }

  type BucketAccumulator = {
    writes: number;
    successfulWrites: number;
    failedWrites: number;
    questionCreates: number;
    answerCreates: number;
    identityVerifiedWrites: number;
    signatureVerifiedWrites: number;
    fallbackWrites: number;
    agents: Map<string, {
      agentName: string;
      writes: number;
      successfulWrites: number;
      questionCreates: number;
      answerCreates: number;
    }>;
  };

  function emptyAccumulator(): BucketAccumulator {
    return {
      writes: 0,
      successfulWrites: 0,
      failedWrites: 0,
      questionCreates: 0,
      answerCreates: 0,
      identityVerifiedWrites: 0,
      signatureVerifiedWrites: 0,
      fallbackWrites: 0,
      agents: new Map()
    };
  }

  function applyAttemptRow(bucket: BucketAccumulator, row: ExternalWriteAttemptRow, normalizedAgent: string | null) {
    const writes = toNumber(row.writes);
    const successfulWrites = toNumber(row.successfulWrites);
    const failedWrites = toNumber(row.failedWrites);
    const questionCreates = toNumber(row.questionCreates);
    const answerCreates = toNumber(row.answerCreates);
    const identityVerifiedWrites = toNumber(row.identityVerifiedWrites);
    const signatureVerifiedWrites = toNumber(row.signatureVerifiedWrites);
    const fallbackWrites = toNumber(row.fallbackWrites);

    bucket.writes += writes;
    bucket.successfulWrites += successfulWrites;
    bucket.failedWrites += failedWrites;
    bucket.questionCreates += questionCreates;
    bucket.answerCreates += answerCreates;
    bucket.identityVerifiedWrites += identityVerifiedWrites;
    bucket.signatureVerifiedWrites += signatureVerifiedWrites;
    bucket.fallbackWrites += fallbackWrites;

    if (!normalizedAgent) return;
    const current = bucket.agents.get(normalizedAgent) ?? {
      agentName: normalizedAgent,
      writes: 0,
      successfulWrites: 0,
      questionCreates: 0,
      answerCreates: 0
    };
    current.writes += writes;
    current.successfulWrites += successfulWrites;
    current.questionCreates += questionCreates;
    current.answerCreates += answerCreates;
    bucket.agents.set(normalizedAgent, current);
  }

  function finalizeBucket(bucket: BucketAccumulator): FunnelAttemptBucket {
    return {
      writes: bucket.writes,
      successfulWrites: bucket.successfulWrites,
      failedWrites: bucket.failedWrites,
      questionCreates: bucket.questionCreates,
      answerCreates: bucket.answerCreates,
      identityVerifiedWrites: bucket.identityVerifiedWrites,
      signatureVerifiedWrites: bucket.signatureVerifiedWrites,
      fallbackWrites: bucket.fallbackWrites,
      activeAgents: bucket.agents.size,
      topAgents: Array.from(bucket.agents.values())
        .sort((a, b) => b.writes - a.writes || b.successfulWrites - a.successfulWrites || a.agentName.localeCompare(b.agentName))
        .slice(0, 20)
    };
  }

  const attemptsAll = emptyAccumulator();
  const attemptsEligible = emptyAccumulator();
  const attemptsExcluded = emptyAccumulator();
  const attemptsProxied = emptyAccumulator();
  const attemptsSynthetic = emptyAccumulator();
  const attemptsUnknownAgent = emptyAccumulator();

  for (const row of externalWriteAttemptRows) {
    const normalizedAgent = normalizeAgentOrNull(row.agentName);
    applyAttemptRow(attemptsAll, row, normalizedAgent);
    if (!normalizedAgent) {
      applyAttemptRow(attemptsUnknownAgent, row, normalizedAgent);
      continue;
    }
    if (isProxiedExternalAgentName(normalizedAgent)) {
      applyAttemptRow(attemptsProxied, row, normalizedAgent);
      if (includeProxied) {
        applyAttemptRow(attemptsEligible, row, normalizedAgent);
      }
      continue;
    }
    if (isExcludedExternalAgentName(normalizedAgent)) {
      applyAttemptRow(attemptsExcluded, row, normalizedAgent);
      continue;
    }
    if (isSyntheticAgentName(normalizedAgent)) {
      applyAttemptRow(attemptsSynthetic, row, normalizedAgent);
      continue;
    }
    applyAttemptRow(attemptsEligible, row, normalizedAgent);
  }

  const attempts = externalOnly
    ? {
      totals: finalizeBucket(attemptsAll),
      buckets: {
        eligible: finalizeBucket(attemptsEligible),
        proxied: finalizeBucket(attemptsProxied),
        excluded: finalizeBucket(attemptsExcluded),
        synthetic: finalizeBucket(attemptsSynthetic),
        unknownAgent: finalizeBucket(attemptsUnknownAgent)
      },
      shares: {
        eligibleWriteShare: ratio(attemptsEligible.writes, attemptsAll.writes),
        proxiedWriteShare: ratio(attemptsProxied.writes, attemptsAll.writes),
        excludedWriteShare: ratio(attemptsExcluded.writes, attemptsAll.writes),
        syntheticWriteShare: ratio(attemptsSynthetic.writes, attemptsAll.writes),
        unknownAgentWriteShare: ratio(attemptsUnknownAgent.writes, attemptsAll.writes)
      }
    }
    : {
      totals: emptyAttemptBucket(),
      buckets: {
        eligible: emptyAttemptBucket(),
        proxied: emptyAttemptBucket(),
        excluded: emptyAttemptBucket(),
        synthetic: emptyAttemptBucket(),
        unknownAgent: emptyAttemptBucket()
      },
      shares: {
        eligibleWriteShare: 0,
        proxiedWriteShare: 0,
        excludedWriteShare: 0,
        syntheticWriteShare: 0,
        unknownAgentWriteShare: 0
      }
    };

  if (externalOnly && scopedAgentSet.size === 0) {
    const likelyCause = attempts.totals.writes === 0
      ? 'no_external_write_attempts'
      : (attempts.buckets.eligible.writes === 0
          ? (attempts.buckets.proxied.writes > 0
              ? 'proxied_external_writes_only'
              : 'all_external_writes_filtered_or_missing_agent_name')
          : 'eligible_writes_present_but_no_bound_agents');
    return {
      days: windowDays,
      since: since.toISOString(),
      externalOnly,
      includeSynthetic,
      includeProxied,
      answerWindowHours,
      identityScope: {
        actorTypes: identity?.actorTypes ?? [],
        boundAgents: 0,
        users: identity?.userIds.length ?? 0,
        filteredOutSyntheticBoundAgents: identity?.filteredOutBoundAgents.length ?? 0,
        filteredOutExcludedBoundAgents: identity?.filteredOutExcludedBoundAgents.length ?? 0
      },
      totals: {
        queued: 0,
        opened: 0,
        pending: 0,
        failed: 0,
        webhookOpened: 0,
        inboxOpened: 0,
        answered: 0,
        accepted: 0,
        answeredWithinWindow: 0
      },
      conversion: {
        openRate: 0,
        answerRateFromOpened: 0,
        acceptRateFromAnswered: 0,
        withinWindowRate: 0
      },
      latencyMinutes: {
        median: null,
        p90: null
      },
      attempts,
      diagnostics: {
        likelyCause,
        hasAttemptedWrites: attempts.totals.writes > 0,
        hasEligibleAttemptedWrites: attempts.buckets.eligible.writes > 0,
        hasProxiedAttemptedWrites: attempts.buckets.proxied.writes > 0,
        queueCoverageFromEligibleQuestionCreates: 0
      },
      daily: [],
      topResponders: []
    };
  }

  const deliveryWhere: Prisma.DeliveryQueueWhereInput = {
    createdAt: { gte: since },
    event: 'question.created',
    questionId: { not: null }
  };
  if (scopeApplied && scopeApplied.length > 0) {
    deliveryWhere.agentName = { in: scopeApplied };
  }
  const rows = await prisma.deliveryQueue.findMany({
    where: deliveryWhere,
    select: {
      id: true,
      agentName: true,
      questionId: true,
      createdAt: true,
      deliveredAt: true,
      webhookUrl: true,
      attemptCount: true,
      maxAttempts: true
    },
    orderBy: { createdAt: 'asc' }
  });

  const deliveries = rows
    .map((row) => ({
      ...row,
      agentName: normalizeAgentOrNull(row.agentName),
      questionId: row.questionId?.trim() ?? null
    }))
    .filter((row): row is typeof row & { agentName: string; questionId: string } => Boolean(row.agentName && row.questionId))
    .filter((row) => includeProxied
      ? (!isExcludedExternalAgentName(row.agentName) || isProxiedExternalAgentName(row.agentName))
      : !isExcludedExternalAgentName(row.agentName))
    .filter((row) => includeSynthetic || !isSyntheticAgentName(row.agentName));

  const queued = deliveries.length;
  const openedRows = deliveries.filter((row) => Boolean(row.deliveredAt));
  const opened = openedRows.length;
  const webhookOpened = openedRows.filter((row) => Boolean(row.webhookUrl)).length;
  const inboxOpened = openedRows.filter((row) => !row.webhookUrl).length;
  const pending = deliveries.filter((row) => !row.deliveredAt && row.attemptCount < row.maxAttempts).length;
  const failed = deliveries.filter((row) => !row.deliveredAt && row.attemptCount >= row.maxAttempts).length;
  const queueCoverageFromEligibleQuestionCreates = ratio(queued, attempts.buckets.eligible.questionCreates);

  const earliestByKey = new Map<string, {
    agentName: string;
    questionId: string;
    deliveredAt: Date;
    createdAt: Date;
  }>();
  for (const row of openedRows) {
    if (!row.deliveredAt) continue;
    const key = `${row.agentName}|${row.questionId}`;
    const existing = earliestByKey.get(key);
    if (!existing || row.deliveredAt.getTime() < existing.deliveredAt.getTime()) {
      earliestByKey.set(key, {
        agentName: row.agentName,
        questionId: row.questionId,
        deliveredAt: row.deliveredAt,
        createdAt: row.createdAt
      });
    }
  }

  const openKeys = Array.from(earliestByKey.values());
  const questionIds = Array.from(new Set(openKeys.map((row) => row.questionId)));
  const agentNames = Array.from(new Set(openKeys.map((row) => row.agentName)));
  const answers = questionIds.length > 0 && agentNames.length > 0
    ? await prisma.answer.findMany({
        where: {
          questionId: { in: questionIds },
          OR: [
            { agentName: { in: agentNames } }
          ]
        },
        select: {
          id: true,
          questionId: true,
          agentName: true,
          userId: true,
          createdAt: true
        },
        orderBy: { createdAt: 'asc' }
      })
    : [];

  const earliestAnswerByKey = new Map<string, { id: string; createdAt: Date }>();
  for (const row of answers) {
    const actor = normalizeAgentOrNull(row.agentName) ?? normalizeAgentOrNull(`user:${row.userId}`);
    if (!actor) continue;
    const key = `${actor}|${row.questionId}`;
    if (!earliestAnswerByKey.has(key)) {
      earliestAnswerByKey.set(key, { id: row.id, createdAt: row.createdAt });
    }
  }

  const answeredKeys: Array<{
    key: string;
    agentName: string;
    questionId: string;
    answerId: string;
    minutes: number;
    withinWindow: boolean;
    day: string;
  }> = [];
  for (const item of openKeys) {
    const key = `${item.agentName}|${item.questionId}`;
    const answer = earliestAnswerByKey.get(key);
    if (!answer) continue;
    if (answer.createdAt.getTime() < item.deliveredAt.getTime()) continue;
    const minutes = (answer.createdAt.getTime() - item.deliveredAt.getTime()) / 60000;
    answeredKeys.push({
      key,
      agentName: item.agentName,
      questionId: item.questionId,
      answerId: answer.id,
      minutes,
      withinWindow: answer.createdAt.getTime() <= item.deliveredAt.getTime() + answerWindowMs,
      day: item.createdAt.toISOString().slice(0, 10)
    });
  }
  const answerIds = Array.from(new Set(answeredKeys.map((row) => row.answerId)));
  const acceptedSet = answerIds.length > 0
    ? new Set(
      (await prisma.questionResolution.findMany({
        where: { answerId: { in: answerIds } },
        select: { answerId: true }
      })).map((row) => row.answerId)
    )
    : new Set<string>();

  const answered = answeredKeys.length;
  const accepted = answeredKeys.filter((row) => acceptedSet.has(row.answerId)).length;
  const answeredWithinWindow = answeredKeys.filter((row) => row.withinWindow).length;
  const latencySorted = answeredKeys.map((row) => row.minutes).filter((value) => Number.isFinite(value) && value >= 0).sort((a, b) => a - b);

  const dailyMap = new Map<string, {
    day: string;
    queued: number;
    opened: number;
    answered: number;
    accepted: number;
  }>();
  const dayCursor = new Date(Date.UTC(since.getUTCFullYear(), since.getUTCMonth(), since.getUTCDate()));
  const dayEnd = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  while (dayCursor <= dayEnd) {
    const day = dayCursor.toISOString().slice(0, 10);
    dailyMap.set(day, { day, queued: 0, opened: 0, answered: 0, accepted: 0 });
    dayCursor.setUTCDate(dayCursor.getUTCDate() + 1);
  }
  for (const row of deliveries) {
    const day = row.createdAt.toISOString().slice(0, 10);
    const entry = dailyMap.get(day);
    if (!entry) continue;
    entry.queued += 1;
    if (row.deliveredAt) entry.opened += 1;
  }
  for (const row of answeredKeys) {
    const entry = dailyMap.get(row.day);
    if (!entry) continue;
    entry.answered += 1;
    if (acceptedSet.has(row.answerId)) entry.accepted += 1;
  }

  const responderMap = new Map<string, { agentName: string; answered: number; accepted: number; medianMinutes: number | null }>();
  const minutesByResponder = new Map<string, number[]>();
  for (const row of answeredKeys) {
    const item = responderMap.get(row.agentName) ?? {
      agentName: row.agentName,
      answered: 0,
      accepted: 0,
      medianMinutes: null
    };
    item.answered += 1;
    if (acceptedSet.has(row.answerId)) item.accepted += 1;
    responderMap.set(row.agentName, item);
    const list = minutesByResponder.get(row.agentName) ?? [];
    list.push(row.minutes);
    minutesByResponder.set(row.agentName, list);
  }
  for (const [agentName, list] of minutesByResponder.entries()) {
    const sorted = list.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
    const row = responderMap.get(agentName);
    if (!row) continue;
    row.medianMinutes = percentileFromSorted(sorted, 0.5);
  }

  const likelyCause = attempts.totals.writes === 0
    ? 'no_external_write_attempts'
    : (attempts.buckets.eligible.writes === 0
        ? (attempts.buckets.proxied.writes > 0
            ? 'proxied_external_writes_only'
            : 'all_external_writes_filtered_or_missing_agent_name')
        : (queued === 0
            ? 'eligible_writes_without_delivery_queue_activity'
            : (opened === 0
                ? 'deliveries_not_opened'
                : (answered === 0
                    ? 'opened_not_answered'
                    : (accepted === 0 ? 'answered_not_accepted' : 'healthy_or_insufficient_data')))));

  return {
    days: windowDays,
    since: since.toISOString(),
    externalOnly,
    includeSynthetic,
    includeProxied,
    answerWindowHours,
    identityScope: {
      actorTypes: identity?.actorTypes ?? [],
      boundAgents: identity?.boundAgents.length ?? 0,
      users: identity?.userIds.length ?? 0,
      filteredOutSyntheticBoundAgents: identity?.filteredOutBoundAgents.length ?? 0,
      filteredOutExcludedBoundAgents: identity?.filteredOutExcludedBoundAgents.length ?? 0
    },
    totals: {
      queued,
      opened,
      pending,
      failed,
      webhookOpened,
      inboxOpened,
      answered,
      accepted,
      answeredWithinWindow
    },
    conversion: {
      openRate: ratio(opened, queued),
      answerRateFromOpened: ratio(answered, opened),
      acceptRateFromAnswered: ratio(accepted, answered),
      withinWindowRate: ratio(answeredWithinWindow, answered)
    },
    latencyMinutes: {
      median: percentileFromSorted(latencySorted, 0.5),
      p90: percentileFromSorted(latencySorted, 0.9)
    },
    attempts,
    diagnostics: {
      likelyCause,
      hasAttemptedWrites: attempts.totals.writes > 0,
      hasEligibleAttemptedWrites: attempts.buckets.eligible.writes > 0,
      hasProxiedAttemptedWrites: attempts.buckets.proxied.writes > 0,
      queueCoverageFromEligibleQuestionCreates
    },
    daily: Array.from(dailyMap.values()).sort((a, b) => a.day.localeCompare(b.day)),
    topResponders: Array.from(responderMap.values())
      .sort((a, b) => b.answered - a.answered || b.accepted - a.accepted || a.agentName.localeCompare(b.agentName))
      .slice(0, 20)
  };
}

type TractionMetricComparator = '>=' | '<=';

type WeeklyTractionMetric = {
  id: string;
  label: string;
  description: string;
  unit: 'count' | 'ratio';
  comparator: TractionMetricComparator;
  target: number;
  value: number;
  pass: boolean;
  gap: number;
  waived?: boolean;
};

type WeeklyTractionScorecard = {
  generatedAt: string;
  window: {
    days: number;
    since: string;
  };
  scope: {
    externalOnly: boolean;
    includeSynthetic: boolean;
  };
  targets: {
    boundAgents: number;
    activeAnswerers7d: number;
    questions7d: number;
    answers7d: number;
    answersPerQuestion: number;
    openRate: number;
    answerRateFromOpened: number;
    acceptRateFromAnswered: number;
    retainedAnswererRate7d: number;
  };
  summary: {
    status: 'pass' | 'fail';
    passCount: number;
    failCount: number;
    score: number;
  };
  metrics: WeeklyTractionMetric[];
  snapshots: {
    external: {
      identity: {
        boundAgents: number;
        users: number;
      };
      content: {
        questionsInRange: number;
        answersInRange: number;
        acceptedInRange: number;
      };
      requests: {
        writesInRange: number;
        activeAgentsInRange: number;
      };
      kpi: {
        currentAnswerers7d: number;
        previousAnswerers7d: number;
        retainedAnswerers7d: number;
        retainedAnswererRate7d: number;
      };
      topAgents: Array<{ agentName: string; count: number }>;
    };
    funnel: {
      totals: {
        queued: number;
        opened: number;
        answered: number;
        accepted: number;
      };
      conversion: {
        openRate: number;
        answerRateFromOpened: number;
        acceptRateFromAnswered: number;
      };
    };
  };
};

function buildTractionMetric(input: {
  id: string;
  label: string;
  description: string;
  unit: 'count' | 'ratio';
  comparator: TractionMetricComparator;
  target: number;
  value: number | null | undefined;
  waived?: boolean;
}) {
  const value = Number.isFinite(input.value ?? NaN) ? Number(input.value) : 0;
  const target = Number.isFinite(input.target) ? Number(input.target) : 0;
  const waived = input.waived === true;
  const pass = waived
    ? true
    : (input.comparator === '>=' ? value >= target : value <= target);
  const gap = waived
    ? 0
    : (input.comparator === '>=' ? value - target : target - value);
  return {
    id: input.id,
    label: input.label,
    description: input.description,
    unit: input.unit,
    comparator: input.comparator,
    target,
    value,
    pass,
    gap,
    waived
  } satisfies WeeklyTractionMetric;
}

async function getWeeklyTractionScorecard(days = TRACTION_SCORECARD_DAYS): Promise<WeeklyTractionScorecard> {
  const windowDays = Math.max(7, Math.min(90, Math.floor(days)));
  const usage = await getUsageSummaryCached(windowDays, false) as Awaited<ReturnType<typeof getUsageSummary>>;
  const funnel = await getTractionFunnel(windowDays, {
    externalOnly: true,
    includeSynthetic: false,
    includeProxied: true,
    answerWindowHours: 24
  });

  const externalIdentity = usage.external?.identity ?? {
    boundAgents: 0,
    users: 0
  };
  const externalContent = usage.external?.content ?? {
    questionsInRange: 0,
    answersInRange: 0,
    acceptedInRange: 0
  };
  const externalRequests = usage.external?.requests ?? {
    writesInRange: 0,
    activeAgentsInRange: 0
  };
  const externalKpi = usage.external?.kpi ?? {
    currentAnswerers7d: 0,
    previousAnswerers7d: 0,
    retainedAnswerers7d: 0,
    retainedAnswererRate7d: 0
  };
  const externalTopAgents = Array.isArray(usage.external?.topAgents)
    ? usage.external.topAgents.map((row) => ({
      agentName: String(row.agentName ?? ''),
      count: toNumber(row.count)
    }))
    : [];

  const metrics: WeeklyTractionMetric[] = [
    buildTractionMetric({
      id: 'bound_agents',
      label: 'Bound external agents',
      description: 'Real external agents with bound identity in scope.',
      unit: 'count',
      comparator: '>=',
      target: TRACTION_TARGET_BOUND_AGENTS,
      value: externalIdentity.boundAgents
    }),
    buildTractionMetric({
      id: 'active_answerers_7d',
      label: 'Active answerers (7d)',
      description: 'Distinct real external answerers in the last 7 days.',
      unit: 'count',
      comparator: '>=',
      target: TRACTION_TARGET_ACTIVE_ANSWERERS_7D,
      value: externalKpi.currentAnswerers7d
    }),
    buildTractionMetric({
      id: 'questions_7d',
      label: 'Questions (window)',
      description: 'Real external questions created in scorecard window.',
      unit: 'count',
      comparator: '>=',
      target: TRACTION_TARGET_QUESTIONS_7D,
      value: externalContent.questionsInRange
    }),
    buildTractionMetric({
      id: 'answers_7d',
      label: 'Answers (window)',
      description: 'Real external answers created in scorecard window.',
      unit: 'count',
      comparator: '>=',
      target: TRACTION_TARGET_ANSWERS_7D,
      value: externalContent.answersInRange
    }),
    buildTractionMetric({
      id: 'answers_per_question',
      label: 'Answers per question',
      description: 'Depth of response for real external questions.',
      unit: 'ratio',
      comparator: '>=',
      target: TRACTION_TARGET_ANSWERS_PER_QUESTION,
      value: ratio(externalContent.answersInRange, externalContent.questionsInRange)
    }),
    buildTractionMetric({
      id: 'open_rate',
      label: 'Delivery open rate',
      description: 'Share of queued deliveries opened by real external agents.',
      unit: 'ratio',
      comparator: '>=',
      target: TRACTION_TARGET_OPEN_RATE,
      value: funnel.conversion.openRate
    }),
    buildTractionMetric({
      id: 'answer_rate_from_opened',
      label: 'Answer rate from opened',
      description: 'Opened deliveries that resulted in an answer.',
      unit: 'ratio',
      comparator: '>=',
      target: TRACTION_TARGET_ANSWER_RATE_FROM_OPENED,
      value: funnel.conversion.answerRateFromOpened
    }),
    buildTractionMetric({
      id: 'accept_rate_from_answered',
      label: 'Accept rate from answered',
      description: 'Answered deliveries that were accepted.',
      unit: 'ratio',
      comparator: '>=',
      target: TRACTION_TARGET_ACCEPT_RATE_FROM_ANSWERED,
      value: funnel.conversion.acceptRateFromAnswered
    }),
    buildTractionMetric({
      id: 'retained_answerer_rate_7d',
      label: 'Retained answerer rate (7d)',
      description: 'Current 7d answerers also active in prior 7d.',
      unit: 'ratio',
      comparator: '>=',
      target: TRACTION_TARGET_RETAINED_ANSWERER_RATE_7D,
      value: externalKpi.retainedAnswererRate7d,
      waived: toNumber(externalKpi.previousAnswerers7d) === 0
    })
  ];

  const passCount = metrics.filter((row) => row.pass).length;
  const failCount = metrics.length - passCount;
  const score = Math.round((passCount / Math.max(1, metrics.length)) * 100);

  return {
    generatedAt: new Date().toISOString(),
    window: {
      days: windowDays,
      since: usage.since
    },
    scope: {
      externalOnly: true,
      includeSynthetic: false
    },
    targets: {
      boundAgents: TRACTION_TARGET_BOUND_AGENTS,
      activeAnswerers7d: TRACTION_TARGET_ACTIVE_ANSWERERS_7D,
      questions7d: TRACTION_TARGET_QUESTIONS_7D,
      answers7d: TRACTION_TARGET_ANSWERS_7D,
      answersPerQuestion: TRACTION_TARGET_ANSWERS_PER_QUESTION,
      openRate: TRACTION_TARGET_OPEN_RATE,
      answerRateFromOpened: TRACTION_TARGET_ANSWER_RATE_FROM_OPENED,
      acceptRateFromAnswered: TRACTION_TARGET_ACCEPT_RATE_FROM_ANSWERED,
      retainedAnswererRate7d: TRACTION_TARGET_RETAINED_ANSWERER_RATE_7D
    },
    summary: {
      status: failCount === 0 ? 'pass' : 'fail',
      passCount,
      failCount,
      score
    },
    metrics,
    snapshots: {
      external: {
        identity: {
          boundAgents: toNumber(externalIdentity.boundAgents),
          users: toNumber(externalIdentity.users)
        },
        content: {
          questionsInRange: toNumber(externalContent.questionsInRange),
          answersInRange: toNumber(externalContent.answersInRange),
          acceptedInRange: toNumber(externalContent.acceptedInRange)
        },
        requests: {
          writesInRange: toNumber(externalRequests.writesInRange),
          activeAgentsInRange: toNumber(externalRequests.activeAgentsInRange)
        },
      kpi: {
        currentAnswerers7d: toNumber(externalKpi.currentAnswerers7d),
        previousAnswerers7d: toNumber(externalKpi.previousAnswerers7d),
        retainedAnswerers7d: toNumber(externalKpi.retainedAnswerers7d),
        retainedAnswererRate7d: toNumber(externalKpi.retainedAnswererRate7d)
      },
        topAgents: externalTopAgents
      },
      funnel: {
        totals: {
          queued: funnel.totals.queued,
          opened: funnel.totals.opened,
          answered: funnel.totals.answered,
          accepted: funnel.totals.accepted
        },
        conversion: {
          openRate: funnel.conversion.openRate,
          answerRateFromOpened: funnel.conversion.answerRateFromOpened,
          acceptRateFromAnswered: funnel.conversion.acceptRateFromAnswered
        }
      }
    }
  };
}

async function dispatchTractionScorecardAlert(input?: {
  source?: 'loop' | 'manual';
  force?: boolean;
  days?: number;
}) {
  const source = input?.source ?? 'manual';
  const force = input?.force === true;
  const scorecard = await getWeeklyTractionScorecard(input?.days ?? TRACTION_SCORECARD_DAYS);
  const status = scorecard.summary.status;
  const failingMetrics = scorecard.metrics
    .filter((row) => !row.pass)
    .map((row) => ({
      id: row.id,
      label: row.label,
      value: row.value,
      comparator: row.comparator,
      target: row.target,
      gap: row.gap,
      unit: row.unit
    }));

  const digest = sha256(JSON.stringify({
    status,
    failingMetrics: failingMetrics.map((row) => ({
      id: row.id,
      value: Number(row.value.toFixed(6)),
      target: Number(row.target.toFixed(6)),
      comparator: row.comparator
    }))
  }));
  const nowMs = Date.now();
  const cooldownMs = TRACTION_ALERT_COOLDOWN_MINUTES * 60 * 1000;
  const changed = digest !== lastTractionAlertDigest || status !== lastTractionAlertStatus;
  const inCooldown = nowMs - lastTractionAlertAt < cooldownMs;
  const shouldSendByStatus = status === 'fail' || lastTractionAlertStatus === 'fail';

  if (!TRACTION_ALERT_WEBHOOK_URL) {
    return {
      ok: true,
      sent: false,
      reason: 'webhook_not_configured',
      source,
      status,
      scorecard
    };
  }
  if (!force && !shouldSendByStatus) {
    return {
      ok: true,
      sent: false,
      reason: 'healthy_no_alert',
      source,
      status,
      scorecard
    };
  }
  if (!force && inCooldown && !changed) {
    return {
      ok: true,
      sent: false,
      reason: 'cooldown',
      source,
      status,
      scorecard
    };
  }

  const payload = {
    event: 'a2abench.traction.scorecard',
    source,
    generatedAt: new Date().toISOString(),
    status,
    summary: scorecard.summary,
    window: scorecard.window,
    failingMetrics,
    metrics: scorecard.metrics.map((row) => ({
      id: row.id,
      label: row.label,
      pass: row.pass,
      value: row.value,
      comparator: row.comparator,
      target: row.target,
      unit: row.unit
    })),
    topAgents: scorecard.snapshots.external.topAgents.slice(0, 10)
  };

  const response = await fetch(TRACTION_ALERT_WEBHOOK_URL, {
    method: 'POST',
    headers: {
      'content-type': 'application/json'
    },
    body: JSON.stringify(payload)
  });
  const responseText = await response.text();
  if (!response.ok) {
    throw new Error(`traction_alert_webhook_failed: HTTP ${response.status} ${responseText.slice(0, 500)}`);
  }

  lastTractionAlertDigest = digest;
  lastTractionAlertStatus = status;
  lastTractionAlertAt = nowMs;

  return {
    ok: true,
    sent: true,
    source,
    status,
    webhookStatus: response.status,
    scorecard
  };
}

function base64UrlEncode(value: string) {
  return Buffer.from(value, 'utf8').toString('base64url');
}

function base64UrlDecode(value: string) {
  return Buffer.from(value, 'base64url').toString('utf8');
}

type AcceptLinkClaims = {
  q: string;
  a: string;
  u: string;
  e: number;
};

function createAcceptLinkToken(claims: AcceptLinkClaims) {
  if (!ACCEPT_LINK_SECRET) return null;
  const payload = base64UrlEncode(JSON.stringify(claims));
  const signature = crypto.createHmac('sha256', ACCEPT_LINK_SECRET).update(payload).digest('base64url');
  // Envelope payload+signature so token remains strictly base64url (route-safe).
  return base64UrlEncode(JSON.stringify({ p: payload, s: signature }));
}

function parseAcceptLinkToken(token: string) {
  if (!ACCEPT_LINK_SECRET) return null;
  const normalized = decodeURIComponent(token);
  let payload = '';
  let signature = '';
  try {
    const envelope = JSON.parse(base64UrlDecode(normalized)) as { p?: unknown; s?: unknown };
    if (typeof envelope?.p === 'string' && typeof envelope?.s === 'string') {
      payload = envelope.p;
      signature = envelope.s;
    }
  } catch {
    // Backward compatible: accept legacy "." and "~" token formats.
    const separator = normalized.includes('~') ? '~' : '.';
    [payload, signature] = normalized.split(separator);
  }
  if (!payload || !signature) return null;
  const expected = crypto.createHmac('sha256', ACCEPT_LINK_SECRET).update(payload).digest('base64url');
  if (expected.length !== signature.length) return null;
  if (!crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(signature))) return null;
  try {
    const parsed = JSON.parse(base64UrlDecode(payload)) as Partial<AcceptLinkClaims>;
    if (!parsed || typeof parsed.q !== 'string' || typeof parsed.a !== 'string' || typeof parsed.u !== 'string') return null;
    const expiresAtMs = Number(parsed.e);
    if (!Number.isFinite(expiresAtMs) || expiresAtMs < Date.now()) return null;
    return {
      questionId: parsed.q,
      answerId: parsed.a,
      ownerUserId: parsed.u,
      expiresAtMs
    };
  } catch {
    return null;
  }
}

function extractAcceptToken(input: string) {
  const raw = input.trim();
  if (!raw) return '';
  if (!/^https?:\/\//i.test(raw)) return raw;
  try {
    const parsed = new URL(raw);
    const queryToken = parsed.searchParams.get('token');
    if (queryToken) return queryToken;
    const segments = parsed.pathname.split('/').filter(Boolean);
    const idx = segments.findIndex((segment) => segment === 'accept-links');
    if (idx >= 0 && idx + 1 < segments.length) return segments[idx + 1];
  } catch {
    return raw;
  }
  return raw;
}

async function acceptAnswerFromToken(token: string, acceptedByAgentName: string | null, baseUrl: string) {
  const claims = parseAcceptLinkToken(token);
  if (!claims) {
    return {
      status: 401,
      payload: { error: 'Invalid or expired accept link.' }
    };
  }
  const result = await acceptAnswerForQuestion({
    questionId: claims.questionId,
    answerId: claims.answerId,
    ownerUserId: claims.ownerUserId,
    acceptedByAgentName,
    baseUrl
  });
  return {
    status: result.status,
    payload: {
      ...result.payload,
      acceptedVia: 'accept_link',
      linkExpiresAt: new Date(claims.expiresAtMs).toISOString()
    }
  };
}

function buildAcceptLink(baseUrl: string, questionId: string, answerId: string, ownerUserId: string, ttlMinutes = ACCEPT_LINK_TTL_MINUTES) {
  const expiresAtMs = Date.now() + Math.max(5, ttlMinutes) * 60 * 1000;
  const token = createAcceptLinkToken({
    q: questionId,
    a: answerId,
    u: ownerUserId,
    e: expiresAtMs
  });
  if (!token) return null;
  return {
    token,
    url: `${baseUrl}/api/v1/accept-links?token=${encodeURIComponent(token)}`,
    expiresAt: new Date(expiresAtMs).toISOString()
  };
}

async function ensureUserHandle(handle: string) {
  const normalized = handle.trim().toLowerCase();
  return prisma.user.upsert({
    where: { handle: normalized },
    update: {},
    create: { handle: normalized }
  });
}

async function processAcceptanceReminders(baseUrl: string, limit = ACCEPTANCE_REMINDER_LIMIT) {
  const now = new Date();
  const stages = getReminderStagesHours();
  const take = Math.max(20, Math.min(1000, Math.max(1, limit) * 4));
  const candidates = await prisma.question.findMany({
    where: {
      resolution: null,
      answers: { some: {} }
    },
    include: {
      tags: { include: { tag: true } },
      answers: { orderBy: { createdAt: 'desc' }, take: 1 },
      reminders: {
        where: { sentAt: { not: null } },
        select: { stageHours: true }
      }
    },
    orderBy: { updatedAt: 'desc' },
    take
  });

  const results: Array<{
    questionId: string;
    answerId: string;
    stageHours: number;
  }> = [];

  for (const question of candidates) {
    if (results.length >= limit) break;
    const latestAnswer = question.answers[0];
    if (!latestAnswer) continue;
    const ageHours = (now.getTime() - latestAnswer.createdAt.getTime()) / (60 * 60 * 1000);
    const seenStages = new Set(question.reminders.map((row) => row.stageHours));
    const stageHours = stages.find((stage) => ageHours >= stage && !seenStages.has(stage));
    if (!stageHours) continue;

    const dueAt = new Date(latestAnswer.createdAt.getTime() + stageHours * 60 * 60 * 1000);
    try {
      await prisma.acceptanceReminder.create({
        data: {
          questionId: question.id,
          answerId: latestAnswer.id,
          stageHours,
          dueAt,
          sentAt: now
        }
      });
    } catch {
      continue;
    }

    const acceptLink = buildAcceptLink(baseUrl, question.id, latestAnswer.id, question.userId);
    void dispatchAcceptanceReminderEvent({
      id: question.id,
      title: question.title,
      bodyText: question.bodyText,
      createdAt: question.createdAt,
      tags: question.tags.map((link) => link.tag.name),
      url: `${baseUrl}/q/${question.id}`,
      answerId: latestAnswer.id,
      answerAgentName: latestAnswer.agentName ?? null,
      answerCreatedAt: latestAnswer.createdAt,
      reminderStageHours: stageHours,
      acceptUrl: acceptLink?.url ?? null,
      source: getQuestionSource(question)
    }).catch(() => undefined);

    results.push({
      questionId: question.id,
      answerId: latestAnswer.id,
      stageHours
    });
  }

  return {
    stages,
    processed: candidates.length,
    queued: results.length,
    reminders: results
  };
}

async function processAutoCloseQuestions(baseUrl: string, limit = AUTO_CLOSE_PROCESS_LIMIT) {
  const now = new Date();
  const questionCutoff = new Date(now.getTime() - (AUTO_CLOSE_AFTER_MINUTES * 60 * 1000));
  const answerCutoff = new Date(now.getTime() - (AUTO_CLOSE_MIN_ANSWER_AGE_MINUTES * 60 * 1000));
  const take = Math.max(20, Math.min(1000, Math.max(1, limit) * 4));
  const candidates = await prisma.question.findMany({
    where: {
      resolution: null,
      createdAt: { lte: questionCutoff },
      answers: { some: { createdAt: { lte: answerCutoff } } }
    },
    include: {
      answers: {
        orderBy: { createdAt: 'asc' },
        select: { id: true, createdAt: true }
      }
    },
    orderBy: { createdAt: 'asc' },
    take
  });

  const results: Array<{
    questionId: string;
    answerId: string;
    changed: boolean;
  }> = [];
  let failed = 0;

  for (const question of candidates) {
    if (results.length >= limit) break;
    const answerIds = question.answers.map((row) => row.id);
    if (answerIds.length === 0) continue;
    const scoreRows = await prisma.answerVote.groupBy({
      by: ['answerId'],
      where: { answerId: { in: answerIds } },
      _sum: { value: true }
    });
    const scoreMap = new Map<string, number>(
      scoreRows.map((row) => [row.answerId, row._sum.value ?? 0])
    );
    const best = question.answers
      .map((row) => ({ ...row, score: scoreMap.get(row.id) ?? 0 }))
      .sort((a, b) => {
        if (b.score !== a.score) return b.score - a.score;
        return a.createdAt.getTime() - b.createdAt.getTime();
      })[0];
    if (!best) continue;

    try {
      const accepted = await acceptAnswerForQuestion({
        questionId: question.id,
        answerId: best.id,
        ownerUserId: question.userId,
        acceptedByAgentName: AUTO_CLOSE_AGENT_NAME,
        baseUrl
      });
      if (accepted.status === 200 && accepted.payload.ok) {
        results.push({
          questionId: question.id,
          answerId: best.id,
          changed: Boolean(accepted.payload.changed)
        });
      } else {
        failed += 1;
      }
    } catch {
      failed += 1;
    }
  }

  return {
    processed: candidates.length,
    closed: results.length,
    failed,
    policy: {
      enabled: AUTO_CLOSE_ENABLED,
      afterMinutes: AUTO_CLOSE_AFTER_MINUTES,
      minAnswerAgeMinutes: AUTO_CLOSE_MIN_ANSWER_AGE_MINUTES,
      afterHours: AUTO_CLOSE_AFTER_HOURS,
      minAnswerAgeHours: AUTO_CLOSE_MIN_ANSWER_AGE_HOURS,
      acceptedBy: AUTO_CLOSE_AGENT_NAME
    },
    results
  };
}

async function processSourceResolutionCallbacks(baseUrl: string, limit = 200, dryRun = false) {
  const take = Math.max(1, Math.min(1000, Math.max(limit, 20)));
  const rows = await prisma.questionResolution.findMany({
    orderBy: { updatedAt: 'desc' },
    take,
    include: {
      answer: {
        select: {
          id: true,
          agentName: true,
          bodyText: true
        }
      },
      question: {
        select: {
          id: true,
          title: true,
          sourceType: true,
          sourceUrl: true
        }
      }
    }
  });

  const results: Array<{
    questionId: string;
    sourceType: string | null;
    sourceUrl: string | null;
    acceptedAnswerId: string;
    sent: boolean;
    reason: string;
    commentUrl?: string | null;
  }> = [];
  let sent = 0;
  let skipped = 0;
  let failed = 0;

  for (const row of rows) {
    const payload = {
      questionId: row.questionId,
      questionTitle: row.question.title,
      questionUrl: `${baseUrl}/q/${row.questionId}`,
      sourceType: row.question.sourceType ?? null,
      sourceUrl: row.question.sourceUrl ?? null,
      acceptedAt: row.updatedAt,
      acceptedAnswerId: row.answerId,
      acceptedAgentName: normalizeAgentOrNull(row.answer.agentName ?? null),
      answerBodyText: row.answer.bodyText ?? null
    };

    const issueRef = parseGithubIssueRef(payload.sourceUrl);
    if (dryRun) {
      const reason = !payload.sourceUrl
        ? 'missing_source_url'
        : (!issueRef ? 'unsupported_source_url' : (!SOURCE_CALLBACK_GITHUB_TOKEN ? 'github_token_missing' : 'eligible'));
      results.push({
        questionId: payload.questionId,
        sourceType: payload.sourceType,
        sourceUrl: payload.sourceUrl,
        acceptedAnswerId: payload.acceptedAnswerId,
        sent: false,
        reason
      });
      if (reason === 'eligible') sent += 1;
      else skipped += 1;
      continue;
    }

    try {
      const outcome = await dispatchSourceResolutionCallback(payload);
      if (outcome.sent) sent += 1;
      else skipped += 1;
      results.push({
        questionId: payload.questionId,
        sourceType: payload.sourceType,
        sourceUrl: payload.sourceUrl,
        acceptedAnswerId: payload.acceptedAnswerId,
        sent: outcome.sent,
        reason: outcome.reason,
        commentUrl: 'commentUrl' in outcome ? (outcome.commentUrl ?? null) : null
      });
    } catch (err) {
      failed += 1;
      results.push({
        questionId: payload.questionId,
        sourceType: payload.sourceType,
        sourceUrl: payload.sourceUrl,
        acceptedAnswerId: payload.acceptedAnswerId,
        sent: false,
        reason: err instanceof Error ? compactText(err.message, 240) : 'source_callback_failed'
      });
    }
  }

  return {
    dryRun,
    scanned: rows.length,
    sent,
    skipped,
    failed,
    results
  };
}

function getNextJobGuardrailSnapshot() {
  return {
    enabled: NEXT_JOB_GUARDRAIL_ENABLED,
    easyModeEnabled: nextJobGuardrailState.easyModeEnabled,
    reason: nextJobGuardrailState.reason,
    updatedAt: nextJobGuardrailState.updatedAt,
    stickyUntil: nextJobGuardrailState.stickyUntil,
    lastDecision: nextJobGuardrailState.lastDecision,
    config: {
      intervalMs: NEXT_JOB_GUARDRAIL_INTERVAL_MS,
      windowMinutes: NEXT_JOB_GUARDRAIL_WINDOW_MINUTES,
      minStrictWrites: NEXT_JOB_GUARDRAIL_MIN_STRICT_WRITES,
      triggerAnswerRate: NEXT_JOB_GUARDRAIL_TRIGGER_ANSWER_RATE,
      recoverAnswerRate: NEXT_JOB_GUARDRAIL_RECOVER_ANSWER_RATE,
      stickyMinutes: NEXT_JOB_GUARDRAIL_STICKY_MINUTES,
      easyMinSolvability: NEXT_JOB_GUARDRAIL_EASY_MIN_SOLVABILITY
    },
    lastWindow: nextJobGuardrailState.lastWindow
  };
}

async function getNextJobGuardrailWindowSnapshot(): Promise<NextJobGuardrailWindow> {
  const until = new Date();
  const since = new Date(until.getTime() - NEXT_JOB_GUARDRAIL_WINDOW_MINUTES * 60 * 1000);
  const rows = await prisma.$queryRaw<Array<{ agentName: string | null; route: string; count: bigint | number | string }>>`
    SELECT
      NULLIF("agentName", '') AS "agentName",
      "route" AS "route",
      COUNT(*) AS count
    FROM "UsageEvent"
    WHERE "createdAt" >= ${since}
      AND "createdAt" <= ${until}
      AND UPPER("method") = 'POST'
      AND "status" BETWEEN 200 AND 299
      AND "route" IN ('/api/v1/questions', '/api/v1/questions/:id/answers', '/api/v1/questions/:id/answer-job')
    GROUP BY 1, 2
  `;

  const snapshot: NextJobGuardrailWindow = {
    since: since.toISOString(),
    until: until.toISOString(),
    strictWrites: 0,
    strictQuestionWrites: 0,
    strictAnswerWrites: 0,
    strictAnswerRate: 0,
    proxiedWrites: 0,
    proxiedQuestionWrites: 0,
    proxiedAnswerWrites: 0
  };

  for (const row of rows) {
    const route = String(row.route ?? '');
    const count = toNumber(row.count);
    if (count <= 0) continue;
    const normalizedAgent = normalizeAgentOrNull(row.agentName);
    if (!normalizedAgent) continue;
    const isQuestionWrite = route === '/api/v1/questions';
    const isAnswerWrite = route === '/api/v1/questions/:id/answers' || route === '/api/v1/questions/:id/answer-job';
    if (!isQuestionWrite && !isAnswerWrite) continue;

    if (isExternalAdoptionAgentName(normalizedAgent)) {
      snapshot.strictWrites += count;
      if (isQuestionWrite) snapshot.strictQuestionWrites += count;
      if (isAnswerWrite) snapshot.strictAnswerWrites += count;
      continue;
    }
    if (isProxiedExternalAgentName(normalizedAgent)) {
      snapshot.proxiedWrites += count;
      if (isQuestionWrite) snapshot.proxiedQuestionWrites += count;
      if (isAnswerWrite) snapshot.proxiedAnswerWrites += count;
    }
  }

  snapshot.strictAnswerRate = ratio(snapshot.strictAnswerWrites, snapshot.strictWrites);
  return snapshot;
}

async function runNextJobGuardrail(input: { source: 'startup' | 'loop' }) {
  if (!NEXT_JOB_GUARDRAIL_ENABLED) {
    nextJobGuardrailState.easyModeEnabled = false;
    nextJobGuardrailState.reason = 'guardrail_disabled';
    nextJobGuardrailState.updatedAt = new Date().toISOString();
    nextJobGuardrailState.stickyUntil = null;
    nextJobGuardrailState.lastDecision = 'disabled';
    nextJobGuardrailState.lastWindow = null;
    return;
  }

  const snapshot = await getNextJobGuardrailWindowSnapshot();
  const nowMs = Date.now();
  const currentlyEnabled = nextJobGuardrailState.easyModeEnabled;
  const stickyUntilMs = nextJobGuardrailState.stickyUntil ? Date.parse(nextJobGuardrailState.stickyUntil) : Number.NaN;
  const stickyActive = Number.isFinite(stickyUntilMs) && stickyUntilMs > nowMs;

  let nextEnabled = currentlyEnabled;
  let reason = nextJobGuardrailState.reason;
  let decision = 'hold';

  if (!currentlyEnabled) {
    if (
      snapshot.strictWrites >= NEXT_JOB_GUARDRAIL_MIN_STRICT_WRITES
      && snapshot.strictAnswerRate < NEXT_JOB_GUARDRAIL_TRIGGER_ANSWER_RATE
    ) {
      nextEnabled = true;
      reason = 'enabled_low_strict_answer_rate';
      decision = 'enable_easy_mode';
    } else if (snapshot.strictWrites < NEXT_JOB_GUARDRAIL_MIN_STRICT_WRITES) {
      reason = 'insufficient_strict_volume';
      decision = 'hold_low_volume';
    } else {
      reason = 'strict_answer_rate_healthy';
      decision = 'hold_healthy';
    }
  } else if (stickyActive) {
    reason = 'sticky_window_active';
    decision = 'hold_sticky';
  } else if (
    snapshot.strictWrites >= NEXT_JOB_GUARDRAIL_MIN_STRICT_WRITES
    && snapshot.strictAnswerRate >= NEXT_JOB_GUARDRAIL_RECOVER_ANSWER_RATE
  ) {
    nextEnabled = false;
    reason = 'disabled_recovered_strict_answer_rate';
    decision = 'disable_recovered';
  } else if (snapshot.strictWrites < NEXT_JOB_GUARDRAIL_MIN_STRICT_WRITES) {
    reason = 'hold_easy_low_volume';
    decision = 'hold_easy_low_volume';
  } else {
    reason = 'hold_easy_low_conversion';
    decision = 'hold_easy_low_conversion';
  }

  const changed = nextEnabled !== currentlyEnabled;
  nextJobGuardrailState.easyModeEnabled = nextEnabled;
  nextJobGuardrailState.reason = reason;
  nextJobGuardrailState.updatedAt = new Date(nowMs).toISOString();
  nextJobGuardrailState.lastDecision = decision;
  nextJobGuardrailState.lastWindow = snapshot;
  if (nextEnabled) {
    nextJobGuardrailState.stickyUntil = new Date(nowMs + NEXT_JOB_GUARDRAIL_STICKY_MINUTES * 60 * 1000).toISOString();
  } else {
    nextJobGuardrailState.stickyUntil = null;
  }

  if (changed) {
    fastify.log.info({
      source: input.source,
      easyModeEnabled: nextEnabled,
      reason,
      strictWrites: snapshot.strictWrites,
      strictAnswerRate: Number(snapshot.strictAnswerRate.toFixed(4)),
      proxiedWrites: snapshot.proxiedWrites
    }, 'next-job guardrail toggled ranking mode');
  }
}

function startBackgroundWorkers() {
  if (!usageFlushTimer) {
    usageFlushTimer = setInterval(() => {
      if (usageEventBuffer.length === 0) return;
      void withPrismaPoolRetry('usage_event_flush', () => flushUsageEventBuffer(), 3).catch((err) => {
        fastify.log.warn({ err }, 'usage event flush failed');
      });
    }, USAGE_LOG_FLUSH_INTERVAL_MS);
    usageFlushTimer.unref?.();
  }

  if (DELIVERY_LOOP_ENABLED && !deliveryLoopTimer) {
    deliveryLoopTimer = setInterval(() => {
      if (deliveryLoopRunning) return;
      deliveryLoopRunning = true;
      void withPrismaPoolRetry('delivery_queue_loop', () => processDeliveryQueue(DELIVERY_PROCESS_LIMIT), 3)
        .catch((err) => {
          fastify.log.warn({ err }, 'delivery queue loop failed');
        })
        .finally(() => {
          deliveryLoopRunning = false;
        });
    }, DELIVERY_LOOP_INTERVAL_MS);
    deliveryLoopTimer.unref?.();
  }

  if (DELIVERY_REQUEUE_OPENED_ENABLED && !deliveryRequeueLoopTimer) {
    deliveryRequeueLoopTimer = setInterval(() => {
      if (deliveryRequeueLoopRunning) return;
      deliveryRequeueLoopRunning = true;
      void withPrismaPoolRetry(
        'delivery_requeue_opened_loop',
        () => processOpenedUnansweredRequeue(),
        3
      )
        .then((summary) => {
          if (summary.requeued > 0) {
            fastify.log.info({
              requeued: summary.requeued,
              scanned: summary.scanned,
              skipped: summary.skipped
            }, 'delivery requeue loop re-enqueued opened unanswered jobs');
          }
        })
        .catch((err) => {
          fastify.log.warn({ err }, 'delivery requeue loop failed');
        })
        .finally(() => {
          deliveryRequeueLoopRunning = false;
        });
    }, DELIVERY_REQUEUE_LOOP_INTERVAL_MS);
    deliveryRequeueLoopTimer.unref?.();
    if (!deliveryRequeueLoopRunning) {
      deliveryRequeueLoopRunning = true;
      void withPrismaPoolRetry(
        'delivery_requeue_opened_startup',
        () => processOpenedUnansweredRequeue(),
        3
      )
        .catch((err) => {
          fastify.log.warn({ err }, 'delivery requeue startup run failed');
        })
        .finally(() => {
          deliveryRequeueLoopRunning = false;
        });
    }
  }

  if (REMINDER_LOOP_ENABLED && !reminderLoopTimer) {
    reminderLoopTimer = setInterval(() => {
      if (reminderLoopRunning) return;
      reminderLoopRunning = true;
      void withPrismaPoolRetry('acceptance_reminder_loop', async () => {
        const summary = await processAcceptanceReminders(SYSTEM_BASE_URL, ACCEPTANCE_REMINDER_LIMIT);
        if (summary.queued > 0) {
          await processDeliveryQueue(Math.min(DELIVERY_PROCESS_LIMIT, summary.queued * 5));
        }
      }, 3)
        .catch((err) => {
          fastify.log.warn({ err }, 'acceptance reminder loop failed');
        })
        .finally(() => {
          reminderLoopRunning = false;
        });
    }, REMINDER_LOOP_INTERVAL_MS);
    reminderLoopTimer.unref?.();
  }

  if (AUTO_CLOSE_ENABLED && !autoCloseLoopTimer) {
    autoCloseLoopTimer = setInterval(() => {
      if (autoCloseLoopRunning) return;
      autoCloseLoopRunning = true;
      void withPrismaPoolRetry('autoclose_loop', async () => {
        const summary = await processAutoCloseQuestions(SYSTEM_BASE_URL, AUTO_CLOSE_PROCESS_LIMIT);
        if (summary.closed > 0) {
          await processDeliveryQueue(Math.min(DELIVERY_PROCESS_LIMIT, summary.closed * 5));
        }
      }, 3)
        .catch((err) => {
          fastify.log.warn({ err }, 'autoclose loop failed');
        })
        .finally(() => {
          autoCloseLoopRunning = false;
        });
    }, AUTO_CLOSE_LOOP_INTERVAL_MS);
    autoCloseLoopTimer.unref?.();
  }

  if (SUBSCRIPTION_PRUNE_ENABLED && !subscriptionPruneLoopTimer) {
    subscriptionPruneLoopTimer = setInterval(() => {
      if (subscriptionPruneLoopRunning) return;
      subscriptionPruneLoopRunning = true;
      void withPrismaPoolRetry('subscription_prune_loop', () => pruneInactiveSubscriptions(), 3)
        .then((summary) => {
          if (summary.disabled > 0) {
            fastify.log.info({
              disabled: summary.disabled,
              candidates: summary.candidates,
              reasons: summary.reasons
            }, 'subscription liveness prune disabled inactive subscriptions');
          }
        })
        .catch((err) => {
          fastify.log.warn({ err }, 'subscription prune loop failed');
        })
        .finally(() => {
          subscriptionPruneLoopRunning = false;
        });
    }, SUBSCRIPTION_PRUNE_INTERVAL_MS);
    subscriptionPruneLoopTimer.unref?.();
  }

  if (TRACTION_ALERT_LOOP_ENABLED && !tractionAlertLoopTimer) {
    tractionAlertLoopTimer = setInterval(() => {
      if (tractionAlertLoopRunning) return;
      tractionAlertLoopRunning = true;
      void withPrismaPoolRetry(
        'traction_alert_loop',
        () => dispatchTractionScorecardAlert({ source: 'loop' }),
        2
      )
        .then((summary) => {
          if (summary.sent) {
            fastify.log.info({
              status: summary.status,
              source: summary.source
            }, 'traction scorecard alert sent');
          }
        })
        .catch((err) => {
          fastify.log.warn({ err }, 'traction alert loop failed');
        })
        .finally(() => {
          tractionAlertLoopRunning = false;
        });
    }, TRACTION_ALERT_LOOP_INTERVAL_MS);
    tractionAlertLoopTimer.unref?.();
  }

  if (IMPORT_SEED_LOOP_ENABLED && !sourceImportLoopTimer) {
    sourceImportLoopTimer = setInterval(() => {
      if (sourceImportLoopRunning) return;
      sourceImportLoopRunning = true;
      void runSourceSeedImport({ source: 'loop' })
        .then((summary) => {
          if (summary.created > 0 || summary.warnings.length > 0) {
            fastify.log.info({
              selected: summary.selected,
              created: summary.created,
              skipped: summary.skipped,
              warnings: summary.warnings.slice(0, 10),
              dryRun: summary.dryRun
            }, 'source seed loop imported unresolved questions');
          }
        })
        .catch((err) => {
          fastify.log.warn({ err }, 'source seed loop failed');
        })
        .finally(() => {
          sourceImportLoopRunning = false;
        });
    }, IMPORT_SEED_LOOP_INTERVAL_MS);
    sourceImportLoopTimer.unref?.();
    if (!sourceImportLoopRunning) {
      sourceImportLoopRunning = true;
      void runSourceSeedImport({ source: 'loop' })
        .then((summary) => {
          if (summary.created > 0 || summary.warnings.length > 0) {
            fastify.log.info({
              selected: summary.selected,
              created: summary.created,
              skipped: summary.skipped,
              warnings: summary.warnings.slice(0, 10),
              dryRun: summary.dryRun
            }, 'source seed startup import completed');
          }
        })
        .catch((err) => {
          fastify.log.warn({ err }, 'source seed startup import failed');
        })
        .finally(() => {
          sourceImportLoopRunning = false;
        });
    }
  }

  if (NEXT_JOB_GUARDRAIL_ENABLED && !nextJobGuardrailLoopTimer) {
    nextJobGuardrailLoopTimer = setInterval(() => {
      if (nextJobGuardrailLoopRunning) return;
      nextJobGuardrailLoopRunning = true;
      void withPrismaPoolRetry('next_job_guardrail_loop', () => runNextJobGuardrail({ source: 'loop' }), 3)
        .catch((err) => {
          fastify.log.warn({ err }, 'next-job guardrail loop failed');
        })
        .finally(() => {
          nextJobGuardrailLoopRunning = false;
        });
    }, NEXT_JOB_GUARDRAIL_INTERVAL_MS);
    nextJobGuardrailLoopTimer.unref?.();
    if (!nextJobGuardrailLoopRunning) {
      nextJobGuardrailLoopRunning = true;
      void withPrismaPoolRetry('next_job_guardrail_startup', () => runNextJobGuardrail({ source: 'startup' }), 3)
        .catch((err) => {
          fastify.log.warn({ err }, 'next-job guardrail startup run failed');
        })
        .finally(() => {
          nextJobGuardrailLoopRunning = false;
        });
    }
  }

  fastify.log.info({
    usageLogFlushMs: USAGE_LOG_FLUSH_INTERVAL_MS,
    deliveryLoopEnabled: DELIVERY_LOOP_ENABLED,
    deliveryLoopMs: DELIVERY_LOOP_INTERVAL_MS,
    deliveryRequireRecentActivity: DELIVERY_REQUIRE_RECENT_ACTIVITY,
    deliveryActiveWebhookWindowHours: DELIVERY_ACTIVE_WEBHOOK_WINDOW_HOURS,
    deliveryActiveInboxWindowMinutes: DELIVERY_ACTIVE_INBOX_WINDOW_MINUTES,
    deliveryNewSubscriptionGraceMinutes: DELIVERY_NEW_SUBSCRIPTION_GRACE_MINUTES,
    deliveryMaxPendingPerSubscription: DELIVERY_MAX_PENDING_PER_SUBSCRIPTION,
    invalidBearerFallbackToKeyless: AUTH_INVALID_BEARER_FALLBACK_TO_KEYLESS,
    deliveryRequeueEnabled: DELIVERY_REQUEUE_OPENED_ENABLED,
    deliveryRequeueAfterMinutes: DELIVERY_REQUEUE_AFTER_MINUTES,
    deliveryRequeueMaxPerQuestionSubscription: DELIVERY_REQUEUE_MAX_PER_QUESTION_SUBSCRIPTION,
    deliveryRequeueLoopMs: DELIVERY_REQUEUE_LOOP_INTERVAL_MS,
    jobDiscoveryAutoSubscribe: JOB_DISCOVERY_AUTO_SUBSCRIBE,
    reminderLoopEnabled: REMINDER_LOOP_ENABLED,
    reminderLoopMs: REMINDER_LOOP_INTERVAL_MS,
    autoCloseEnabled: AUTO_CLOSE_ENABLED,
    autoCloseLoopMs: AUTO_CLOSE_LOOP_INTERVAL_MS,
    autoCloseAfterMinutes: AUTO_CLOSE_AFTER_MINUTES,
    autoCloseMinAnswerAgeMinutes: AUTO_CLOSE_MIN_ANSWER_AGE_MINUTES,
    autoCloseAfterHours: AUTO_CLOSE_AFTER_HOURS,
    autoCloseMinAnswerAgeHours: AUTO_CLOSE_MIN_ANSWER_AGE_HOURS,
    subscriptionPruneEnabled: SUBSCRIPTION_PRUNE_ENABLED,
    subscriptionPruneIntervalMs: SUBSCRIPTION_PRUNE_INTERVAL_MS,
    subscriptionPruneWindowMinutes: SUBSCRIPTION_PRUNE_WINDOW_MINUTES,
    subscriptionPruneStaleMinutes: SUBSCRIPTION_PRUNE_STALE_MINUTES,
    subscriptionPruneMinAgeMinutes: SUBSCRIPTION_PRUNE_MIN_AGE_MINUTES,
    subscriptionPruneWindowHours: SUBSCRIPTION_PRUNE_WINDOW_HOURS,
    subscriptionPruneStaleHours: SUBSCRIPTION_PRUNE_STALE_HOURS,
    subscriptionPruneMinAgeHours: SUBSCRIPTION_PRUNE_MIN_AGE_HOURS,
    tractionScorecardDays: TRACTION_SCORECARD_DAYS,
    tractionAlertLoopEnabled: TRACTION_ALERT_LOOP_ENABLED,
    tractionAlertLoopIntervalMs: TRACTION_ALERT_LOOP_INTERVAL_MS,
    tractionAlertWebhookConfigured: TRACTION_ALERT_WEBHOOK_URL.length > 0,
    sourceSeedLoopEnabled: IMPORT_SEED_LOOP_ENABLED,
    sourceSeedLoopIntervalMs: IMPORT_SEED_LOOP_INTERVAL_MS,
    sourceSeedGithubRepos: IMPORT_SEED_GITHUB_REPOS.length,
    sourceSeedDiscordRepos: IMPORT_SEED_DISCORD_REPOS.length,
    sourceSeedStackOverflowTags: IMPORT_SEED_STACKOVERFLOW_TAGS.length,
    sourceSeedDryRun: IMPORT_SEED_DRY_RUN,
    nextJobGuardrailEnabled: NEXT_JOB_GUARDRAIL_ENABLED,
    nextJobGuardrailIntervalMs: NEXT_JOB_GUARDRAIL_INTERVAL_MS,
    nextJobGuardrailWindowMinutes: NEXT_JOB_GUARDRAIL_WINDOW_MINUTES,
    nextJobGuardrailMinStrictWrites: NEXT_JOB_GUARDRAIL_MIN_STRICT_WRITES,
    nextJobGuardrailTriggerAnswerRate: NEXT_JOB_GUARDRAIL_TRIGGER_ANSWER_RATE,
    nextJobGuardrailRecoverAnswerRate: NEXT_JOB_GUARDRAIL_RECOVER_ANSWER_RATE,
    nextJobGuardrailEasyMinSolvability: NEXT_JOB_GUARDRAIL_EASY_MIN_SOLVABILITY,
    pushSolvabilityFilterEnabled: PUSH_SOLVABILITY_FILTER_ENABLED,
    pushSolvabilityMinScore: PUSH_SOLVABILITY_MIN_SCORE,
    pushSolvabilityUnscopedMinScore: PUSH_SOLVABILITY_UNSCOPED_MIN_SCORE,
    nextBestJobMinSolvability: NEXT_BEST_JOB_MIN_SOLVABILITY
  }, 'background workers started');
}

async function stopBackgroundWorkers() {
  if (usageFlushTimer) {
    clearInterval(usageFlushTimer);
    usageFlushTimer = null;
  }
  if (deliveryLoopTimer) {
    clearInterval(deliveryLoopTimer);
    deliveryLoopTimer = null;
  }
  if (deliveryRequeueLoopTimer) {
    clearInterval(deliveryRequeueLoopTimer);
    deliveryRequeueLoopTimer = null;
  }
  if (reminderLoopTimer) {
    clearInterval(reminderLoopTimer);
    reminderLoopTimer = null;
  }
  if (autoCloseLoopTimer) {
    clearInterval(autoCloseLoopTimer);
    autoCloseLoopTimer = null;
  }
  if (subscriptionPruneLoopTimer) {
    clearInterval(subscriptionPruneLoopTimer);
    subscriptionPruneLoopTimer = null;
  }
  if (tractionAlertLoopTimer) {
    clearInterval(tractionAlertLoopTimer);
    tractionAlertLoopTimer = null;
  }
  if (sourceImportLoopTimer) {
    clearInterval(sourceImportLoopTimer);
    sourceImportLoopTimer = null;
  }
  if (nextJobGuardrailLoopTimer) {
    clearInterval(nextJobGuardrailLoopTimer);
    nextJobGuardrailLoopTimer = null;
  }
  nextJobGuardrailLoopRunning = false;

  if (usageEventFlushPromise) {
    try {
      await usageEventFlushPromise;
    } catch {
      // swallow during shutdown
    }
  }
  if (usageEventBuffer.length > 0) {
    try {
      await withPrismaPoolRetry('usage_event_flush_shutdown', () => flushUsageEventBuffer(usageEventBuffer.length), 2);
    } catch {
      // swallow during shutdown
    }
  }
}

async function acceptAnswerForQuestion(input: {
  questionId: string;
  answerId: string;
  ownerUserId: string;
  acceptedByAgentName: string | null;
  baseUrl: string;
}) {
  const question = await prisma.question.findUnique({
    where: { id: input.questionId },
    include: {
      resolution: true,
      bounty: true,
      tags: { include: { tag: true } }
    }
  });
  if (!question) return { status: 404, payload: { error: 'Question not found' } };
  if (question.userId !== input.ownerUserId) {
    return { status: 403, payload: { error: 'Only the question owner can accept an answer.' } };
  }

  const target = await prisma.answer.findFirst({
    where: { id: input.answerId, questionId: input.questionId },
    select: { id: true, agentName: true, userId: true, createdAt: true, bodyText: true }
  });
  if (!target) return { status: 404, payload: { error: 'Answer not found for this question.' } };

  if (question.resolution?.answerId === target.id) {
    return {
      status: 200,
      payload: {
        ok: true,
        questionId: input.questionId,
        acceptedAnswerId: target.id,
        changed: false
      }
    };
  }

  const prevAnswerId = question.resolution?.answerId ?? null;
  let previousAgentName: string | null = null;
  if (prevAnswerId) {
    const prevAnswer = await prisma.answer.findUnique({
      where: { id: prevAnswerId },
      select: { agentName: true }
    });
    previousAgentName = normalizeAgentOrNull(prevAnswer?.agentName ?? null);
  }
  const targetAgentName = normalizeAgentOrNull(target.agentName);
  const acceptedAt = new Date();
  const bountyAmount = getActiveBountyAmount(question.bounty);
  const isSelfAccept = target.userId === question.userId;
  const shouldPayoutBounty = bountyAmount > 0 && !isSelfAccept;
  const payoutReason = shouldPayoutBounty
    ? 'payout_applied'
    : (isSelfAccept ? 'self_accept_no_payout' : 'no_active_bounty');
  let starterBonusPaid = 0;

  await prisma.$transaction(async (tx) => {
    await tx.questionClaim.updateMany({
      where: {
        questionId: input.questionId,
        state: { in: ['claimed', 'answered'] },
        expiresAt: { lt: acceptedAt }
      },
      data: {
        state: 'expired',
        releasedAt: acceptedAt,
        verifyReason: 'claim_ttl_elapsed'
      }
    });

    await tx.questionResolution.upsert({
      where: { questionId: input.questionId },
      create: {
        questionId: input.questionId,
        answerId: target.id,
        acceptedByAgentName: input.acceptedByAgentName
      },
      update: {
        answerId: target.id,
        acceptedByAgentName: input.acceptedByAgentName
      }
    });

    if (previousAgentName && previousAgentName !== targetAgentName) {
      await tx.agentProfile.upsert({
        where: { name: previousAgentName },
        update: {
          reputation: { decrement: ACCEPT_REPUTATION_REWARD },
          acceptedCount: { decrement: 1 }
        },
        create: {
          name: previousAgentName,
          reputation: -ACCEPT_REPUTATION_REWARD,
          acceptedCount: -1
        }
      });
    }

    let targetAcceptedBefore = false;
    if (targetAgentName) {
      const profileBefore = await tx.agentProfile.findUnique({
        where: { name: targetAgentName },
        select: { acceptedCount: true }
      });
      targetAcceptedBefore = (profileBefore?.acceptedCount ?? 0) > 0;
      await tx.agentProfile.upsert({
        where: { name: targetAgentName },
        update: {
          reputation: { increment: ACCEPT_REPUTATION_REWARD },
          acceptedCount: { increment: 1 }
        },
        create: {
          name: targetAgentName,
          reputation: ACCEPT_REPUTATION_REWARD,
          acceptedCount: 1
        }
      });

      if (STARTER_BONUS_CREDITS > 0 && !targetAcceptedBefore) {
        const priorStarter = await tx.agentCreditLedger.findFirst({
          where: {
            agentName: targetAgentName,
            reason: 'starter_bonus_first_accepted'
          },
          select: { id: true }
        });
        if (!priorStarter) {
          starterBonusPaid = STARTER_BONUS_CREDITS;
          await tx.agentProfile.upsert({
            where: { name: targetAgentName },
            update: { credits: { increment: STARTER_BONUS_CREDITS } },
            create: { name: targetAgentName, credits: STARTER_BONUS_CREDITS }
          });
          await tx.agentCreditLedger.create({
            data: {
              agentName: targetAgentName,
              delta: STARTER_BONUS_CREDITS,
              reason: 'starter_bonus_first_accepted',
              questionId: input.questionId,
              answerId: target.id
            }
          });
        }
      }
    }

    if (shouldPayoutBounty) {
      if (targetAgentName) {
        await tx.agentProfile.upsert({
          where: { name: targetAgentName },
          update: { credits: { increment: bountyAmount } },
          create: { name: targetAgentName, credits: bountyAmount }
        });
        await tx.agentCreditLedger.create({
          data: {
            agentName: targetAgentName,
            delta: bountyAmount,
            reason: 'bounty_payout',
            questionId: input.questionId,
            answerId: target.id
          }
        });
      }
      if (question.bounty) {
        await tx.questionBounty.update({
          where: { questionId: input.questionId },
          data: { active: false }
        });
      }
    }

    if (targetAgentName) {
      const targetClaim = await tx.questionClaim.findFirst({
        where: {
          questionId: input.questionId,
          agentName: targetAgentName,
          state: { in: ['claimed', 'answered', 'verified'] },
          OR: [{ answerId: target.id }, { answerId: null }]
        },
        orderBy: { createdAt: 'desc' }
      });
      if (targetClaim) {
        await tx.questionClaim.update({
          where: { id: targetClaim.id },
          data: {
            state: 'verified',
            answerId: target.id,
            answeredAt: targetClaim.answeredAt ?? acceptedAt,
            verifiedAt: acceptedAt,
            expiresAt: acceptedAt,
            verifiedByAgent: input.acceptedByAgentName,
            verifyReason: shouldPayoutBounty ? 'accepted_with_bounty' : payoutReason
          }
        });
      } else {
        await tx.questionClaim.create({
          data: {
            questionId: input.questionId,
            agentName: targetAgentName,
            state: 'verified',
            expiresAt: acceptedAt,
            answerId: target.id,
            answeredAt: acceptedAt,
            verifiedAt: acceptedAt,
            verifiedByAgent: input.acceptedByAgentName,
            verifyReason: shouldPayoutBounty ? 'accepted_with_bounty' : payoutReason
          }
        });
      }
    }

    const competingClaimWhere: Prisma.QuestionClaimWhereInput = {
      questionId: input.questionId,
      state: { in: ['claimed', 'answered'] }
    };
    if (targetAgentName) {
      competingClaimWhere.agentName = { not: targetAgentName };
    }
    await tx.questionClaim.updateMany({
      where: competingClaimWhere,
      data: {
        state: 'released',
        releasedAt: acceptedAt,
        verifyReason: 'accepted_elsewhere'
      }
    });
  });

  void dispatchQuestionAcceptedEvent({
    id: question.id,
    title: question.title,
    bodyText: question.bodyText,
    createdAt: question.createdAt,
    tags: question.tags.map((link) => link.tag.name),
    url: `${input.baseUrl}/q/${question.id}`,
    acceptedAt,
    acceptedAnswerId: target.id,
    acceptedAgentName: targetAgentName,
    acceptedByAgentName: input.acceptedByAgentName,
    bountyPaid: shouldPayoutBounty ? bountyAmount : 0,
    starterBonusPaid,
    source: getQuestionSource(question)
  }).catch(() => undefined);

  void dispatchSourceResolutionCallback({
    questionId: question.id,
    questionTitle: question.title,
    questionUrl: `${input.baseUrl}/q/${question.id}`,
    sourceType: question.sourceType ?? null,
    sourceUrl: question.sourceUrl ?? null,
    acceptedAt,
    acceptedAnswerId: target.id,
    acceptedAgentName: targetAgentName,
    answerBodyText: target.bodyText ?? null
  }).catch(() => undefined);

  return {
    status: 200,
    payload: {
      ok: true,
      changed: true,
      questionId: input.questionId,
      acceptedAnswerId: target.id,
      previousAnswerId: prevAnswerId,
      bountyPaid: shouldPayoutBounty ? bountyAmount : 0,
      payout: {
        eligible: bountyAmount > 0,
        applied: shouldPayoutBounty,
        amount: shouldPayoutBounty ? bountyAmount : 0,
        currency: question.bounty?.currency ?? 'credits',
        reason: payoutReason
      },
      starterBonus: {
        eligible: Boolean(targetAgentName) && STARTER_BONUS_CREDITS > 0,
        applied: starterBonusPaid > 0,
        amount: starterBonusPaid
      },
      completion: {
        verified: Boolean(targetAgentName),
        acceptedAt: acceptedAt.toISOString(),
        acceptedByAgentName: input.acceptedByAgentName
      }
    }
  };
}

fastify.addHook('onRequest', async (request, reply) => {
  (request as { startTimeNs?: bigint }).startTimeNs = process.hrtime.bigint();
  if (request.method === 'GET') {
    const rawUrl = request.raw.url ?? request.url;
    if (rawUrl) {
      const canonical = getCanonicalWellKnownPath(rawUrl);
      if (canonical && stripQuery(rawUrl) !== canonical) {
        const queryIndex = rawUrl.indexOf('?');
        const query = queryIndex === -1 ? '' : rawUrl.slice(queryIndex);
        reply.redirect(`${canonical}${query}`, 301);
        return;
      }
    }
  }
});

fastify.addHook('preHandler', async (request) => {
  if (!CAPTURE_AGENT_PAYLOADS) return;
  const route = resolveRoute(request as RouteRequest);
  if (!CAPTURED_ROUTES.has(route)) return;
  const userAgent = normalizeHeader(request.headers['user-agent']).slice(0, 256) || null;
  const agentName = getAgentName(request.headers);
  if (!isAgentTraffic(agentName, userAgent)) return;
  const payload = buildRequestPayload(request as { body?: unknown; query?: unknown; params?: unknown });
  (request as { payloadCapture?: { requestBody?: unknown; responseBody?: unknown; route?: string } }).payloadCapture = {
    requestBody: payload,
    route
  };
});

fastify.addHook('onSend', async (request, reply, payload) => {
  if (!CAPTURE_AGENT_PAYLOADS) return payload;
  const capture = (request as { payloadCapture?: { requestBody?: unknown; responseBody?: unknown } }).payloadCapture;
  if (capture) {
    capture.responseBody = payload;
  }
  return payload;
});

fastify.addHook('onResponse', async (request, reply) => {
  if (request.method === 'OPTIONS') return;
  const startNs = (request as { startTimeNs?: bigint }).startTimeNs;
  const durationMs = startNs ? Math.max(0, Number(process.hrtime.bigint() - startNs) / 1_000_000) : 0;
  const route = resolveRoute(request as RouteRequest);
  const logNoise = normalizeHeader(process.env.LOG_NOISE) === 'true';
  if (!logNoise && isNoiseEvent({ method: request.method, route, status: reply.statusCode })) {
    return;
  }
  const authMeta = getRequestAuthMeta(request);
  const apiKeyPrefix = authMeta ? buildUsageApiKeyPrefix(authMeta) : extractApiKeyPrefix(request.headers);
  const userAgent = normalizeHeader(request.headers['user-agent']).slice(0, 256) || null;
  const ip = getClientIp(request as RouteRequest & { ip?: string; socket?: { remoteAddress?: string } });
  const referer = normalizeHeader(request.headers.referer).slice(0, 512) || null;
  const agentName = getAgentName(request.headers) ?? authMeta?.boundAgentName ?? null;

  enqueueUsageEvent({
    method: request.method,
    route,
    status: reply.statusCode,
    durationMs: Math.round(durationMs),
    apiKeyPrefix,
    userAgent,
    ip,
    referer,
    agentName
  });
  if (usageEventBuffer.length >= USAGE_LOG_FLUSH_BATCH_SIZE) {
    void flushUsageEventBuffer().catch((err) => {
      request.log.warn({ err }, 'usage event flush failed');
    });
  }

  const capture = (request as { payloadCapture?: { requestBody?: unknown; responseBody?: unknown; route?: string } }).payloadCapture;
  if (capture) {
    try {
      await storeAgentPayloadEvent({
        source: 'api',
        kind: request.method === 'GET' ? 'rest_read' : 'rest_write',
        method: request.method,
        route: capture.route ?? route,
        status: reply.statusCode,
        durationMs: Math.round(durationMs),
        requestId: request.id,
        agentName,
        userAgent,
        ip,
        apiKeyPrefix,
        requestBody: capture.requestBody,
        responseBody: capture.responseBody
      });
    } catch (err) {
      request.log.warn({ err }, 'agent payload logging failed');
    }
  }
});

fastify.get('/api/openapi.json', async () => {
  return fastify.swagger();
});

fastify.get('/.well-known/agent.json', async (request) => {
  return agentCard(getBaseUrl(request));
});

fastify.get('/.well-known/agent-card.json', async (request) => {
  return agentCard(getBaseUrl(request));
});

fastify.post('/api/v1/a2a', {
  schema: {
    tags: ['a2a'],
    body: {
      oneOf: [
        {
          type: 'object',
          properties: {
            jsonrpc: { type: 'string' },
            id: {},
            method: { type: 'string' },
            params: {}
          },
          required: ['jsonrpc', 'method']
        },
        {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              jsonrpc: { type: 'string' },
              id: {},
              method: { type: 'string' },
              params: {}
            },
            required: ['jsonrpc', 'method']
          }
        }
      ]
    }
  }
}, async (request, reply) => {
  const processOne = async (input: unknown) => {
    if (!isJsonObject(input)) {
      return makeJsonRpcError(null, -32600, 'Invalid Request', { detail: 'Expected object request.' });
    }
    const id = input.id ?? null;
    if (input.jsonrpc !== '2.0') {
      return makeJsonRpcError(id, -32600, 'Invalid Request', { detail: 'jsonrpc must be "2.0".' });
    }
    const method = normalizeHeader(typeof input.method === 'string' ? input.method : '');
    if (!method) {
      return makeJsonRpcError(id, -32600, 'Invalid Request', { detail: 'method is required.' });
    }

    if (method === 'sendMessage' || method === 'sendStreamingMessage') {
      const { action, args, messageText } = resolveA2aActionInput(input.params);
      if (!action) {
        return makeJsonRpcError(id, -32602, 'Invalid params', {
          detail: 'sendMessage requires action/skill/tool or a text message.',
          methods: ['sendMessage', 'sendStreamingMessage'],
          supportedActions: Array.from(A2A_ACTIONS)
        });
      }
      if (!A2A_ACTIONS.has(action)) {
        return makeJsonRpcError(id, -32602, 'Invalid params', {
          detail: `Unsupported action "${action}"`,
          supportedActions: Array.from(A2A_ACTIONS)
        });
      }

      const agentName = normalizeAgentOrNull(
        firstString(
          isJsonObject(input.params) ? input.params.agentName : null,
          getAgentName(request.headers)
        )
      );
      const task = newA2aTask(method, action, {
        args,
        messageText: messageText || null
      });
      appendA2aTaskEvent(task, 'task.created', { status: task.status, action });
      task.status = 'working';
      appendA2aTaskEvent(task, 'task.started', { status: task.status, action });

      try {
        const result = await runA2aActionViaInject(request, action, args, agentName);
        if (!result.ok) {
          task.error = {
            code: 'action_failed',
            message: isJsonObject(result.payload) && typeof result.payload.error === 'string'
              ? result.payload.error
              : `Action failed with status ${result.statusCode}`,
            status: result.statusCode
          };
          task.output = {
            ok: false,
            route: result.route,
            method: result.method,
            statusCode: result.statusCode,
            payload: result.payload
          };
          markA2aTaskTerminal(task, 'failed');
          appendA2aTaskEvent(task, 'task.failed', task.error);
        } else {
          task.output = {
            ok: true,
            route: result.route,
            method: result.method,
            statusCode: result.statusCode,
            payload: result.payload
          };
          markA2aTaskTerminal(task, 'completed');
          appendA2aTaskEvent(task, 'task.completed', { statusCode: result.statusCode });
        }
      } catch (err) {
        task.error = {
          code: 'internal_error',
          message: err instanceof Error ? err.message : 'Unknown error while executing action',
          status: null
        };
        task.output = {
          ok: false,
          payload: null
        };
        markA2aTaskTerminal(task, 'failed');
        appendA2aTaskEvent(task, 'task.failed', task.error);
      }

      const baseUrl = getBaseUrl(request);
      return makeJsonRpcResponse(id, {
        task: serializeA2aTask(task, baseUrl),
        stream: method === 'sendStreamingMessage'
          ? {
              events: `${baseUrl}/api/v1/a2a/tasks/${task.id}/events`
            }
          : undefined
      });
    }

    if (method === 'getTask') {
      const params = isJsonObject(input.params) ? input.params : {};
      const taskId = firstString(params.taskId, params.id);
      if (!taskId) {
        return makeJsonRpcError(id, -32602, 'Invalid params', { detail: 'getTask requires taskId or id.' });
      }
      const task = getA2aTask(taskId);
      if (!task) {
        return makeJsonRpcError(id, -32004, 'Task not found', { taskId });
      }
      return makeJsonRpcResponse(id, {
        task: serializeA2aTask(task, getBaseUrl(request))
      });
    }

    if (method === 'cancelTask') {
      const params = isJsonObject(input.params) ? input.params : {};
      const taskId = firstString(params.taskId, params.id);
      if (!taskId) {
        return makeJsonRpcError(id, -32602, 'Invalid params', { detail: 'cancelTask requires taskId or id.' });
      }
      const task = getA2aTask(taskId);
      if (!task) {
        return makeJsonRpcError(id, -32004, 'Task not found', { taskId });
      }
      if (!isTerminalA2aStatus(task.status)) {
        task.canceledAtMs = Date.now();
        task.error = {
          code: 'task_canceled',
          message: 'Task canceled by client.',
          status: null
        };
        markA2aTaskTerminal(task, 'canceled');
        appendA2aTaskEvent(task, 'task.canceled');
      }
      return makeJsonRpcResponse(id, {
        task: serializeA2aTask(task, getBaseUrl(request))
      });
    }

    return makeJsonRpcError(id, -32601, 'Method not found', {
      method,
      supportedMethods: ['sendMessage', 'sendStreamingMessage', 'getTask', 'cancelTask']
    });
  };

  if (Array.isArray(request.body)) {
    if (request.body.length === 0) {
      reply.code(400).send(makeJsonRpcError(null, -32600, 'Invalid Request', { detail: 'Batch cannot be empty.' }));
      return;
    }
    const responses = await Promise.all(request.body.map((entry) => processOne(entry)));
    reply.code(200).send(responses);
    return;
  }

  const response = await processOne(request.body);
  reply.code(200).send(response);
});

fastify.get('/api/v1/a2a/tasks/:id/events', {
  schema: {
    tags: ['a2a'],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    }
  }
}, async (request, reply) => {
  const { id } = request.params as { id: string };
  const task = getA2aTask(id);
  if (!task) {
    reply.code(404).send({ error: 'Task not found' });
    return;
  }

  const baseUrl = getBaseUrl(request);
  reply.hijack();
  reply.raw.setHeader('Content-Type', 'text/event-stream');
  reply.raw.setHeader('Cache-Control', 'no-cache, no-transform');
  reply.raw.setHeader('Connection', 'keep-alive');

  let cursor = 0;
  const pushEvent = (event: string, data: unknown, eventId?: string) => {
    if (eventId) reply.raw.write(`id: ${eventId}\n`);
    reply.raw.write(`event: ${event}\n`);
    reply.raw.write(`data: ${JSON.stringify(data)}\n\n`);
  };
  const flushFromCursor = () => {
    const current = getA2aTask(id);
    if (!current) return null;
    while (cursor < current.events.length) {
      const event = current.events[cursor];
      cursor += 1;
      pushEvent(event.type, event, event.id);
    }
    return current;
  };

  pushEvent('task.snapshot', { task: serializeA2aTask(task, baseUrl) });
  flushFromCursor();

  const interval = setInterval(() => {
    const current = flushFromCursor();
    if (!current) {
      pushEvent('task.missing', { id });
      clearInterval(interval);
      reply.raw.end();
      return;
    }
    if (isTerminalA2aStatus(current.status) && cursor >= current.events.length) {
      pushEvent('task.done', { id: current.id, status: current.status });
      clearInterval(interval);
      reply.raw.end();
      return;
    }
    reply.raw.write(': heartbeat\n\n');
  }, 1000);

  request.raw.on('close', () => {
    clearInterval(interval);
  });
});

fastify.get('/api/v1/health', {
  schema: {
    tags: ['health'],
    response: {
      200: {
        type: 'object',
        properties: {
          ok: { type: 'boolean' }
        }
      }
    }
  }
}, async () => ({ ok: true }));

fastify.get('/robots.txt', async (request, reply) => {
  const baseUrl = PUBLIC_BASE_URL || (process.env.NODE_ENV === 'production'
    ? 'https://a2abench-api.web.app'
    : getBaseUrl(request));
  const lines = [
    'User-agent: *',
    'Disallow: /admin/',
    'Disallow: /api/v1/admin',
    'Disallow: /api/v1/usage',
    'Disallow: /docs/',
    'Allow: /q/',
    'Allow: /.well-known/',
    `Sitemap: ${baseUrl}/sitemap.xml`
  ];
  reply.type('text/plain').send(lines.join('\n'));
});

fastify.get('/sitemap.xml', {
  schema: {
    tags: ['meta'],
    response: {
      200: { type: 'string' }
    }
  }
}, async (request, reply) => {
  const baseUrl = PUBLIC_BASE_URL || (process.env.NODE_ENV === 'production'
    ? 'https://a2abench-api.web.app'
    : getBaseUrl(request));
  try {
    const seedIds = ['seed_v2_01', 'seed_v2_02', 'seed_v2_03', 'seed_v2_04', 'seed_v2_05', 'seed_v2_06'];
    const urls = [
      `${baseUrl}/.well-known/agent.json`,
      `${baseUrl}/api/openapi.json`,
      `${baseUrl}/leaderboard/weekly`,
      `${baseUrl}/q/demo_q1`,
      ...seedIds.map((id) => `${baseUrl}/q/${id}`)
    ];
    const body = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.map((loc) => `  <url><loc>${loc}</loc></url>`).join('\n')}
</urlset>`;
    reply.type('application/xml').send(body);
  } catch (err) {
    request.log.warn({ err }, 'sitemap generation failed');
    const fallback = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>${baseUrl}/.well-known/agent.json</loc></url>
  <url><loc>${baseUrl}/api/openapi.json</loc></url>
  <url><loc>${baseUrl}/leaderboard/weekly</loc></url>
</urlset>`;
    reply.type('application/xml').send(fallback);
  }
});

fastify.get('/api/v1/usage/summary', {
  schema: {
    tags: ['usage', 'admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        days: { type: 'integer', minimum: 1, maximum: 90 },
        includeNoise: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { days?: number; includeNoise?: boolean };
  const days = Math.min(90, Math.max(1, Number(query.days ?? 7)));
  return getUsageSummaryCached(days, Boolean(query.includeNoise));
});

async function getUsageSummary(days: number, includeNoise: boolean) {
  const now = new Date();
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  const previousSince = new Date(since.getTime() - days * 24 * 60 * 60 * 1000);
  const last24h = new Date(Date.now() - 24 * 60 * 60 * 1000);
  const last7d = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
  const previous7d = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);
  const noiseWhere: Prisma.UsageEventWhereInput = {
    OR: [
      { route: '/api/v1/auth/trial-key', method: { in: ['GET', 'HEAD'] }, status: 405 },
      { route: '/api/v1/questions/:id/answers', method: { in: ['GET', 'HEAD'] }, status: 405 },
      { route: '/q/:id', method: { in: ['GET', 'HEAD'] }, status: 400 },
      { route: '/api/v1/questions/:id', method: { in: ['GET', 'HEAD'] }, status: 400 },
      { route: '/', method: { in: ['GET', 'HEAD'] }, status: 404 },
      { route: '/api/v1/fetch', method: { in: ['GET', 'HEAD'] }, status: 404 },
      { route: '/docs/.well-known/agent.json', method: { in: ['GET', 'HEAD'] }, status: 404 },
      {
        AND: [
          { route: { endsWith: '/.well-known/agent.json' } },
          { route: { not: '/.well-known/agent.json' } },
          { method: { in: ['GET', 'HEAD'] } },
          { status: { in: [301, 308, 404] } }
        ]
      },
      {
        AND: [
          { route: { endsWith: '/.well-known/agent-card.json' } },
          { route: { not: '/.well-known/agent-card.json' } },
          { method: { in: ['GET', 'HEAD'] } },
          { status: { in: [301, 308, 404] } }
        ]
      },
      { route: '/api/v1/usage/summary', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } },
      { route: '/admin/usage', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } },
      { route: '/admin/usage/data', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } },
      { route: '/admin/agent-events', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } },
      { route: '/admin/agent-events/data', method: { in: ['GET', 'HEAD'] }, status: { in: [401, 403] } }
    ]
  };
  const usageWhere = includeNoise
    ? { createdAt: { gte: since } }
    : { AND: [{ createdAt: { gte: since } }, { NOT: noiseWhere }] };
  const last24hWhere = includeNoise
    ? { createdAt: { gte: last24h } }
    : { AND: [{ createdAt: { gte: last24h } }, { NOT: noiseWhere }] };
  const noiseSql = includeNoise
    ? Prisma.empty
    : Prisma.sql`
      AND NOT (
        ("route" = '/api/v1/auth/trial-key' AND "method" IN ('GET','HEAD') AND "status" = 405)
        OR ("route" = '/api/v1/questions/:id/answers' AND "method" IN ('GET','HEAD') AND "status" = 405)
        OR ("route" = '/q/:id' AND "method" IN ('GET','HEAD') AND "status" = 400)
        OR ("route" = '/api/v1/questions/:id' AND "method" IN ('GET','HEAD') AND "status" = 400)
        OR ("route" = '/' AND "method" IN ('GET','HEAD') AND "status" = 404)
        OR ("route" = '/api/v1/fetch' AND "method" IN ('GET','HEAD') AND "status" = 404)
        OR ("route" = '/docs/.well-known/agent.json' AND "method" IN ('GET','HEAD') AND "status" = 404)
        OR ("route" LIKE '%/.well-known/agent.json' AND "route" <> '/.well-known/agent.json' AND "method" IN ('GET','HEAD') AND "status" IN (301,308,404))
        OR ("route" LIKE '%/.well-known/agent-card.json' AND "route" <> '/.well-known/agent-card.json' AND "method" IN ('GET','HEAD') AND "status" IN (301,308,404))
        OR ("route" = '/api/v1/usage/summary' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
        OR ("route" = '/admin/usage' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
        OR ("route" = '/admin/usage/data' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
        OR ("route" = '/admin/agent-events' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
        OR ("route" = '/admin/agent-events/data' AND "method" IN ('GET','HEAD') AND "status" IN (401,403))
      )
    `;

  const total = await prisma.usageEvent.count({ where: usageWhere });
  const lastDay = await prisma.usageEvent.count({ where: last24hWhere });
  const byRoute = await prisma.usageEvent.groupBy({
    by: ['route'],
    where: usageWhere,
    _count: { route: true },
    orderBy: { _count: { route: 'desc' } },
    take: 10
  });
  const byStatus = await prisma.usageEvent.groupBy({
    by: ['status'],
    where: usageWhere,
    _count: { status: true },
    orderBy: { _count: { status: 'desc' } }
  });
  const dailyRows = await prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
    SELECT date_trunc('day', "createdAt") AS day, COUNT(*) AS count
    FROM "UsageEvent"
    WHERE "createdAt" >= ${since}
    ${noiseSql}
    GROUP BY 1
    ORDER BY day ASC
  `;
  const totalQuestions = await prisma.question.count();
  const totalAnswers = await prisma.answer.count();
  const questionsInRange = await prisma.question.count({ where: { createdAt: { gte: since } } });
  const answersInRange = await prisma.answer.count({ where: { createdAt: { gte: since } } });
  const previousQuestionsInRange = await prisma.question.count({ where: { createdAt: { gte: previousSince, lt: since } } });
  const previousAnswersInRange = await prisma.answer.count({ where: { createdAt: { gte: previousSince, lt: since } } });
  const questionsAnsweredInRange = await prisma.question.count({ where: { createdAt: { gte: since }, answers: { some: {} } } });
  const questionsAcceptedInRange = await prisma.questionResolution.count({ where: { question: { createdAt: { gte: since } } } });
  const unansweredTotal = await prisma.question.count({ where: { answers: { none: {} } } });
  const uniqueAskersInRangeRows = await prisma.$queryRaw<Array<{ count: bigint | number | string }>>`
    SELECT COUNT(DISTINCT "userId") AS count
    FROM "Question"
    WHERE "createdAt" >= ${since}
  `;
  const uniqueAskersPreviousRangeRows = await prisma.$queryRaw<Array<{ count: bigint | number | string }>>`
    SELECT COUNT(DISTINCT "userId") AS count
    FROM "Question"
    WHERE "createdAt" >= ${previousSince} AND "createdAt" < ${since}
  `;
  const uniqueAnswerersInRangeRows = await prisma.$queryRaw<Array<{ count: bigint | number | string }>>`
    SELECT COUNT(DISTINCT COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId"))) AS count
    FROM "Answer"
    WHERE "createdAt" >= ${since}
  `;
  const uniqueAnswerersPreviousRangeRows = await prisma.$queryRaw<Array<{ count: bigint | number | string }>>`
    SELECT COUNT(DISTINCT COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId"))) AS count
    FROM "Answer"
    WHERE "createdAt" >= ${previousSince} AND "createdAt" < ${since}
  `;
  const externalActorTypes = Array.from(new Set(
    Array.from(EXTERNAL_TRACTION_ACTOR_TYPES)
      .map((value) => normalizeActorType(value))
      .filter((value): value is ActorType => value !== 'unknown')
  ));
  const externalKeyRows = await prisma.apiKey.findMany({
    select: { userId: true, name: true }
  });
  const externalUserIdsAllSet = new Set<string>();
  const externalBoundAgentsAllSet = new Set<string>();
  const externalUserIdsRealSet = new Set<string>();
  const externalBoundAgentsRealSet = new Set<string>();
  const externalUserIdsSyntheticSet = new Set<string>();
  const externalUserIdsExcludedSet = new Set<string>();
  const externalBoundAgentsSyntheticSet = new Set<string>();
  const externalBoundAgentsExcludedSet = new Set<string>();
  for (const row of externalKeyRows) {
    const meta = parseApiKeyIdentityMeta(row.name);
    if (!externalActorTypes.includes(meta.actorType)) continue;
    const normalizedBound = normalizeAgentOrNull(meta.boundAgentName);
    externalUserIdsAllSet.add(row.userId);
    if (normalizedBound) externalBoundAgentsAllSet.add(normalizedBound);
    if (isSyntheticAgentName(normalizedBound) && normalizedBound) {
      externalBoundAgentsSyntheticSet.add(normalizedBound);
    }
    if (isExcludedExternalAgentName(normalizedBound)) {
      externalUserIdsExcludedSet.add(row.userId);
      if (normalizedBound) externalBoundAgentsExcludedSet.add(normalizedBound);
      continue;
    }
    if (isSyntheticAgentName(normalizedBound)) {
      externalUserIdsSyntheticSet.add(row.userId);
      continue;
    }
    externalUserIdsRealSet.add(row.userId);
    if (normalizedBound) externalBoundAgentsRealSet.add(normalizedBound);
  }
  const externalUserIds = Array.from(externalUserIdsRealSet);
  const externalBoundAgents = Array.from(externalBoundAgentsRealSet);
  const externalUserIdsAll = Array.from(externalUserIdsAllSet);
  const externalBoundAgentsAll = Array.from(externalBoundAgentsAllSet);
  const externalUserIdsSynthetic = Array.from(externalUserIdsSyntheticSet);
  const externalUserIdsExcluded = Array.from(externalUserIdsExcludedSet);
  const externalBoundAgentsSynthetic = Array.from(externalBoundAgentsSyntheticSet);
  const externalBoundAgentsExcluded = Array.from(externalBoundAgentsExcludedSet);
  const externalAnswerScopeOr: Prisma.AnswerWhereInput[] = [];
  if (externalBoundAgents.length > 0) {
    externalAnswerScopeOr.push({ agentName: { in: externalBoundAgents } });
  }
  if (externalUserIds.length > 0) {
    externalAnswerScopeOr.push({ userId: { in: externalUserIds } });
  }
  const externalAnswerScopeWhere = externalAnswerScopeOr.length > 0
    ? { OR: externalAnswerScopeOr }
    : null;
  const externalQuestionsInRange = externalUserIds.length > 0
    ? await prisma.question.count({ where: { createdAt: { gte: since }, userId: { in: externalUserIds } } })
    : 0;
  const externalQuestionsLast24h = externalUserIds.length > 0
    ? await prisma.question.count({ where: { createdAt: { gte: last24h }, userId: { in: externalUserIds } } })
    : 0;
  const externalAnswersInRange = externalAnswerScopeWhere
    ? await prisma.answer.count({ where: { createdAt: { gte: since }, ...externalAnswerScopeWhere } })
    : 0;
  const externalAnswersLast24h = externalAnswerScopeWhere
    ? await prisma.answer.count({ where: { createdAt: { gte: last24h }, ...externalAnswerScopeWhere } })
    : 0;
  const externalAcceptedInRange = externalAnswerScopeWhere
    ? await prisma.questionResolution.count({
        where: {
          createdAt: { gte: since },
          answer: externalAnswerScopeWhere
        }
      })
    : 0;
  const externalAcceptedLast24h = externalAnswerScopeWhere
    ? await prisma.questionResolution.count({
        where: {
          createdAt: { gte: last24h },
          answer: externalAnswerScopeWhere
        }
      })
    : 0;

  const externalUsageActorCondition = externalActorTypes.length > 0
    ? Prisma.sql`
      "apiKeyPrefix" IS NOT NULL
      AND POSITION('|actor=' IN "apiKeyPrefix") > 0
      AND split_part(split_part("apiKeyPrefix", '|actor=', 2), '|', 1) IN (${Prisma.join(externalActorTypes)})
    `
    : Prisma.sql`FALSE`;

  const externalRequestAgentRows = await prisma.$queryRaw<Array<{
    agentName: string | null;
    writesInRange: bigint | number | string;
    writesLast24h: bigint | number | string;
    verifiedWritesInRange: bigint | number | string;
    signedWritesInRange: bigint | number | string;
    questionWritesInRange: bigint | number | string;
    answerWritesInRange: bigint | number | string;
  }>>`
    SELECT
      NULLIF("agentName", '') AS "agentName",
      COUNT(*) FILTER (
        WHERE UPPER("method") IN ('POST', 'PUT', 'PATCH', 'DELETE')
      ) AS "writesInRange",
      COUNT(*) FILTER (
        WHERE "createdAt" >= ${last24h}
          AND UPPER("method") IN ('POST', 'PUT', 'PATCH', 'DELETE')
      ) AS "writesLast24h",
      COUNT(*) FILTER (
        WHERE UPPER("method") IN ('POST', 'PUT', 'PATCH', 'DELETE')
          AND POSITION('|idv=' IN "apiKeyPrefix") > 0
          AND split_part(split_part("apiKeyPrefix", '|idv=', 2), '|', 1) IN ('1', 'true')
      ) AS "verifiedWritesInRange",
      COUNT(*) FILTER (
        WHERE UPPER("method") IN ('POST', 'PUT', 'PATCH', 'DELETE')
          AND POSITION('|sigv=' IN "apiKeyPrefix") > 0
          AND split_part(split_part("apiKeyPrefix", '|sigv=', 2), '|', 1) IN ('1', 'true')
      ) AS "signedWritesInRange",
      COUNT(*) FILTER (
        WHERE "route" = '/api/v1/questions'
          AND "status" BETWEEN 200 AND 299
          AND UPPER("method") = 'POST'
      ) AS "questionWritesInRange",
      COUNT(*) FILTER (
        WHERE "route" IN ('/api/v1/questions/:id/answers', '/api/v1/questions/:id/answer-job')
          AND "status" BETWEEN 200 AND 299
          AND UPPER("method") = 'POST'
      ) AS "answerWritesInRange"
    FROM "UsageEvent"
    WHERE "createdAt" >= ${since}
      ${noiseSql}
      AND ${externalUsageActorCondition}
    GROUP BY 1
  `;
  const tractionAskerDailyRows = await prisma.$queryRaw<Array<{ day: Date | string; askers: bigint | number | string }>>`
    SELECT date_trunc('day', "createdAt") AS day, COUNT(DISTINCT "userId") AS askers
    FROM "Question"
    WHERE "createdAt" >= ${since}
    GROUP BY 1
    ORDER BY day ASC
  `;
  const tractionAnswererDailyRows = await prisma.$queryRaw<Array<{ day: Date | string; answerers: bigint | number | string }>>`
    SELECT
      date_trunc('day', "createdAt") AS day,
      COUNT(DISTINCT COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId"))) AS answerers
    FROM "Answer"
    WHERE "createdAt" >= ${since}
    GROUP BY 1
    ORDER BY day ASC
  `;
  const questionDailyRows = await prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
    SELECT date_trunc('day', "createdAt") AS day, COUNT(*) AS count
    FROM "Question"
    WHERE "createdAt" >= ${since}
    GROUP BY 1
    ORDER BY day ASC
  `;
  const answerDailyRows = await prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
    SELECT date_trunc('day', "createdAt") AS day, COUNT(*) AS count
    FROM "Answer"
    WHERE "createdAt" >= ${since}
    GROUP BY 1
    ORDER BY day ASC
  `;
  const answeredQuestionDailyRows = await prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
    SELECT date_trunc('day', q."createdAt") AS day, COUNT(*) AS count
    FROM "Question" q
    WHERE q."createdAt" >= ${since}
      AND EXISTS (
        SELECT 1
        FROM "Answer" a
        WHERE a."questionId" = q."id"
      )
    GROUP BY 1
    ORDER BY day ASC
  `;
  const acceptedQuestionDailyRows = await prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
    SELECT date_trunc('day', q."createdAt") AS day, COUNT(*) AS count
    FROM "QuestionResolution" r
    JOIN "Question" q ON q."id" = r."questionId"
    WHERE q."createdAt" >= ${since}
    GROUP BY 1
    ORDER BY day ASC
  `;
  const externalQuestionDailyRows = externalUserIds.length > 0
    ? await prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
      SELECT date_trunc('day', "createdAt") AS day, COUNT(*) AS count
      FROM "Question"
      WHERE "createdAt" >= ${since}
        AND "userId" IN (${Prisma.join(externalUserIds)})
      GROUP BY 1
      ORDER BY day ASC
    `
    : [];
  const externalAnswerDailyRows = externalAnswerScopeWhere
    ? await prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
      SELECT date_trunc('day', "createdAt") AS day, COUNT(*) AS count
      FROM "Answer"
      WHERE "createdAt" >= ${since}
        AND (
          ${externalBoundAgents.length > 0
            ? Prisma.sql`"agentName" IN (${Prisma.join(externalBoundAgents)})`
            : Prisma.sql`FALSE`}
          OR
          ${externalUserIds.length > 0
            ? Prisma.sql`"userId" IN (${Prisma.join(externalUserIds)})`
            : Prisma.sql`FALSE`}
        )
      GROUP BY 1
      ORDER BY day ASC
    `
    : [];
  const [externalCurrentAnswerers7dRows, externalPreviousAnswerers7dRows] = externalBoundAgents.length > 0
    ? await Promise.all([
      prisma.answer.findMany({
        where: {
          createdAt: { gte: last7d },
          agentName: { in: externalBoundAgents }
        },
        select: { agentName: true },
        distinct: ['agentName']
      }),
      prisma.answer.findMany({
        where: {
          createdAt: { gte: previous7d, lt: last7d },
          agentName: { in: externalBoundAgents }
        },
        select: { agentName: true },
        distinct: ['agentName']
      })
    ])
    : [[], []];
  const externalCurrentAnswerers7d = new Set(
    externalCurrentAnswerers7dRows
      .map((row) => normalizeAgentOrNull(row.agentName))
      .filter((value): value is string => Boolean(value))
  );
  const externalPreviousAnswerers7d = new Set(
    externalPreviousAnswerers7dRows
      .map((row) => normalizeAgentOrNull(row.agentName))
      .filter((value): value is string => Boolean(value))
  );
  const retainedExternalAnswerers7d = Array.from(externalCurrentAnswerers7d)
    .filter((agentName) => externalPreviousAnswerers7d.has(agentName));

  const qaByDay = new Map<string, { day: string; questions: number; answers: number }>();
  for (const row of questionDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = qaByDay.get(day) ?? { day, questions: 0, answers: 0 };
    existing.questions = Number(row.count);
    qaByDay.set(day, existing);
  }
  for (const row of answerDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = qaByDay.get(day) ?? { day, questions: 0, answers: 0 };
    existing.answers = Number(row.count);
    qaByDay.set(day, existing);
  }

  const startDay = new Date(Date.UTC(since.getUTCFullYear(), since.getUTCMonth(), since.getUTCDate()));
  const endDay = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const cursor = new Date(startDay);
  while (cursor <= endDay) {
    const day = cursor.toISOString().slice(0, 10);
    if (!qaByDay.has(day)) {
      qaByDay.set(day, { day, questions: 0, answers: 0 });
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }

  const qaDaily = Array.from(qaByDay.values()).sort((a, b) => a.day.localeCompare(b.day));
  const externalQaByDay = new Map<string, { day: string; questions: number; answers: number }>();
  for (const row of externalQuestionDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = externalQaByDay.get(day) ?? { day, questions: 0, answers: 0 };
    existing.questions = toNumber(row.count);
    externalQaByDay.set(day, existing);
  }
  for (const row of externalAnswerDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = externalQaByDay.get(day) ?? { day, questions: 0, answers: 0 };
    existing.answers = toNumber(row.count);
    externalQaByDay.set(day, existing);
  }
  const externalQaCursor = new Date(startDay);
  while (externalQaCursor <= endDay) {
    const day = externalQaCursor.toISOString().slice(0, 10);
    if (!externalQaByDay.has(day)) {
      externalQaByDay.set(day, { day, questions: 0, answers: 0 });
    }
    externalQaCursor.setUTCDate(externalQaCursor.getUTCDate() + 1);
  }
  const externalQaDaily = Array.from(externalQaByDay.values()).sort((a, b) => a.day.localeCompare(b.day));

  const externalRequestAgentStats = externalRequestAgentRows
    .map((row) => ({
      agentName: normalizeAgentOrNull(row.agentName) ?? 'unknown',
      writesInRange: toNumber(row.writesInRange),
      writesLast24h: toNumber(row.writesLast24h),
      verifiedWritesInRange: toNumber(row.verifiedWritesInRange),
      signedWritesInRange: toNumber(row.signedWritesInRange),
      questionWritesInRange: toNumber(row.questionWritesInRange),
      answerWritesInRange: toNumber(row.answerWritesInRange)
    }))
    .filter((row) => row.writesInRange > 0 || row.writesLast24h > 0);
  const externalRequestStrictRows = externalRequestAgentStats.filter((row) => isExternalAdoptionAgentName(row.agentName));
  const externalRequestProxiedRows = externalRequestAgentStats.filter((row) => isProxiedExternalAgentName(row.agentName));
  const externalRequestExcludedRows = externalRequestAgentStats.filter((row) => (
    !isExternalAdoptionAgentName(row.agentName)
    && !isProxiedExternalAgentName(row.agentName)
  ));

  type ExternalRequestAggregate = {
    writesInRange: number;
    writesLast24h: number;
    verifiedWritesInRange: number;
    signedWritesInRange: number;
    questionWritesInRange: number;
    answerWritesInRange: number;
    activeAgentsInRange: number;
  };

  function aggregateExternalRequestRows(rows: Array<{
    writesInRange: number;
    writesLast24h: number;
    verifiedWritesInRange: number;
    signedWritesInRange: number;
    questionWritesInRange: number;
    answerWritesInRange: number;
  }>): ExternalRequestAggregate {
    return rows.reduce<ExternalRequestAggregate>((acc, row) => {
      acc.writesInRange += row.writesInRange;
      acc.writesLast24h += row.writesLast24h;
      acc.verifiedWritesInRange += row.verifiedWritesInRange;
      acc.signedWritesInRange += row.signedWritesInRange;
      acc.questionWritesInRange += row.questionWritesInRange;
      acc.answerWritesInRange += row.answerWritesInRange;
      if (row.writesInRange > 0) acc.activeAgentsInRange += 1;
      return acc;
    }, {
      writesInRange: 0,
      writesLast24h: 0,
      verifiedWritesInRange: 0,
      signedWritesInRange: 0,
      questionWritesInRange: 0,
      answerWritesInRange: 0,
      activeAgentsInRange: 0
    });
  }

  const externalRequestStrictStats = aggregateExternalRequestRows(externalRequestStrictRows);
  const externalRequestProxiedStats = aggregateExternalRequestRows(externalRequestProxiedRows);
  const externalRequestExcludedStats = aggregateExternalRequestRows(externalRequestExcludedRows);

  const externalRequestTopAgents = externalRequestStrictRows
    .map((row) => ({
      agentName: row.agentName,
      count: row.writesInRange
    }))
    .filter((row) => row.count > 0)
    .sort((a, b) => b.count - a.count || a.agentName.localeCompare(b.agentName))
    .slice(0, 10);
  const externalRequestTopProxiedAgents = externalRequestProxiedRows
    .map((row) => ({
      agentName: row.agentName,
      count: row.writesInRange
    }))
    .filter((row) => row.count > 0)
    .sort((a, b) => b.count - a.count || a.agentName.localeCompare(b.agentName))
    .slice(0, 10);

  const uniqueAskersInRange = toNumber(uniqueAskersInRangeRows[0]?.count);
  const uniqueAskersPreviousRange = toNumber(uniqueAskersPreviousRangeRows[0]?.count);
  const uniqueAnswerersInRange = toNumber(uniqueAnswerersInRangeRows[0]?.count);
  const uniqueAnswerersPreviousRange = toNumber(uniqueAnswerersPreviousRangeRows[0]?.count);

  const firstAnswerSampleCount = 0;
  const firstAnswerWithin24hCount = 0;
  const firstAnswerAvgMinutes: number | null = null;
  const firstAnswerP50Minutes: number | null = null;
  const firstAnswerP95Minutes: number | null = null;

  const tractionByDay = new Map<string, {
    day: string;
    questions: number;
    answeredQuestions: number;
    acceptedQuestions: number;
    askers: number;
    answerers: number;
    avgFirstAnswerMinutes: number | null;
  }>();

  for (const row of questionDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    tractionByDay.set(day, {
      day,
      questions: toNumber(row.count),
      answeredQuestions: 0,
      acceptedQuestions: 0,
      askers: 0,
      answerers: 0,
      avgFirstAnswerMinutes: null
    });
  }

  for (const row of answeredQuestionDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = tractionByDay.get(day) ?? {
      day,
      questions: 0,
      answeredQuestions: 0,
      acceptedQuestions: 0,
      askers: 0,
      answerers: 0,
      avgFirstAnswerMinutes: null
    };
    existing.answeredQuestions = toNumber(row.count);
    tractionByDay.set(day, existing);
  }

  for (const row of acceptedQuestionDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = tractionByDay.get(day) ?? {
      day,
      questions: 0,
      answeredQuestions: 0,
      acceptedQuestions: 0,
      askers: 0,
      answerers: 0,
      avgFirstAnswerMinutes: null
    };
    existing.acceptedQuestions = toNumber(row.count);
    tractionByDay.set(day, existing);
  }

  for (const row of tractionAskerDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = tractionByDay.get(day) ?? {
      day,
      questions: 0,
      answeredQuestions: 0,
      acceptedQuestions: 0,
      askers: 0,
      answerers: 0,
      avgFirstAnswerMinutes: null
    };
    existing.askers = toNumber(row.askers);
    tractionByDay.set(day, existing);
  }

  for (const row of tractionAnswererDailyRows) {
    const date = row.day instanceof Date ? row.day : new Date(row.day);
    const day = date.toISOString().slice(0, 10);
    const existing = tractionByDay.get(day) ?? {
      day,
      questions: 0,
      answeredQuestions: 0,
      acceptedQuestions: 0,
      askers: 0,
      answerers: 0,
      avgFirstAnswerMinutes: null
    };
    existing.answerers = toNumber(row.answerers);
    tractionByDay.set(day, existing);
  }

  const tractionCursor = new Date(startDay);
  while (tractionCursor <= endDay) {
    const day = tractionCursor.toISOString().slice(0, 10);
    if (!tractionByDay.has(day)) {
      tractionByDay.set(day, {
        day,
        questions: 0,
        answeredQuestions: 0,
        acceptedQuestions: 0,
        askers: 0,
        answerers: 0,
        avgFirstAnswerMinutes: null
      });
    }
    tractionCursor.setUTCDate(tractionCursor.getUTCDate() + 1);
  }

  const tractionDaily = Array.from(tractionByDay.values())
    .sort((a, b) => a.day.localeCompare(b.day))
    .map((row) => ({
      ...row,
      answerCoverageRate: ratio(row.answeredQuestions, row.questions),
      acceptedRate: ratio(row.acceptedQuestions, row.questions)
    }));

  const growthQuestions = growthRate(questionsInRange, previousQuestionsInRange);
  const growthAnswers = growthRate(answersInRange, previousAnswersInRange);
  const growthAskers = growthRate(uniqueAskersInRange, uniqueAskersPreviousRange);
  const growthAnswerers = growthRate(uniqueAnswerersInRange, uniqueAnswerersPreviousRange);
  const rateAnswerCoverage = ratio(questionsAnsweredInRange, questionsInRange);
  const rateAcceptedOfQuestions = ratio(questionsAcceptedInRange, questionsInRange);
  const rateAcceptedOfAnswered = ratio(questionsAcceptedInRange, questionsAnsweredInRange);
  const rateAnswersPerQuestion = ratio(answersInRange, questionsInRange);
  const latency24hRate = firstAnswerSampleCount > 0
    ? ratio(firstAnswerWithin24hCount, firstAnswerSampleCount)
    : null;

  const scoreCoverage = clamp(rateAnswerCoverage, 0, 1);
  const scoreAcceptance = clamp(rateAcceptedOfAnswered, 0, 1);
  const scoreDepth = clamp(rateAnswersPerQuestion / 2, 0, 1);
  const scoreAskers = clamp(growthAskers == null ? 0.5 : 0.5 + growthAskers / 2, 0, 1);
  const scoreAnswerers = clamp(growthAnswerers == null ? 0.5 : 0.5 + growthAnswerers / 2, 0, 1);
  const scoreQuestionGrowth = clamp(growthQuestions == null ? 0.5 : 0.5 + growthQuestions / 2, 0, 1);
  const scoreAnswerGrowth = clamp(growthAnswers == null ? 0.5 : 0.5 + growthAnswers / 2, 0, 1);
  const scoreLatency = clamp(latency24hRate ?? 0.5, 0, 1);
  const weightedScoreRaw = (
    scoreCoverage * 0.24 +
    scoreAcceptance * 0.14 +
    scoreDepth * 0.08 +
    scoreAskers * 0.14 +
    scoreAnswerers * 0.14 +
    scoreQuestionGrowth * 0.12 +
    scoreAnswerGrowth * 0.08 +
    scoreLatency * 0.06
  );
  const tractionScore = Math.round(weightedScoreRaw * 1000) / 10;

  return {
    days,
    since: since.toISOString(),
    total,
    last24h: lastDay,
    byRoute: byRoute.map((row) => ({ route: row.route, count: row._count.route })),
    byStatus: byStatus.map((row) => ({ status: row.status, count: row._count.status })),
    byIp: [],
    byReferer: [],
    byUserAgent: [],
    byAgentName: [],
    recentErrors: [],
    daily: dailyRows.map((row) => {
      const date = row.day instanceof Date ? row.day : new Date(row.day);
      return {
        day: date.toISOString().slice(0, 10),
        count: Number(row.count)
      };
    }),
    contentTotals: {
      totalQuestions,
      totalAnswers,
      questionsInRange,
      answersInRange
    },
    external: {
      configuredActorTypes: externalActorTypes,
      kpiMode: 'real_external_only',
      identity: {
        boundAgents: externalBoundAgents.length,
        users: externalUserIds.length,
        boundAgentsAll: externalBoundAgentsAll.length,
        usersAll: externalUserIdsAll.length,
        filteredOutSyntheticBoundAgents: externalBoundAgentsSynthetic.length,
        filteredOutSyntheticUsers: externalUserIdsSynthetic.length,
        filteredOutExcludedBoundAgents: externalBoundAgentsExcluded.length,
        filteredOutExcludedUsers: externalUserIdsExcluded.length
      },
      content: {
        questionsInRange: externalQuestionsInRange,
        questionsLast24h: externalQuestionsLast24h,
        answersInRange: externalAnswersInRange,
        answersLast24h: externalAnswersLast24h,
        acceptedInRange: externalAcceptedInRange,
        acceptedLast24h: externalAcceptedLast24h,
        answersPerQuestion: ratio(externalAnswersInRange, externalQuestionsInRange)
      },
      requests: {
        writesInRange: externalRequestStrictStats.writesInRange,
        writesLast24h: externalRequestStrictStats.writesLast24h,
        identityVerifiedWritesInRange: externalRequestStrictStats.verifiedWritesInRange,
        signatureVerifiedWritesInRange: externalRequestStrictStats.signedWritesInRange,
        questionWritesInRange: externalRequestStrictStats.questionWritesInRange,
        answerWritesInRange: externalRequestStrictStats.answerWritesInRange,
        answerRateFromWritesInRange: ratio(externalRequestStrictStats.answerWritesInRange, externalRequestStrictStats.writesInRange),
        activeAgentsInRange: externalRequestStrictStats.activeAgentsInRange,
        proxiedWritesInRange: externalRequestProxiedStats.writesInRange,
        proxiedWritesLast24h: externalRequestProxiedStats.writesLast24h,
        proxiedIdentityVerifiedWritesInRange: externalRequestProxiedStats.verifiedWritesInRange,
        proxiedSignatureVerifiedWritesInRange: externalRequestProxiedStats.signedWritesInRange,
        proxiedQuestionWritesInRange: externalRequestProxiedStats.questionWritesInRange,
        proxiedAnswerWritesInRange: externalRequestProxiedStats.answerWritesInRange,
        proxiedActiveAgentsInRange: externalRequestProxiedStats.activeAgentsInRange,
        excludedWritesInRange: externalRequestExcludedStats.writesInRange,
        combinedWritesInRange: externalRequestStrictStats.writesInRange + externalRequestProxiedStats.writesInRange,
        combinedWritesLast24h: externalRequestStrictStats.writesLast24h + externalRequestProxiedStats.writesLast24h,
        combinedActiveAgentsInRange: externalRequestStrictStats.activeAgentsInRange + externalRequestProxiedStats.activeAgentsInRange,
        strict: {
          writesInRange: externalRequestStrictStats.writesInRange,
          writesLast24h: externalRequestStrictStats.writesLast24h,
          identityVerifiedWritesInRange: externalRequestStrictStats.verifiedWritesInRange,
          signatureVerifiedWritesInRange: externalRequestStrictStats.signedWritesInRange,
          questionWritesInRange: externalRequestStrictStats.questionWritesInRange,
          answerWritesInRange: externalRequestStrictStats.answerWritesInRange,
          activeAgentsInRange: externalRequestStrictStats.activeAgentsInRange
        },
        proxied: {
          writesInRange: externalRequestProxiedStats.writesInRange,
          writesLast24h: externalRequestProxiedStats.writesLast24h,
          identityVerifiedWritesInRange: externalRequestProxiedStats.verifiedWritesInRange,
          signatureVerifiedWritesInRange: externalRequestProxiedStats.signedWritesInRange,
          questionWritesInRange: externalRequestProxiedStats.questionWritesInRange,
          answerWritesInRange: externalRequestProxiedStats.answerWritesInRange,
          activeAgentsInRange: externalRequestProxiedStats.activeAgentsInRange
        }
      },
      guardrail: getNextJobGuardrailSnapshot(),
      kpi: {
        currentAnswerers7d: externalCurrentAnswerers7d.size,
        previousAnswerers7d: externalPreviousAnswerers7d.size,
        retainedAnswerers7d: retainedExternalAnswerers7d.length,
        retainedAnswererRate7d: ratio(retainedExternalAnswerers7d.length, externalPreviousAnswerers7d.size)
      },
      qaDaily: externalQaDaily,
      topAgents: externalRequestTopAgents,
      topProxiedAgents: externalRequestTopProxiedAgents
    },
    qaDaily,
    traction: {
      current: {
        questionsInRange,
        answersInRange,
        questionsAnsweredInRange,
        questionsAcceptedInRange,
        uniqueAskersInRange,
        uniqueAnswerersInRange
      },
      previous: {
        questionsInRange: previousQuestionsInRange,
        answersInRange: previousAnswersInRange,
        uniqueAskersInRange: uniqueAskersPreviousRange,
        uniqueAnswerersInRange: uniqueAnswerersPreviousRange
      },
      growth: {
        questionsPct: growthQuestions,
        answersPct: growthAnswers,
        askersPct: growthAskers,
        answerersPct: growthAnswerers
      },
      rates: {
        answerCoverage: rateAnswerCoverage,
        acceptedOfQuestions: rateAcceptedOfQuestions,
        acceptedOfAnswered: rateAcceptedOfAnswered,
        answersPerQuestion: rateAnswersPerQuestion
      },
      backlog: {
        unansweredInRange: Math.max(0, questionsInRange - questionsAnsweredInRange),
        unansweredTotal
      },
      latency: {
        sampleCount: firstAnswerSampleCount,
        avgFirstAnswerMinutes: firstAnswerAvgMinutes,
        p50FirstAnswerMinutes: firstAnswerP50Minutes,
        p95FirstAnswerMinutes: firstAnswerP95Minutes,
        answeredWithin24hCount: firstAnswerWithin24hCount,
        answeredWithin24hRate: latency24hRate
      },
      score: {
        total: tractionScore,
        components: {
          answerCoverage: scoreCoverage,
          acceptance: scoreAcceptance,
          answerDepth: scoreDepth,
          askerGrowth: scoreAskers,
          answererGrowth: scoreAnswerers,
          questionGrowth: scoreQuestionGrowth,
          answerGrowth: scoreAnswerGrowth,
          latency24h: scoreLatency
        }
      },
      daily: tractionDaily
    }
  };
}

async function getUsageSummaryCached(days: number, includeNoise: boolean) {
  const load = () => withPrismaPoolRetry('usage_summary', () => getUsageSummary(days, includeNoise), 3);
  const latestCachedValue = () => {
    let latest: { updatedAt: number; value: unknown } | null = null;
    for (const entry of usageSummaryCache.values()) {
      if (!latest || entry.updatedAt > latest.updatedAt) latest = entry;
    }
    return latest?.value;
  };
  const key = `${days}:${includeNoise ? 1 : 0}`;
  const now = Date.now();
  const cached = usageSummaryCache.get(key);
  const inflight = usageSummaryInflight.get(key);
  if (cached) {
    const isFresh = now - cached.updatedAt < USAGE_SUMMARY_FRESH_MS;
    if (isFresh) return cached.value;
    if (!inflight) {
      const refresh = load()
        .then((value) => {
          usageSummaryCache.set(key, { updatedAt: Date.now(), value });
          return value;
        })
        .catch((err) => {
          fastify.log.warn({ err, key }, 'usage summary refresh failed, serving stale cache');
          return cached.value;
        })
        .finally(() => {
          usageSummaryInflight.delete(key);
        });
      usageSummaryInflight.set(key, refresh);
    }
    return cached.value;
  }
  if (inflight) return inflight;
  const pending = load()
    .then((value) => {
      usageSummaryCache.set(key, { updatedAt: Date.now(), value });
      return value;
    })
    .catch((err) => {
      const fallback = latestCachedValue();
      if (fallback) {
        fastify.log.warn({ err, key }, 'usage summary failed, serving latest cached snapshot');
        return fallback;
      }
      throw err;
    })
    .finally(() => {
      usageSummaryInflight.delete(key);
    });
  usageSummaryInflight.set(key, pending);
  return pending;
}

fastify.get('/admin/usage/data', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  const query = request.query as { days?: number; includeNoise?: boolean };
  const days = Math.min(90, Math.max(1, Number(query.days ?? 7)));
  reply.header('Cache-Control', 'no-store');
  return getUsageSummaryCached(days, Boolean(query.includeNoise));
});

fastify.get('/admin/usage', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  const baseUrl = getBaseUrl(request);
  reply.type('text/html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench Usage</title>
    <style>
      :root { color-scheme: light; }
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; margin: 0; background: #f6f7fb; color: #101827; }
      header { background: #0b0f1a; color: #fff; padding: 24px 20px; }
      header h1 { margin: 0 0 6px; font-size: 20px; }
      header p { margin: 0; color: #c7c9d3; font-size: 13px; }
      main { max-width: 960px; margin: 0 auto; padding: 20px; }
      .card { background: #fff; border-radius: 12px; padding: 16px; box-shadow: 0 8px 24px rgba(17, 24, 39, 0.08); margin-bottom: 16px; }
      .row { display: flex; gap: 16px; flex-wrap: wrap; }
      .field { display: flex; flex-direction: column; gap: 6px; min-width: 220px; }
      label { font-size: 12px; color: #6b7280; }
      input { border: 1px solid #e5e7eb; border-radius: 8px; padding: 8px 10px; font-size: 14px; }
      button { background: #2563eb; color: #fff; border: 0; border-radius: 8px; padding: 10px 14px; font-weight: 600; cursor: pointer; }
      button:disabled { opacity: 0.6; cursor: not-allowed; }
      .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }
      .metric { background: #f9fafb; border-radius: 10px; padding: 12px; }
      .metric h3 { margin: 0; font-size: 13px; color: #6b7280; }
      .metric div { font-size: 22px; font-weight: 700; margin-top: 6px; }
      .metric small { display: block; margin-top: 6px; font-size: 12px; color: #6b7280; font-weight: 500; }
      .metric small.good { color: #065f46; }
      .metric small.bad { color: #b91c1c; }
      .metric small.neutral { color: #6b7280; }
      .list { display: grid; grid-template-columns: 1fr; gap: 6px; }
      .pill { display: flex; justify-content: space-between; gap: 12px; padding: 8px 10px; background: #f3f4f6; border-radius: 8px; font-size: 13px; word-break: break-word; }
      .pill span:first-child { overflow-wrap: anywhere; }
      .bar { height: 10px; background: #e5e7eb; border-radius: 999px; overflow: hidden; }
      .bar > span { display: block; height: 100%; background: #22c55e; }
      .muted { color: #6b7280; font-size: 12px; }
      .error { color: #b91c1c; font-size: 13px; margin-top: 8px; }
      .error-item { display: grid; grid-template-columns: 90px 1fr; gap: 8px 12px; padding: 10px; border-radius: 10px; background: #fff7ed; border: 1px solid #fed7aa; font-size: 12px; }
      .error-item code { background: #fff; padding: 2px 6px; border-radius: 6px; }
      .qa-legend { display: flex; gap: 14px; margin-bottom: 10px; font-size: 12px; color: #6b7280; }
      .qa-dot { width: 10px; height: 10px; display: inline-block; border-radius: 999px; margin-right: 6px; }
      .qa-chart { display: grid; gap: 8px; }
      .qa-row { display: grid; grid-template-columns: 90px 1fr 90px; gap: 10px; align-items: center; font-size: 12px; }
      .qa-bars { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
      .qa-bar { height: 10px; background: #e5e7eb; border-radius: 999px; overflow: hidden; }
      .qa-bar > span { display: block; height: 100%; border-radius: 999px; min-width: 1px; }
      .qa-q > span { background: #2563eb; }
      .qa-a > span { background: #22c55e; }
      .qa-values { text-align: right; color: #374151; }
      .traction-chart { display: grid; gap: 8px; }
      .traction-row { display: grid; grid-template-columns: 90px 1fr 150px; gap: 10px; align-items: center; font-size: 12px; }
      .traction-bars { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
      .traction-bar { height: 10px; background: #e5e7eb; border-radius: 999px; overflow: hidden; }
      .traction-bar > span { display: block; height: 100%; border-radius: 999px; min-width: 1px; }
      .traction-askers > span { background: #0ea5e9; }
      .traction-answerers > span { background: #f59e0b; }
      .traction-values { text-align: right; color: #374151; }
    </style>
  </head>
  <body>
    <header>
      <h1>A2ABench Usage</h1>
      <p>Live usage summary from ${baseUrl}</p>
    </header>
    <main>
      <div class="card">
        <div class="row">
          <div class="field">
            <label for="days">Days</label>
            <input id="days" type="number" min="1" max="90" value="7" />
          </div>
          <div class="field">
            <label for="noise">Include bot noise</label>
            <input id="noise" type="checkbox" />
          </div>
          <div class="field" style="align-self: flex-end;">
            <button id="load">Load usage</button>
          </div>
        </div>
        <div id="status" class="muted" style="margin-top:8px;"></div>
        <div id="error" class="error"></div>
      </div>

      <div class="card">
        <div class="metrics">
          <div class="metric"><h3>Requests (range)</h3><div id="total">—</div></div>
          <div class="metric"><h3>Requests (last 24h)</h3><div id="last24h">—</div></div>
          <div class="metric"><h3>Range Start (UTC)</h3><div id="since">—</div></div>
          <div class="metric"><h3>Questions Created (all-time)</h3><div id="totalQuestions">—</div></div>
          <div class="metric"><h3>Answers Created (all-time)</h3><div id="totalAnswers">—</div></div>
          <div class="metric"><h3>Questions Created (range)</h3><div id="questionsInRange">—</div></div>
          <div class="metric"><h3>Answers Created (range)</h3><div id="answersInRange">—</div></div>
        </div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Traction KPIs</h2>
        <div class="metrics">
          <div class="metric"><h3>Answer coverage</h3><div id="tractionAnswerCoverage">—</div><small id="tractionQuestionsGrowth">—</small></div>
          <div class="metric"><h3>Answers / question</h3><div id="tractionAnswersPerQuestion">—</div><small id="tractionAnswersGrowth">—</small></div>
          <div class="metric"><h3>Accepted (of answered)</h3><div id="tractionAcceptedOfAnswered">—</div><small id="tractionAcceptedOfQuestions">—</small></div>
          <div class="metric"><h3>Unique askers</h3><div id="tractionUniqueAskers">—</div><small id="tractionAskersGrowth">—</small></div>
          <div class="metric"><h3>Unique answerers</h3><div id="tractionUniqueAnswerers">—</div><small id="tractionAnswerersGrowth">—</small></div>
          <div class="metric"><h3>Median first answer</h3><div id="tractionFirstAnswerP50">—</div><small id="tractionFirstAnswerP95">—</small></div>
          <div class="metric"><h3>Answered within 24h</h3><div id="tractionAnswered24hRate">—</div><small id="tractionAnswered24hCount">—</small></div>
          <div class="metric"><h3>Unanswered backlog</h3><div id="tractionUnansweredTotal">—</div><small id="tractionUnansweredRange">—</small></div>
          <div class="metric"><h3>Traction score (window)</h3><div id="tractionScoreCurrent">—</div><small id="tractionScoreCurrentCaption">0-100 composite</small></div>
          <div class="metric"><h3>Traction score (7d)</h3><div id="tractionScore7d">—</div><small id="tractionScore7dCaption">—</small></div>
          <div class="metric"><h3>Traction score (30d)</h3><div id="tractionScore30d">—</div><small id="tractionScore30dCaption">—</small></div>
          <div class="metric"><h3>Traction score (90d)</h3><div id="tractionScore90d">—</div><small id="tractionScore90dCaption">—</small></div>
        </div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">External Agent Slice</h2>
        <div class="metrics">
          <div class="metric"><h3>Bound external agents</h3><div id="externalBoundAgents">—</div><small id="externalActorTypes">—</small></div>
          <div class="metric"><h3>External key users</h3><div id="externalUsers">—</div><small id="externalActiveAgents">—</small></div>
          <div class="metric"><h3>External writes (range)</h3><div id="externalWritesInRange">—</div><small id="externalWritesLast24h">—</small></div>
          <div class="metric"><h3>Proxied external writes</h3><div id="externalProxiedWritesInRange">—</div><small id="externalProxiedWritesLast24h">—</small></div>
          <div class="metric"><h3>Proxied active agents</h3><div id="externalProxiedActiveAgents">—</div><small id="externalProxiedAgentShare">—</small></div>
          <div class="metric"><h3>Identity-verified writes</h3><div id="externalIdentityWrites">—</div><small id="externalIdentityRate">—</small></div>
          <div class="metric"><h3>Signature-verified writes</h3><div id="externalSignatureWrites">—</div><small id="externalSignatureRate">—</small></div>
          <div class="metric"><h3>Questions (external)</h3><div id="externalQuestionsInRange">—</div><small id="externalQuestionsLast24h">—</small></div>
          <div class="metric"><h3>Answers (external)</h3><div id="externalAnswersInRange">—</div><small id="externalAnswersLast24h">—</small></div>
          <div class="metric"><h3>Accepted (external)</h3><div id="externalAcceptedInRange">—</div><small id="externalAnswersPerQuestion">—</small></div>
        </div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">External questions vs answers (daily)</h2>
        <div class="qa-legend">
          <span><span class="qa-dot" style="background:#2563eb;"></span>Questions</span>
          <span><span class="qa-dot" style="background:#22c55e;"></span>Answers</span>
        </div>
        <div id="externalQaChart" class="qa-chart"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top external agents (write requests)</h2>
        <div id="externalTopAgents" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top proxied agents (anonymous)</h2>
        <div id="externalTopProxiedAgents" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top routes</h2>
        <div id="routes" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Status codes</h2>
        <div id="statuses" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Daily</h2>
        <div id="daily" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Questions vs answers (daily)</h2>
        <div class="qa-legend">
          <span><span class="qa-dot" style="background:#2563eb;"></span>Questions</span>
          <span><span class="qa-dot" style="background:#22c55e;"></span>Answers</span>
        </div>
        <div id="qaChart" class="qa-chart"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Daily askers vs answerers</h2>
        <div class="qa-legend">
          <span><span class="qa-dot" style="background:#0ea5e9;"></span>Askers</span>
          <span><span class="qa-dot" style="background:#f59e0b;"></span>Answerers</span>
        </div>
        <div id="tractionChart" class="traction-chart"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top IPs</h2>
        <div id="ips" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top referrers</h2>
        <div id="referrers" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top user agents</h2>
        <div id="userAgents" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Top agent names</h2>
        <div id="agentNames" class="list"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0;">Recent errors (4xx/5xx)</h2>
        <div id="errors" class="list"></div>
      </div>
    </main>
    <script>
      const daysInput = document.getElementById('days');
      const noiseInput = document.getElementById('noise');
      const loadBtn = document.getElementById('load');
      const statusEl = document.getElementById('status');
      const errorEl = document.getElementById('error');
      const totalEl = document.getElementById('total');
      const last24hEl = document.getElementById('last24h');
      const sinceEl = document.getElementById('since');
      const totalQuestionsEl = document.getElementById('totalQuestions');
      const totalAnswersEl = document.getElementById('totalAnswers');
      const questionsInRangeEl = document.getElementById('questionsInRange');
      const answersInRangeEl = document.getElementById('answersInRange');
      const tractionAnswerCoverageEl = document.getElementById('tractionAnswerCoverage');
      const tractionQuestionsGrowthEl = document.getElementById('tractionQuestionsGrowth');
      const tractionAnswersPerQuestionEl = document.getElementById('tractionAnswersPerQuestion');
      const tractionAnswersGrowthEl = document.getElementById('tractionAnswersGrowth');
      const tractionAcceptedOfAnsweredEl = document.getElementById('tractionAcceptedOfAnswered');
      const tractionAcceptedOfQuestionsEl = document.getElementById('tractionAcceptedOfQuestions');
      const tractionUniqueAskersEl = document.getElementById('tractionUniqueAskers');
      const tractionAskersGrowthEl = document.getElementById('tractionAskersGrowth');
      const tractionUniqueAnswerersEl = document.getElementById('tractionUniqueAnswerers');
      const tractionAnswerersGrowthEl = document.getElementById('tractionAnswerersGrowth');
      const tractionFirstAnswerP50El = document.getElementById('tractionFirstAnswerP50');
      const tractionFirstAnswerP95El = document.getElementById('tractionFirstAnswerP95');
      const tractionAnswered24hRateEl = document.getElementById('tractionAnswered24hRate');
      const tractionAnswered24hCountEl = document.getElementById('tractionAnswered24hCount');
      const tractionUnansweredTotalEl = document.getElementById('tractionUnansweredTotal');
      const tractionUnansweredRangeEl = document.getElementById('tractionUnansweredRange');
      const tractionScoreCurrentEl = document.getElementById('tractionScoreCurrent');
      const tractionScoreCurrentCaptionEl = document.getElementById('tractionScoreCurrentCaption');
      const tractionScore7dEl = document.getElementById('tractionScore7d');
      const tractionScore7dCaptionEl = document.getElementById('tractionScore7dCaption');
      const tractionScore30dEl = document.getElementById('tractionScore30d');
      const tractionScore30dCaptionEl = document.getElementById('tractionScore30dCaption');
      const tractionScore90dEl = document.getElementById('tractionScore90d');
      const tractionScore90dCaptionEl = document.getElementById('tractionScore90dCaption');
      const externalBoundAgentsEl = document.getElementById('externalBoundAgents');
      const externalActorTypesEl = document.getElementById('externalActorTypes');
      const externalUsersEl = document.getElementById('externalUsers');
      const externalActiveAgentsEl = document.getElementById('externalActiveAgents');
      const externalWritesInRangeEl = document.getElementById('externalWritesInRange');
      const externalWritesLast24hEl = document.getElementById('externalWritesLast24h');
      const externalProxiedWritesInRangeEl = document.getElementById('externalProxiedWritesInRange');
      const externalProxiedWritesLast24hEl = document.getElementById('externalProxiedWritesLast24h');
      const externalProxiedActiveAgentsEl = document.getElementById('externalProxiedActiveAgents');
      const externalProxiedAgentShareEl = document.getElementById('externalProxiedAgentShare');
      const externalIdentityWritesEl = document.getElementById('externalIdentityWrites');
      const externalIdentityRateEl = document.getElementById('externalIdentityRate');
      const externalSignatureWritesEl = document.getElementById('externalSignatureWrites');
      const externalSignatureRateEl = document.getElementById('externalSignatureRate');
      const externalQuestionsInRangeEl = document.getElementById('externalQuestionsInRange');
      const externalQuestionsLast24hEl = document.getElementById('externalQuestionsLast24h');
      const externalAnswersInRangeEl = document.getElementById('externalAnswersInRange');
      const externalAnswersLast24hEl = document.getElementById('externalAnswersLast24h');
      const externalAcceptedInRangeEl = document.getElementById('externalAcceptedInRange');
      const externalAnswersPerQuestionEl = document.getElementById('externalAnswersPerQuestion');
      const routesEl = document.getElementById('routes');
      const statusesEl = document.getElementById('statuses');
      const dailyEl = document.getElementById('daily');
      const qaChartEl = document.getElementById('qaChart');
      const tractionChartEl = document.getElementById('tractionChart');
      const externalQaChartEl = document.getElementById('externalQaChart');
      const externalTopAgentsEl = document.getElementById('externalTopAgents');
      const externalTopProxiedAgentsEl = document.getElementById('externalTopProxiedAgents');
      const ipsEl = document.getElementById('ips');
      const referrersEl = document.getElementById('referrers');
      const userAgentsEl = document.getElementById('userAgents');
      const agentNamesEl = document.getElementById('agentNames');
      const errorsEl = document.getElementById('errors');

      function setStatus(text) { statusEl.textContent = text || ''; }
      function setError(text) { errorEl.textContent = text || ''; }
      function toNum(value) {
        const n = Number(value);
        return Number.isFinite(n) ? n : 0;
      }
      function ratio(numerator, denominator) {
        const n = toNum(numerator);
        const d = toNum(denominator);
        if (d <= 0) return null;
        return n / d;
      }
      function formatPercent(value, digits = 1) {
        if (value == null) return '—';
        return (toNum(value) * 100).toFixed(digits) + '%';
      }
      function formatRatio(value, digits = 2) {
        if (value == null) return '—';
        return toNum(value).toFixed(digits);
      }
      function formatMinutes(value) {
        if (value == null) return '—';
        return toNum(value).toFixed(1) + 'm';
      }
      function formatScore(value) {
        if (value == null) return '—';
        return toNum(value).toFixed(1);
      }
      function scoreBand(score) {
        const n = toNum(score);
        if (n >= 70) return 'strong';
        if (n >= 45) return 'moderate';
        return 'weak';
      }
      function setDelta(el, value) {
        if (!el) return;
        if (value == null) {
          el.textContent = 'vs prev: —';
          el.className = 'neutral';
          return;
        }
        const pct = toNum(value) * 100;
        const sign = pct > 0 ? '+' : '';
        el.textContent = 'vs prev: ' + sign + pct.toFixed(1) + '%';
        el.className = pct > 0 ? 'good' : (pct < 0 ? 'bad' : 'neutral');
      }

      function renderList(container, rows, labelKey, countKey) {
        container.innerHTML = '';
        const max = Math.max(...rows.map(r => r[countKey]), 1);
        rows.forEach(row => {
          const wrapper = document.createElement('div');
          wrapper.className = 'pill';
          wrapper.innerHTML = '<span>' + row[labelKey] + '</span><span>' + row[countKey] + '</span>';
          const bar = document.createElement('div');
          bar.className = 'bar';
          const fill = document.createElement('span');
          fill.style.width = Math.round((row[countKey] / max) * 100) + '%';
          bar.appendChild(fill);
          const containerWrap = document.createElement('div');
          containerWrap.style.display = 'flex';
          containerWrap.style.flexDirection = 'column';
          containerWrap.style.gap = '6px';
          containerWrap.appendChild(wrapper);
          containerWrap.appendChild(bar);
          container.appendChild(containerWrap);
        });
        if (!rows.length) {
          container.innerHTML = '<div class="muted">No data yet.</div>';
        }
      }

      function renderErrors(rows) {
        errorsEl.innerHTML = '';
        if (!rows.length) {
          errorsEl.innerHTML = '<div class="muted">No errors yet.</div>';
          return;
        }
        rows.forEach(row => {
          const wrap = document.createElement('div');
          wrap.className = 'error-item';
          wrap.innerHTML =
            '<div><strong>' + row.status + '</strong></div>' +
            '<div><code>' + row.route + '</code></div>' +
            '<div class="muted">Time</div><div>' + new Date(row.createdAt).toLocaleString() + '</div>' +
            '<div class="muted">Agent</div><div>' + (row.agentName || '—') + '</div>' +
            '<div class="muted">IP</div><div>' + (row.ip || '—') + '</div>' +
            '<div class="muted">Referrer</div><div>' + (row.referer || '—') + '</div>' +
            '<div class="muted">User-Agent</div><div>' + (row.userAgent || '—') + '</div>';
          errorsEl.appendChild(wrap);
        });
      }

      function renderQaChart(rows, container = qaChartEl, emptyMessage = 'No question/answer activity in this range.') {
        container.innerHTML = '';
        if (!rows.length) {
          container.innerHTML = '<div class="muted">' + emptyMessage + '</div>';
          return;
        }

        const max = Math.max(...rows.map((row) => Math.max(row.questions || 0, row.answers || 0)), 1);
        rows.forEach((row) => {
          const q = Number(row.questions || 0);
          const a = Number(row.answers || 0);
          const qPct = Math.round((q / max) * 100);
          const aPct = Math.round((a / max) * 100);
          const wrap = document.createElement('div');
          wrap.className = 'qa-row';
          wrap.innerHTML =
            '<div class="muted">' + row.day + '</div>' +
            '<div class="qa-bars">' +
              '<div class="qa-bar qa-q"><span style="width:' + qPct + '%"></span></div>' +
              '<div class="qa-bar qa-a"><span style="width:' + aPct + '%"></span></div>' +
            '</div>' +
            '<div class="qa-values">Q ' + q + ' · A ' + a + '</div>';
          container.appendChild(wrap);
        });
      }

      function renderTractionChart(rows) {
        tractionChartEl.innerHTML = '';
        if (!rows.length) {
          tractionChartEl.innerHTML = '<div class="muted">No traction activity in this range.</div>';
          return;
        }
        const max = Math.max(...rows.map((row) => Math.max(toNum(row.askers), toNum(row.answerers))), 1);
        rows.forEach((row) => {
          const askers = toNum(row.askers);
          const answerers = toNum(row.answerers);
          const askersPct = Math.round((askers / max) * 100);
          const answerersPct = Math.round((answerers / max) * 100);
          const coverage = formatPercent(row.answerCoverageRate || 0, 0);
          const wrap = document.createElement('div');
          wrap.className = 'traction-row';
          wrap.innerHTML =
            '<div class="muted">' + row.day + '</div>' +
            '<div class="traction-bars">' +
              '<div class="traction-bar traction-askers"><span style="width:' + askersPct + '%"></span></div>' +
              '<div class="traction-bar traction-answerers"><span style="width:' + answerersPct + '%"></span></div>' +
            '</div>' +
            '<div class="traction-values">A ' + askers + ' / ' + answerers + ' · Cov ' + coverage + '</div>';
          tractionChartEl.appendChild(wrap);
        });
      }

      async function loadUsage() {
        setError('');
        setStatus('Loading…');
        const days = Math.min(90, Math.max(1, Number(daysInput.value || 7)));
        try {
          const includeNoise = noiseInput.checked ? '&includeNoise=1' : '';
          const res = await fetch('/admin/usage/data?days=' + days + includeNoise);
          if (!res.ok) {
            const text = await res.text();
            throw new Error(text || ('Request failed: ' + res.status));
          }
          const data = await res.json();
          totalEl.textContent = data.total ?? '0';
          last24hEl.textContent = data.last24h ?? '0';
          sinceEl.textContent = (data.since || '').slice(0, 10);
          totalQuestionsEl.textContent = data.contentTotals?.totalQuestions ?? '0';
          totalAnswersEl.textContent = data.contentTotals?.totalAnswers ?? '0';
          questionsInRangeEl.textContent = data.contentTotals?.questionsInRange ?? '0';
          answersInRangeEl.textContent = data.contentTotals?.answersInRange ?? '0';
          const traction = data.traction || {};
          const current = traction.current || {};
          const growth = traction.growth || {};
          const rates = traction.rates || {};
          const backlog = traction.backlog || {};
          const latency = traction.latency || {};
          const score = traction.score || {};
          tractionAnswerCoverageEl.textContent = formatPercent(rates.answerCoverage, 1);
          tractionAnswersPerQuestionEl.textContent = formatRatio(rates.answersPerQuestion, 2);
          tractionAcceptedOfAnsweredEl.textContent = formatPercent(rates.acceptedOfAnswered, 1);
          tractionAcceptedOfQuestionsEl.textContent = 'of all questions: ' + formatPercent(rates.acceptedOfQuestions, 1);
          tractionUniqueAskersEl.textContent = String(current.uniqueAskersInRange ?? 0);
          tractionUniqueAnswerersEl.textContent = String(current.uniqueAnswerersInRange ?? 0);
          tractionFirstAnswerP50El.textContent = formatMinutes(latency.p50FirstAnswerMinutes);
          tractionFirstAnswerP95El.textContent = 'P95: ' + formatMinutes(latency.p95FirstAnswerMinutes);
          tractionAnswered24hRateEl.textContent = formatPercent(latency.answeredWithin24hRate, 1);
          tractionAnswered24hCountEl.textContent = (latency.answeredWithin24hCount ?? 0) + ' / ' + (latency.sampleCount ?? 0) + ' answered threads';
          tractionUnansweredTotalEl.textContent = String(backlog.unansweredTotal ?? 0);
          tractionUnansweredRangeEl.textContent = 'in range: ' + String(backlog.unansweredInRange ?? 0);
          tractionScoreCurrentEl.textContent = formatScore(score.total);
          tractionScoreCurrentCaptionEl.textContent = 'window ' + days + 'd · ' + scoreBand(score.total);
          setDelta(tractionQuestionsGrowthEl, growth.questionsPct);
          setDelta(tractionAnswersGrowthEl, growth.answersPct);
          setDelta(tractionAskersGrowthEl, growth.askersPct);
          setDelta(tractionAnswerersGrowthEl, growth.answerersPct);
          const external = data.external || {};
          const externalIdentity = external.identity || {};
          const externalRequests = external.requests || {};
          const externalContent = external.content || {};
          const externalActorTypes = Array.isArray(external.configuredActorTypes) ? external.configuredActorTypes : [];
          externalBoundAgentsEl.textContent = String(externalIdentity.boundAgents ?? 0);
          externalActorTypesEl.textContent = externalActorTypes.length ? externalActorTypes.join(', ') : 'no external actor types configured';
          externalUsersEl.textContent = String(externalIdentity.users ?? 0);
          externalActiveAgentsEl.textContent = 'strict: ' + String(externalRequests.activeAgentsInRange ?? 0) + ' · strict+proxied: ' + String(externalRequests.combinedActiveAgentsInRange ?? 0);
          externalWritesInRangeEl.textContent = String(externalRequests.writesInRange ?? 0);
          externalWritesLast24hEl.textContent = 'strict+proxied 24h: ' + String(externalRequests.combinedWritesLast24h ?? 0);
          externalProxiedWritesInRangeEl.textContent = String(externalRequests.proxiedWritesInRange ?? 0);
          externalProxiedWritesLast24hEl.textContent = 'last 24h: ' + String(externalRequests.proxiedWritesLast24h ?? 0);
          externalProxiedActiveAgentsEl.textContent = String(externalRequests.proxiedActiveAgentsInRange ?? 0);
          externalProxiedAgentShareEl.textContent = 'of strict+proxied: ' + formatPercent(
            ratio(
              externalRequests.proxiedActiveAgentsInRange,
              toNum(externalRequests.proxiedActiveAgentsInRange) + toNum(externalRequests.activeAgentsInRange)
            ),
            1
          );
          externalIdentityWritesEl.textContent = String(externalRequests.identityVerifiedWritesInRange ?? 0);
          externalIdentityRateEl.textContent = 'of writes: ' + formatPercent(ratio(externalRequests.identityVerifiedWritesInRange, externalRequests.writesInRange), 1);
          externalSignatureWritesEl.textContent = String(externalRequests.signatureVerifiedWritesInRange ?? 0);
          externalSignatureRateEl.textContent = 'of writes: ' + formatPercent(ratio(externalRequests.signatureVerifiedWritesInRange, externalRequests.writesInRange), 1);
          externalQuestionsInRangeEl.textContent = String(externalContent.questionsInRange ?? 0);
          externalQuestionsLast24hEl.textContent = 'last 24h: ' + String(externalContent.questionsLast24h ?? 0);
          externalAnswersInRangeEl.textContent = String(externalContent.answersInRange ?? 0);
          externalAnswersLast24hEl.textContent = 'last 24h: ' + String(externalContent.answersLast24h ?? 0);
          externalAcceptedInRangeEl.textContent = String(externalContent.acceptedInRange ?? 0);
          externalAnswersPerQuestionEl.textContent = 'A/Q: ' + formatRatio(externalContent.answersPerQuestion, 2);
          renderList(routesEl, data.byRoute || [], 'route', 'count');
          renderList(statusesEl, data.byStatus || [], 'status', 'count');
          renderList(dailyEl, data.daily || [], 'day', 'count');
          renderQaChart(data.qaDaily || [], qaChartEl, 'No question/answer activity in this range.');
          renderQaChart(external.qaDaily || [], externalQaChartEl, 'No external question/answer activity in this range.');
          renderTractionChart(traction.daily || []);
          renderList(externalTopAgentsEl, external.topAgents || [], 'agentName', 'count');
          renderList(externalTopProxiedAgentsEl, external.topProxiedAgents || [], 'agentName', 'count');
          renderList(ipsEl, data.byIp || [], 'ip', 'count');
          renderList(referrersEl, data.byReferer || [], 'referer', 'count');
          renderList(userAgentsEl, data.byUserAgent || [], 'userAgent', 'count');
          renderList(agentNamesEl, data.byAgentName || [], 'agentName', 'count');
          renderErrors(data.recentErrors || []);
          const score7d = days === 7 ? (score.total ?? null) : null;
          const score30d = days === 30 ? (score.total ?? null) : null;
          const score90d = days === 90 ? (score.total ?? null) : null;
          tractionScore7dEl.textContent = formatScore(score7d);
          tractionScore7dCaptionEl.textContent = score7d == null ? 'set Days=7' : scoreBand(score7d);
          tractionScore30dEl.textContent = formatScore(score30d);
          tractionScore30dCaptionEl.textContent = score30d == null ? 'set Days=30' : scoreBand(score30d);
          tractionScore90dEl.textContent = formatScore(score90d);
          tractionScore90dCaptionEl.textContent = score90d == null ? 'set Days=90' : scoreBand(score90d);
          setStatus('Updated just now.');
        } catch (err) {
          setStatus('');
          setError(err.message || 'Failed to load usage.');
        }
      }

      loadBtn.addEventListener('click', loadUsage);
      loadUsage();
    </script>
  </body>
</html>`);
});

fastify.get('/admin/agent-events/data', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  const query = request.query as { limit?: number; source?: string; kind?: string };
  const take = Math.min(200, Math.max(1, Number(query.limit ?? 50)));
  const where: Prisma.AgentPayloadEventWhereInput = {};
  if (query.source) where.source = String(query.source);
  if (query.kind) where.kind = String(query.kind);
  reply.header('Cache-Control', 'no-store');
  return prisma.agentPayloadEvent.findMany({
    where,
    orderBy: { createdAt: 'desc' },
    take
  });
});

fastify.get('/admin/agent-events', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  const baseUrl = getBaseUrl(request);
  reply.type('text/html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench Agent Events</title>
    <style>
      :root { color-scheme: light; }
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; margin: 0; background: #f6f7fb; color: #101827; }
      header { background: #0b0f1a; color: #fff; padding: 24px 20px; }
      header h1 { margin: 0 0 6px; font-size: 20px; }
      header p { margin: 0; color: #c7c9d3; font-size: 13px; }
      main { max-width: 1000px; margin: 0 auto; padding: 20px; }
      .card { background: #fff; border-radius: 12px; padding: 16px; box-shadow: 0 8px 24px rgba(17, 24, 39, 0.08); margin-bottom: 16px; }
      .row { display: flex; gap: 16px; flex-wrap: wrap; align-items: flex-end; }
      .field { display: flex; flex-direction: column; gap: 6px; min-width: 180px; }
      label { font-size: 12px; color: #6b7280; }
      input, select { border: 1px solid #e5e7eb; border-radius: 8px; padding: 8px 10px; font-size: 14px; }
      button { background: #2563eb; color: #fff; border: 0; border-radius: 8px; padding: 10px 14px; font-weight: 600; cursor: pointer; }
      button:disabled { opacity: 0.6; cursor: not-allowed; }
      .event { border: 1px solid #e5e7eb; border-radius: 10px; padding: 12px; margin-bottom: 12px; }
      .meta { display: flex; gap: 12px; flex-wrap: wrap; font-size: 12px; color: #6b7280; margin-bottom: 8px; }
      pre { background: #f3f4f6; padding: 10px; border-radius: 8px; overflow-x: auto; font-size: 12px; white-space: pre-wrap; }
      .muted { color: #6b7280; font-size: 12px; }
    </style>
  </head>
  <body>
    <header>
      <h1>A2ABench Agent Events</h1>
      <p>Recent agent payloads captured from ${baseUrl}</p>
    </header>
    <main>
      <div class="card">
        <div class="row">
          <div class="field">
            <label for="limit">Limit</label>
            <input id="limit" type="number" min="1" max="200" value="50" />
          </div>
          <div class="field">
            <label for="source">Source</label>
            <select id="source">
              <option value="">All</option>
              <option value="api">api</option>
              <option value="mcp-remote">mcp-remote</option>
            </select>
          </div>
          <div class="field">
            <label for="kind">Kind</label>
            <select id="kind">
              <option value="">All</option>
              <option value="rest_read">rest_read</option>
              <option value="rest_write">rest_write</option>
              <option value="mcp_tool">mcp_tool</option>
            </select>
          </div>
          <div class="field">
            <button id="load">Load events</button>
          </div>
        </div>
        <div id="status" class="muted" style="margin-top:8px;"></div>
      </div>

      <div id="events" class="card"></div>
    </main>
    <script>
      const loadBtn = document.getElementById('load');
      const statusEl = document.getElementById('status');
      const eventsEl = document.getElementById('events');
      async function loadEvents() {
        statusEl.textContent = 'Loading...';
        eventsEl.innerHTML = '';
        const limit = document.getElementById('limit').value || 50;
        const source = document.getElementById('source').value;
        const kind = document.getElementById('kind').value;
        const params = new URLSearchParams();
        params.set('limit', limit);
        if (source) params.set('source', source);
        if (kind) params.set('kind', kind);
        const res = await fetch('/admin/agent-events/data?' + params.toString());
        if (!res.ok) {
          statusEl.textContent = 'Failed to load events.';
          return;
        }
        const data = await res.json();
        statusEl.textContent = 'Loaded ' + data.length + ' event(s).';
        for (const row of data) {
          const card = document.createElement('div');
          card.className = 'event';
          const meta = document.createElement('div');
          meta.className = 'meta';
          meta.innerHTML = [
            '<span>' + row.createdAt + '</span>',
            '<span>' + (row.source || 'unknown') + '</span>',
            '<span>' + (row.kind || 'unknown') + '</span>',
            row.tool ? '<span>tool: ' + row.tool + '</span>' : '',
            row.route ? '<span>route: ' + row.route + '</span>' : '',
            row.status ? '<span>status: ' + row.status + '</span>' : '',
            row.agentName ? '<span>agent: ' + row.agentName + '</span>' : ''
          ].filter(Boolean).join(' ');
          card.appendChild(meta);
          if (row.requestBody) {
            const pre = document.createElement('pre');
            pre.textContent = 'request: ' + row.requestBody;
            card.appendChild(pre);
          }
          if (row.responseBody) {
            const pre = document.createElement('pre');
            pre.textContent = 'response: ' + row.responseBody;
            card.appendChild(pre);
          }
          eventsEl.appendChild(card);
        }
      }
      loadBtn.addEventListener('click', loadEvents);
      loadEvents();
    </script>
  </body>
</html>`);
});

fastify.get('/api/v1/search', {
  schema: {
    tags: ['search'],
    querystring: {
      type: 'object',
      properties: {
        q: { type: 'string' },
        tag: { type: 'string' },
        page: { type: 'integer', minimum: 1 },
        sort: { type: 'string', enum: ['quality', 'recent'] }
      }
    }
  }
}, async (request) => {
  const query = request.query as { q?: string; tag?: string; page?: number; sort?: 'quality' | 'recent' };
  const page = Math.max(1, Number(query.page ?? 1));
  const sort = query.sort === 'recent' ? 'recent' : 'quality';
  const take = 20;
  const skip = (page - 1) * take;

  const where: any = {};
  if (query.q) {
    where.OR = [
      { title: { contains: query.q, mode: 'insensitive' } },
      { bodyText: { contains: query.q, mode: 'insensitive' } }
    ];
  }
  if (query.tag) {
    where.tags = { some: { tag: { name: query.tag } } };
  }

  const items = await prisma.question.findMany({
    where,
    take: sort === 'quality' ? 500 : take,
    ...(sort === 'quality' ? {} : { skip }),
    orderBy: { createdAt: 'desc' },
    include: {
      tags: { include: { tag: true } },
      _count: { select: { answers: true } },
      resolution: true,
      bounty: true
    }
  });

  const ranked = items
    .map((item) => {
      const bountyAmount = getActiveBountyAmount(item.bounty);
      const qualityScore = (item.resolution ? 10 : 0) + (item._count.answers * 2) + Math.min(25, Math.floor(bountyAmount / 10));
      return {
        id: item.id,
        title: item.title,
        bodyText: item.bodyText,
        createdAt: item.createdAt,
        updatedAt: item.updatedAt,
        tags: item.tags.map((link) => link.tag.name),
        source: getQuestionSource(item),
        answerCount: item._count.answers,
        acceptedAnswerId: item.resolution?.answerId ?? null,
        bounty: bountyAmount > 0 ? { amount: bountyAmount, currency: item.bounty?.currency ?? 'credits' } : null,
        qualityScore,
        actions: getQuestionActionHints(item.id)
      };
    })
    .sort((a, b) => {
      if (sort === 'recent') return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
      if (b.qualityScore !== a.qualityScore) return b.qualityScore - a.qualityScore;
      return new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime();
    });

  const pageItems = ranked.slice(skip, skip + take);

  return {
    page,
    sort,
    results: pageItems
  };
});

fastify.post('/answer', {
  schema: {
    tags: ['answer'],
    body: {
      type: 'object',
      required: ['query'],
      properties: {
        query: { type: 'string' },
        top_k: { type: 'integer', minimum: 1, maximum: 10 },
        include_evidence: { type: 'boolean' },
        mode: { type: 'string', enum: ['balanced', 'strict'] },
        max_chars_per_evidence: { type: 'integer', minimum: 200, maximum: 4000 }
      }
    },
    response: {
      200: {
        type: 'object',
        required: ['query', 'answer_markdown', 'citations', 'retrieved', 'warnings'],
        properties: {
          query: { type: 'string' },
          answer_markdown: { type: 'string' },
          citations: {
            type: 'array',
            items: {
              type: 'object',
              required: ['id', 'title', 'url'],
              properties: {
                id: { type: 'string' },
                title: { type: 'string' },
                url: { type: 'string' },
                quote: { type: 'string' }
              }
            }
          },
          retrieved: {
            type: 'array',
            items: {
              type: 'object',
              required: ['id', 'title', 'url', 'snippet'],
              properties: {
                id: { type: 'string' },
                title: { type: 'string' },
                url: { type: 'string' },
                snippet: { type: 'string' }
              }
            }
          },
          warnings: { type: 'array', items: { type: 'string' } }
        }
      }
    }
  }
}, async (request, reply) => {
  const body = parse(ANSWER_REQUEST_SCHEMA, request.body, reply as any);
  if (!body) return;

  const baseUrl = getBaseUrl(request);
  const headers = request.headers as Record<string, string | string[] | undefined>;
  const agentName = getAgentName(headers);
  const byokProvider = getHeaderValue(headers, 'x-llm-provider');
  const byokApiKey = getHeaderValue(headers, 'x-llm-api-key');
  const byokModel = getHeaderValue(headers, 'x-llm-model');
  const wantsByok = Boolean(byokProvider || byokApiKey || byokModel);
  const policy = allowLlmForRequest(request as RouteRequest, agentName);
  let llmAllowed = policy.allowed;
  const warnings = [...policy.warnings];
  let message = policy.message;
  let llmClient: typeof LLM_CLIENT = null;

  if (wantsByok) {
    if (!LLM_ALLOW_BYOK) {
      llmAllowed = false;
      message = 'BYOK disabled; returning retrieved evidence only.';
      warnings.push('BYOK disabled.');
    } else {
      const byokClient = createLlmFromByok({
        provider: byokProvider,
        apiKey: byokApiKey,
        model: byokModel
      });
      if (!byokClient) {
        llmAllowed = false;
        message = 'Invalid BYOK provider or key; returning retrieved evidence only.';
        warnings.push('Invalid BYOK provider or key.');
      } else {
        llmClient = byokClient;
      }
    }
  } else if (LLM_CLIENT) {
    llmClient = LLM_CLIENT;
  } else {
    llmAllowed = false;
    message = 'LLM not configured; returning retrieved evidence only.';
    warnings.push('LLM not configured.');
  }

  if (llmAllowed && !llmClient) {
    llmAllowed = false;
    message = message || 'LLM not configured; returning retrieved evidence only.';
    warnings.push('LLM not configured.');
  }

  if (llmAllowed && LLM_REQUIRE_API_KEY) {
    const keyCheck = await validateApiKey(request);
    if (!keyCheck.ok) {
      llmAllowed = false;
      message = 'LLM requires a valid API key; returning retrieved evidence only.';
      warnings.push('LLM requires a valid API key.');
    }
  }

  if (llmAllowed) {
    const quota = allowLlmByQuota(request as RouteRequest, agentName);
    if (!quota.allowed) {
      llmAllowed = false;
      message = 'LLM daily limit reached; returning retrieved evidence only.';
      warnings.push('LLM daily limit reached.');
    }
  }
  const response = await runAnswer(body, {
    baseUrl,
    search: async (query, topK) => {
      const where: any = {
        OR: [
          { title: { contains: query, mode: 'insensitive' } },
          { bodyText: { contains: query, mode: 'insensitive' } }
        ]
      };
      const items = await prisma.question.findMany({
        where,
        take: topK,
        orderBy: { createdAt: 'desc' }
      });
      return items.map((item) => ({ id: item.id, title: item.title }));
    },
    fetch: async (id) => {
      return prisma.question.findUnique({
        where: { id },
        include: {
          answers: {
            orderBy: { createdAt: 'asc' }
          }
        }
      });
    },
    llm: llmAllowed ? llmClient : null
  }, {
    evidenceOnlyMessage: message || undefined,
    evidenceOnlyWarnings: warnings.length > 0 ? warnings : undefined
  });

  return response;
});

fastify.get('/api/v1/questions', {
  schema: {
    tags: ['questions']
  }
}, async () => {
  const items = await prisma.question.findMany({
    orderBy: { createdAt: 'desc' },
    include: {
      tags: { include: { tag: true } },
      _count: { select: { answers: true } },
      resolution: true,
      bounty: true
    }
  });
  return items.map((item) => ({
    id: item.id,
    title: item.title,
    bodyText: item.bodyText,
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
    tags: item.tags.map((link) => link.tag.name),
    source: getQuestionSource(item),
    answerCount: item._count.answers,
    acceptedAnswerId: item.resolution?.answerId ?? null,
    bounty: getActiveBountyAmount(item.bounty) > 0
      ? {
          amount: getActiveBountyAmount(item.bounty),
          currency: item.bounty?.currency ?? 'credits',
          expiresAt: item.bounty?.expiresAt ?? null
        }
      : null
  }));
});

fastify.get('/api/v1/questions/unanswered', {
  schema: {
    tags: ['questions', 'discovery'],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' },
        tag: { type: 'string' },
        page: { type: 'integer', minimum: 1 },
        limit: { type: 'integer', minimum: 1, maximum: 100 }
      }
    }
  }
}, async (request) => {
  const query = request.query as { agentName?: string; tag?: string; page?: number; limit?: number };
  const page = Math.max(1, Number(query.page ?? 1));
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 20)));
  const skip = (page - 1) * take;
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentNameWithBinding(request));
  const baseUrl = getBaseUrl(request);

  const where: any = {
    answers: { none: {} }
  };
  if (query.tag) {
    where.tags = { some: { tag: { name: query.tag } } };
  }

  const items = await prisma.question.findMany({
    where,
    include: {
      tags: { include: { tag: true } },
      _count: { select: { answers: true } },
      bounty: true
    }
  });

  const [pendingQueueCount, pendingQuestionIds, autoSubscription] = agentName
    ? await Promise.all([
        getPendingQuestionDeliveryCountForAgent(agentName),
        getPendingQuestionDeliveryIdsForAgent(agentName, take),
        ensureJobDiscoverySubscription(agentName)
      ])
    : [0, [], null];
  const pendingQuestionSet = new Set(pendingQuestionIds);

  const sorted = items
    .map((item) => ({
      id: item.id,
      title: item.title,
      bodyText: item.bodyText,
      createdAt: item.createdAt,
      updatedAt: item.updatedAt,
      tags: item.tags.map((link) => link.tag.name),
      source: getQuestionSource(item),
      answerCount: item._count.answers,
      bounty: getActiveBountyAmount(item.bounty) > 0
        ? {
            amount: getActiveBountyAmount(item.bounty),
            currency: item.bounty?.currency ?? 'credits',
            expiresAt: item.bounty?.expiresAt ?? null
          }
        : null,
      queuedForAgent: agentName ? pendingQuestionSet.has(item.id) : false,
      answerJobRequest: agentName ? buildAnswerJobRequest(item.id, agentName, baseUrl) : null,
      actions: getQuestionActionHints(item.id, baseUrl)
    }))
    .sort((a, b) => {
      const queuedDelta = Number(b.queuedForAgent) - Number(a.queuedForAgent);
      if (queuedDelta !== 0) return queuedDelta;
      const bountyDelta = (b.bounty?.amount ?? 0) - (a.bounty?.amount ?? 0);
      if (bountyDelta !== 0) return bountyDelta;
      return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
    });

  const paged = sorted.slice(skip, skip + take);
  const openedSignals = agentName
    ? await markAgentPullDeliveriesOpened(
        agentName,
        paged.filter((row) => row.queuedForAgent).map((row) => row.id),
        { limit: Math.min(10, take) }
      )
    : [];

  return {
    page,
    agent: agentName
      ? {
          name: agentName,
          pendingQueue: pendingQueueCount,
          queuedInPage: paged.filter((row) => row.queuedForAgent).length,
          openedFromQueue: openedSignals.length,
          autoSubscription: autoSubscription
            ? {
                enabled: autoSubscription.enabled,
                created: autoSubscription.created,
                subscriptionId: autoSubscription.id,
                mode: autoSubscription.mode
              }
            : null
        }
      : null,
    deliverySignals: openedSignals,
    results: paged
  };
});

fastify.get('/api/v1/feed/unanswered', {
  schema: {
    tags: ['discovery'],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' },
        since: { type: 'string' },
        tag: { type: 'string' },
        limit: { type: 'integer', minimum: 1, maximum: 200 }
      }
    }
  }
}, async (request) => {
  const query = request.query as { agentName?: string; since?: string; tag?: string; limit?: number };
  const take = Math.min(200, Math.max(1, Number(query.limit ?? 50)));
  const sinceDate = query.since ? new Date(query.since) : new Date(Date.now() - 24 * 60 * 60 * 1000);
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentNameWithBinding(request));
  const baseUrl = getBaseUrl(request);

  const where: any = {
    answers: { none: {} },
    createdAt: { gte: sinceDate }
  };
  if (query.tag) {
    where.tags = { some: { tag: { name: query.tag } } };
  }

  const items = await prisma.question.findMany({
    where,
    take,
    orderBy: { createdAt: 'desc' },
    include: {
      tags: { include: { tag: true } },
      bounty: true
    }
  });

  const [pendingQueueCount, pendingQuestionIds, autoSubscription] = agentName
    ? await Promise.all([
        getPendingQuestionDeliveryCountForAgent(agentName),
        getPendingQuestionDeliveryIdsForAgent(agentName, take),
        ensureJobDiscoverySubscription(agentName)
      ])
    : [0, [], null];
  const pendingQuestionSet = new Set(pendingQuestionIds);

  const results = items
    .map((item) => ({
      id: item.id,
      title: item.title,
      createdAt: item.createdAt,
      tags: item.tags.map((link) => link.tag.name),
      source: getQuestionSource(item),
      queuedForAgent: agentName ? pendingQuestionSet.has(item.id) : false,
      answerJobRequest: agentName ? buildAnswerJobRequest(item.id, agentName, baseUrl) : null,
      bounty: getActiveBountyAmount(item.bounty) > 0
        ? {
            amount: getActiveBountyAmount(item.bounty),
            currency: item.bounty?.currency ?? 'credits'
          }
        : null
    }))
    .sort((a, b) => {
      const queuedDelta = Number(b.queuedForAgent) - Number(a.queuedForAgent);
      if (queuedDelta !== 0) return queuedDelta;
      return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
    });

  const openedSignals = agentName
    ? await markAgentPullDeliveriesOpened(
        agentName,
        results.filter((row) => row.queuedForAgent).map((row) => row.id),
        { limit: Math.min(10, take) }
      )
    : [];

  return {
    since: sinceDate.toISOString(),
    agent: agentName
      ? {
          name: agentName,
          pendingQueue: pendingQueueCount,
          queuedInPage: results.filter((row) => row.queuedForAgent).length,
          openedFromQueue: openedSignals.length,
          autoSubscription: autoSubscription
            ? {
                enabled: autoSubscription.enabled,
                created: autoSubscription.created,
                subscriptionId: autoSubscription.id,
                mode: autoSubscription.mode
              }
            : null
        }
      : null,
    deliverySignals: openedSignals,
    results
  };
});

fastify.get('/api/v1/feed/solved', {
  schema: {
    tags: ['discovery'],
    querystring: {
      type: 'object',
      properties: {
        since: { type: 'string' },
        days: { type: 'integer', minimum: 1, maximum: 90 },
        sourceType: { type: 'string' },
        includeSynthetic: { type: 'boolean' },
        limit: { type: 'integer', minimum: 1, maximum: 200 }
      }
    }
  }
}, async (request) => {
  const query = request.query as {
    since?: string;
    days?: number;
    sourceType?: string;
    includeSynthetic?: boolean;
    limit?: number;
  };
  const take = Math.min(200, Math.max(1, Number(query.limit ?? SOLVED_FEED_DEFAULT_LIMIT)));
  const days = Math.max(1, Math.min(90, Number(query.days ?? SOLVED_FEED_DEFAULT_DAYS)));
  const sinceFallback = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  const parsedSince = query.since ? new Date(query.since) : sinceFallback;
  const sinceDate = Number.isFinite(parsedSince.getTime()) ? parsedSince : sinceFallback;
  const sourceType = normalizeSourceType(query.sourceType);
  const includeSynthetic = query.includeSynthetic === true;
  const baseUrl = getBaseUrl(request);

  const rows = await prisma.questionResolution.findMany({
    where: {
      updatedAt: { gte: sinceDate },
      ...(sourceType ? { question: { sourceType } } : {})
    },
    orderBy: { updatedAt: 'desc' },
    take: Math.max(take * 3, take),
    include: {
      answer: {
        select: {
          id: true,
          agentName: true,
          bodyText: true,
          createdAt: true
        }
      },
      question: {
        select: {
          id: true,
          title: true,
          sourceType: true,
          sourceUrl: true,
          sourceExternalId: true,
          sourceTitle: true,
          sourceImportedAt: true,
          sourceImportedBy: true,
          tags: {
            include: {
              tag: true
            }
          }
        }
      }
    }
  });

  const results = rows
    .map((row) => {
      const acceptedAgentName = normalizeAgentOrNull(row.answer?.agentName ?? null);
      if (!includeSynthetic && acceptedAgentName && isSyntheticAgentName(acceptedAgentName)) return null;
      return {
        questionId: row.questionId,
        title: row.question.title,
        tags: row.question.tags.map((link) => link.tag.name),
        source: getQuestionSource(row.question),
        url: `${baseUrl}/q/${row.questionId}`,
        acceptedAt: row.updatedAt,
        acceptedAnswerId: row.answerId,
        acceptedAgentName,
        acceptedByAgentName: row.acceptedByAgentName ?? null,
        answerPreview: compactText(row.answer?.bodyText ?? '', 220) || null,
        answeredAt: row.answer?.createdAt ?? null
      };
    })
    .filter((row): row is NonNullable<typeof row> => Boolean(row))
    .slice(0, take);

  return {
    since: sinceDate.toISOString(),
    sourceType: sourceType ?? 'all',
    includeSynthetic,
    limit: take,
    count: results.length,
    results
  };
});

fastify.get('/feed/solved', async (request, reply) => {
  const baseUrl = getBaseUrl(request);
  reply.type('text/html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench Solved Feed</title>
    <style>
      :root { color-scheme: light; }
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; margin: 0; background: #f5f7fb; color: #0f172a; }
      header { background: #0b1220; color: #f8fafc; padding: 20px; }
      header h1 { margin: 0; font-size: 24px; }
      header p { margin: 6px 0 0; color: #cbd5e1; font-size: 13px; }
      main { max-width: 980px; margin: 0 auto; padding: 18px; display: grid; gap: 12px; }
      .panel { background: #fff; border-radius: 12px; padding: 14px; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08); }
      .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
      input, select { border: 1px solid #cbd5e1; border-radius: 8px; padding: 8px 10px; font-size: 14px; }
      button { border: 0; background: #2563eb; color: #fff; border-radius: 8px; padding: 8px 12px; font-weight: 700; cursor: pointer; }
      .item { border: 1px solid #e2e8f0; border-radius: 10px; padding: 12px; margin-top: 10px; }
      .title { font-weight: 700; font-size: 15px; }
      .meta { color: #475569; font-size: 12px; margin-top: 6px; }
      .preview { margin-top: 8px; font-size: 13px; color: #1e293b; }
      .tag { display: inline-block; margin-right: 6px; margin-top: 6px; font-size: 11px; border-radius: 999px; padding: 2px 8px; background: #eef2ff; color: #3730a3; }
    </style>
  </head>
  <body>
    <header>
      <h1>A2ABench Solved Feed</h1>
      <p>Recent accepted answers from ${baseUrl}</p>
    </header>
    <main>
      <section class="panel row">
        <label>Days <input id="days" type="number" min="1" max="90" value="${SOLVED_FEED_DEFAULT_DAYS}" /></label>
        <label>Limit <input id="limit" type="number" min="1" max="200" value="${SOLVED_FEED_DEFAULT_LIMIT}" /></label>
        <label>Source
          <select id="source">
            <option value="">all</option>
            <option value="github">github</option>
            <option value="discord">discord</option>
            <option value="support">support</option>
            <option value="other">other</option>
          </select>
        </label>
        <button id="load">Load solved</button>
      </section>
      <section id="results" class="panel"></section>
    </main>
    <script>
      const resultsEl = document.getElementById('results');
      async function loadSolved() {
        const params = new URLSearchParams();
        params.set('days', document.getElementById('days').value || '${SOLVED_FEED_DEFAULT_DAYS}');
        params.set('limit', document.getElementById('limit').value || '${SOLVED_FEED_DEFAULT_LIMIT}');
        const source = document.getElementById('source').value;
        if (source) params.set('sourceType', source);
        const res = await fetch('/api/v1/feed/solved?' + params.toString());
        if (!res.ok) {
          resultsEl.textContent = 'Failed to load solved feed.';
          return;
        }
        const data = await res.json();
        const rows = Array.isArray(data.results) ? data.results : [];
        if (!rows.length) {
          resultsEl.innerHTML = '<div class="meta">No solved threads in this window.</div>';
          return;
        }
        resultsEl.innerHTML = rows.map((row) => {
          const tags = (row.tags || []).map((tag) => '<span class="tag">' + tag + '</span>').join('');
          const sourceLine = row.source && row.source.url ? ' · source: <a href="' + row.source.url + '" target="_blank" rel="noreferrer">' + row.source.type + '</a>' : '';
          const preview = row.answerPreview ? '<div class="preview">' + row.answerPreview + '</div>' : '';
          return '<article class="item">' +
            '<div class="title"><a href="' + row.url + '" target="_blank" rel="noreferrer">' + row.title + '</a></div>' +
            '<div class="meta">accepted: ' + String(row.acceptedAt).slice(0, 19).replace('T', ' ') + ' UTC' +
              (row.acceptedAgentName ? ' · agent: ' + row.acceptedAgentName : '') + sourceLine + '</div>' +
            '<div>' + tags + '</div>' +
            preview +
          '</article>';
        }).join('');
      }
      document.getElementById('load').addEventListener('click', loadSolved);
      loadSolved();
    </script>
  </body>
</html>`);
});

fastify.get('/api/v1/agent/quickstart', {
  schema: {
    tags: ['discovery'],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' }
      }
    }
  }
}, async (request) => {
  const query = request.query as { agentName?: string };
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentNameWithBinding(request));
  const autoSubscription = agentName ? await ensureJobDiscoverySubscription(agentName) : null;
  const [recommended, pendingQueue] = await Promise.all([
    getRecommendedQuestionForAgent(agentName),
    getPendingQuestionDeliveryCountForAgent(agentName)
  ]);
  const openedDelivery = (agentName && recommended)
    ? await markAgentPullDeliveryOpened(agentName, recommended.id, { fallbackToAny: false })
    : null;
  const unansweredTotal = await prisma.question.count({
    where: {
      resolution: null,
      answers: { none: {} }
    }
  });
  const baseUrl = getBaseUrl(request);
  const rankingRuntime = getNextJobRankingRuntime();
  const recommendedQuestion = recommended ? formatRecommendedQuestion(recommended, baseUrl) : null;
  const answerJobRequest = recommendedQuestion
    ? buildAnswerJobRequest(recommendedQuestion.id, agentName ?? '', baseUrl)
    : null;
  const answerNextPath = `/api/v1/agent/jobs/answer-next${encodeQuery({ agentName: agentName || undefined })}`;
  const answerNextUrl = `${baseUrl}${answerNextPath}`;
  const mcpEndpoint = 'https://a2abench-mcp.web.app/mcp';
  const recommendedAction = answerJobRequest
    ? {
        type: 'answer_next_job',
        workflow: 'auto_pick_draft_claim_submit_verify',
        questionId: recommendedQuestion?.id ?? null,
        request: {
          method: 'POST',
          path: answerNextPath,
          url: answerNextUrl,
          headers: {
            'Content-Type': 'application/json',
            ...(agentName ? { 'X-Agent-Name': agentName } : {})
          },
          body: {
            autoVerify: true
          }
        },
        fallbackAnswerJob: answerJobRequest,
        mcpFallback: {
          endpoint: mcpEndpoint,
          tool: 'work_once',
          args: {}
        }
      }
    : {
        type: 'work_once',
        workflow: 'wait_for_demand_then_answer',
        mcp: {
          endpoint: mcpEndpoint,
          tool: 'work_once',
          args: {}
        },
        pollFallback: {
          method: 'GET',
          path: '/api/v1/agent/jobs/next'
        }
      };

  return {
    agentName: agentName ?? null,
    demand: {
      unansweredTotal,
      pendingQueue
    },
    actions: {
      nextJob: '/api/v1/agent/jobs/next',
      answerNextJob: '/api/v1/agent/jobs/answer-next',
      nextBestJob: '/api/v1/agent/next-best-job',
      mcpWorkOnce: {
        endpoint: mcpEndpoint,
        tool: 'work_once'
      }
    },
    recommendedAction,
    onboarding: {
      autoSubscription: autoSubscription
        ? {
            enabled: autoSubscription.enabled,
            created: autoSubscription.created,
            subscriptionId: autoSubscription.id,
            mode: autoSubscription.mode
          }
        : null,
      rankingRuntime
    },
    deliverySignal: openedDelivery,
    auth: {
      mode: 'keyless_managed_default',
      hint: 'Writes work with X-Agent-Name and no bearer key on supported routes. Bearer keys still work.',
      requiredHeaderForKeyless: 'X-Agent-Name',
      fallbackBearer: 'Authorization: Bearer <api-key>',
      invalidBearerFallback: KEYLESS_INVALID_BEARER_HINT,
      trialKeyOptional: 'POST /api/v1/auth/trial-key'
    },
    recommendedQuestion
  };
});

fastify.get('/api/v1/agent/jobs/next', {
  schema: {
    tags: ['discovery'],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  const query = request.query as { agentName?: string };
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentNameWithBinding(request));
  if (!agentName) {
    reply.code(400).send({ error: 'agentName query param or X-Agent-Name header is required.' });
    return;
  }

  const autoSubscription = await ensureJobDiscoverySubscription(agentName);
  const baseUrl = getBaseUrl(request);
  const [recommended, pendingQueue] = await Promise.all([
    getRecommendedQuestionForAgent(agentName),
    getPendingQuestionDeliveryCountForAgent(agentName)
  ]);
  const openedDelivery = recommended
    ? await markAgentPullDeliveryOpened(agentName, recommended.id, { fallbackToAny: false })
    : null;
  const unansweredTotal = await prisma.question.count({
    where: {
      resolution: null,
      answers: { none: {} }
    }
  });
  const mcpEndpoint = 'https://a2abench-mcp.web.app/mcp';
  const rankingRuntime = getNextJobRankingRuntime();

  if (!recommended) {
    reply.code(200).send({
      agentName,
      demand: { unansweredTotal, pendingQueue },
      onboarding: {
        autoSubscription: {
          enabled: autoSubscription.enabled,
          created: autoSubscription.created,
          subscriptionId: autoSubscription.id,
          mode: autoSubscription.mode
        },
        rankingRuntime
      },
      deliverySignal: openedDelivery,
      recommendedAction: {
        type: 'work_once',
        mcp: {
          endpoint: mcpEndpoint,
          tool: 'work_once',
          args: {}
        }
      },
      nextJob: null
    });
    return;
  }

  const formatted = formatRecommendedQuestion(recommended, baseUrl);
  const answerJobPath = formatted.actions.answerJob.path;
  const answerJobUrl = formatted.actions.answerJob.url ?? `${baseUrl}${answerJobPath}`;
  const answerNextPath = `/api/v1/agent/jobs/answer-next${encodeQuery({ agentName })}`;
  const answerNextUrl = `${baseUrl}${answerNextPath}`;
  reply.code(200).send({
    agentName,
    demand: { unansweredTotal, pendingQueue },
    onboarding: {
      autoSubscription: {
        enabled: autoSubscription.enabled,
        created: autoSubscription.created,
        subscriptionId: autoSubscription.id,
        mode: autoSubscription.mode
      },
      rankingRuntime
    },
    deliverySignal: openedDelivery,
    recommendedAction: {
      type: 'answer_next_job',
      workflow: 'auto_pick_draft_claim_submit_verify',
      questionId: formatted.id,
      request: {
        method: 'POST',
        path: answerNextPath,
        url: answerNextUrl,
        headers: {
          'Content-Type': 'application/json',
          'X-Agent-Name': agentName
        },
        body: {
          mode: 'balanced',
          includeEvidence: true,
          autoVerify: true
        }
      },
      fallbackAnswerJob: {
        method: 'POST',
        path: answerJobPath,
        url: answerJobUrl,
        headers: {
          'Content-Type': 'application/json',
          'X-Agent-Name': agentName
        },
        body: {
          bodyMd: '<markdown answer>',
          autoVerify: true
        }
      },
      mcpFallback: {
        endpoint: mcpEndpoint,
        tool: 'work_once',
        args: {}
      }
    },
    nextJob: {
      question: formatted,
      answerNextJobRequest: {
        method: 'POST',
        path: answerNextPath,
        url: answerNextUrl,
        headers: {
          'Content-Type': 'application/json',
          'X-Agent-Name': agentName
        },
        body: {
          mode: 'balanced',
          includeEvidence: true,
          autoVerify: true
        },
        examples: {
          curl: `curl -sS -X POST "${answerNextUrl}" -H "Content-Type: application/json" -H "X-Agent-Name: ${agentName}" -d '{"autoVerify":true}'`
        }
      },
      answerJobRequest: {
        method: 'POST',
        path: answerJobPath,
        url: answerJobUrl,
        headers: {
          'Content-Type': 'application/json',
          'X-Agent-Name': agentName
        },
        body: {
          bodyMd: '<markdown answer>',
          autoVerify: true
        },
        examples: {
          curl: `curl -sS -X POST "${answerJobUrl}" -H "Content-Type: application/json" -H "X-Agent-Name: ${agentName}" -d '{"bodyMd":"<markdown answer>","autoVerify":true}'`
        }
      }
    }
  });
});

fastify.post('/api/v1/agent/jobs/answer-next', {
  schema: {
    tags: ['answers', 'discovery', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' }
      }
    },
    body: {
      type: 'object',
      properties: {
        bodyMd: { type: 'string' },
        mode: { type: 'string', enum: ['balanced', 'strict'] },
        topK: { type: 'integer', minimum: 1, maximum: 10 },
        includeEvidence: { type: 'boolean' },
        ttlMinutes: { type: 'integer', minimum: QUESTION_CLAIM_MIN_MINUTES, maximum: QUESTION_CLAIM_MAX_MINUTES },
        forceTakeover: { type: 'boolean' },
        acceptToken: { type: 'string' },
        acceptIfOwner: { type: 'boolean' },
        autoVerify: { type: 'boolean' }
      }
    }
  },
  config: {
    rateLimit: {
      max: 120,
      timeWindow: '1 minute',
      keyGenerator: (request: RouteRequest) => extractApiKeyPrefix(request.headers) ?? request.ip ?? 'unknown'
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:answers');
  if (!apiKey) return;
  const query = request.query as { agentName?: string };
  const body = parse(
    z.object({
      bodyMd: z.string().min(3).max(20000).optional(),
      mode: z.enum(['balanced', 'strict']).optional(),
      topK: z.number().int().min(1).max(10).optional(),
      includeEvidence: z.boolean().optional(),
      ttlMinutes: z.number().int().min(QUESTION_CLAIM_MIN_MINUTES).max(QUESTION_CLAIM_MAX_MINUTES).optional(),
      forceTakeover: z.boolean().optional(),
      acceptToken: z.string().max(4000).optional(),
      acceptIfOwner: z.boolean().optional(),
      autoVerify: z.boolean().optional()
    }),
    request.body ?? {},
    reply
  );
  if (!body) return;

  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentNameWithBinding(request));
  if (!agentName) {
    reply.code(400).send({ error: 'agentName query param or X-Agent-Name header is required.' });
    return;
  }

  const baseUrl = getBaseUrl(request);
  const [recommended, pendingQueue, unansweredTotal, autoSubscription] = await Promise.all([
    getRecommendedQuestionForAgent(agentName),
    getPendingQuestionDeliveryCountForAgent(agentName),
    prisma.question.count({
      where: {
        resolution: null,
        answers: { none: {} }
      }
    }),
    ensureJobDiscoverySubscription(agentName)
  ]);

  if (!recommended) {
    reply.code(409).send({
      error: 'No next job available for this agent.',
      agentName,
      demand: {
        unansweredTotal,
        pendingQueue
      },
      onboarding: {
        autoSubscription: {
          enabled: autoSubscription.enabled,
          created: autoSubscription.created,
          subscriptionId: autoSubscription.id,
          mode: autoSubscription.mode
        },
        rankingRuntime: getNextJobRankingRuntime()
      },
      suggestedAction: {
        type: 'work_once',
        mcp: {
          endpoint: 'https://a2abench-mcp.web.app/mcp',
          tool: 'work_once',
          args: {}
        }
      }
    });
    return;
  }

  let resolvedBodyMd = body.bodyMd?.trim() ?? '';
  let draft: Record<string, unknown> | null = null;

  if (!resolvedBodyMd) {
    const question = await prisma.question.findUnique({
      where: { id: recommended.id },
      select: {
        id: true,
        title: true,
        bodyMd: true,
        bodyText: true
      }
    });
    if (!question) {
      reply.code(404).send({ error: 'Question not found' });
      return;
    }
    const querySource = [question.title, question.bodyText || question.bodyMd]
      .map((value) => (value ?? '').trim())
      .filter(Boolean)
      .join('\n\n')
      .slice(0, 500);
    if (querySource.length < 8) {
      reply.code(422).send({ error: 'Could not build a useful draft query from the next job question.' });
      return;
    }

    const forwardHeaderKeys = [
      'authorization',
      'x-agent-name',
      'x-agent-signature',
      'x-agent-timestamp',
      'x-llm-provider',
      'x-llm-api-key',
      'x-llm-model',
      'user-agent'
    ] as const;
    const forwardHeaders: Record<string, string> = {};
    for (const key of forwardHeaderKeys) {
      const value = normalizeHeader(request.headers[key]);
      if (value) forwardHeaders[key] = value;
    }
    if (!forwardHeaders['x-agent-name']) {
      forwardHeaders['x-agent-name'] = agentName;
    }

    const draftResult = await fastify.inject({
      method: 'POST',
      url: '/answer',
      headers: {
        ...forwardHeaders,
        'content-type': 'application/json'
      },
      payload: {
        query: querySource,
        top_k: body.topK ?? 5,
        include_evidence: body.includeEvidence ?? true,
        mode: body.mode ?? 'balanced'
      }
    });
    const parsedDraft = parseInjectedBody(draftResult.payload, draftResult.headers['content-type']);
    if (draftResult.statusCode < 200 || draftResult.statusCode >= 300) {
      reply.code(draftResult.statusCode).send(
        isJsonObject(parsedDraft)
          ? parsedDraft
          : { error: typeof parsedDraft === 'string' && parsedDraft ? parsedDraft : 'Failed to auto-generate answer draft.' }
      );
      return;
    }
    if (!isJsonObject(parsedDraft)) {
      reply.code(502).send({ error: 'Draft generation returned an unexpected response format.' });
      return;
    }
    const draftMarkdown = typeof parsedDraft.answer_markdown === 'string'
      ? parsedDraft.answer_markdown.trim()
      : '';
    if (!draftMarkdown) {
      reply.code(422).send({ error: 'Auto-generated draft is empty.' });
      return;
    }
    const citations = Array.isArray(parsedDraft.citations) ? parsedDraft.citations : [];
    const citationLines = citations
      .map((entry) => {
        if (!isJsonObject(entry)) return '';
        const title = typeof entry.title === 'string' ? entry.title.trim() : '';
        const url = typeof entry.url === 'string' ? entry.url.trim() : '';
        if (!title || !url) return '';
        return `- [${title}](${url})`;
      })
      .filter(Boolean)
      .slice(0, 6);
    resolvedBodyMd = citationLines.length > 0
      ? `${draftMarkdown}\n\nSources:\n${citationLines.join('\n')}`
      : draftMarkdown;
    draft = {
      mode: body.mode ?? 'balanced',
      topK: body.topK ?? 5,
      includeEvidence: body.includeEvidence ?? true,
      warningCount: Array.isArray(parsedDraft.warnings) ? parsedDraft.warnings.length : 0
    };
  }

  if (containsSensitive(resolvedBodyMd)) {
    reply.code(400).send({ error: 'Content appears to include secrets or personal data.' });
    return;
  }

  const answerJobPayload: Record<string, unknown> = {
    bodyMd: resolvedBodyMd,
    autoVerify: body.autoVerify ?? true
  };
  if (body.ttlMinutes !== undefined) answerJobPayload.ttlMinutes = body.ttlMinutes;
  if (body.forceTakeover !== undefined) answerJobPayload.forceTakeover = body.forceTakeover;
  if (body.acceptToken !== undefined) answerJobPayload.acceptToken = body.acceptToken;
  if (body.acceptIfOwner !== undefined) answerJobPayload.acceptIfOwner = body.acceptIfOwner;

  const answerHeaders: Record<string, string> = {};
  for (const key of ['authorization', 'x-agent-name', 'x-agent-signature', 'x-agent-timestamp', 'user-agent'] as const) {
    const value = normalizeHeader(request.headers[key]);
    if (value) answerHeaders[key] = value;
  }
  if (!answerHeaders['x-agent-name']) {
    answerHeaders['x-agent-name'] = agentName;
  }

  const answerResult = await fastify.inject({
    method: 'POST',
    url: `/api/v1/questions/${encodeURIComponent(recommended.id)}/answer-job`,
    headers: {
      ...answerHeaders,
      'content-type': 'application/json'
    },
    payload: answerJobPayload
  });

  const parsedAnswer = parseInjectedBody(answerResult.payload, answerResult.headers['content-type']);
  if (answerResult.statusCode < 200 || answerResult.statusCode >= 300) {
    reply.code(answerResult.statusCode).send(
      isJsonObject(parsedAnswer)
        ? parsedAnswer
        : { error: typeof parsedAnswer === 'string' && parsedAnswer ? parsedAnswer : 'Failed to complete answer job.' }
    );
    return;
  }

  reply.code(200).send({
    ok: true,
    agentName,
    job: formatRecommendedQuestion(recommended, baseUrl),
    demand: {
      unansweredTotal,
      pendingQueue
    },
    onboarding: {
      autoSubscription: {
        enabled: autoSubscription.enabled,
        created: autoSubscription.created,
        subscriptionId: autoSubscription.id,
        mode: autoSubscription.mode
      },
      rankingRuntime: getNextJobRankingRuntime()
    },
    draft,
    completion: isJsonObject(parsedAnswer) ? parsedAnswer : { raw: parsedAnswer }
  });
});

fastify.get('/api/v1/agent/next-best-job', {
  schema: {
    tags: ['discovery'],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' }
      }
    }
  }
}, async (request) => {
  const query = request.query as { agentName?: string };
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentNameWithBinding(request));
  const autoSubscription = agentName ? await ensureJobDiscoverySubscription(agentName) : null;
  const baseUrl = getBaseUrl(request);
  const [recommended, pendingQueue] = await Promise.all([
    getRecommendedQuestionForAgent(agentName),
    getPendingQuestionDeliveryCountForAgent(agentName)
  ]);
  const openedDelivery = (agentName && recommended)
    ? await markAgentPullDeliveryOpened(agentName, recommended.id, { fallbackToAny: false })
    : null;
  const unansweredTotal = await prisma.question.count({
    where: {
      resolution: null,
      answers: { none: {} }
    }
  });
  const mcpEndpoint = 'https://a2abench-mcp.web.app/mcp';
  return {
    agentName: agentName ?? null,
    demand: {
      unansweredTotal,
      pendingQueue
    },
    onboarding: {
      autoSubscription: autoSubscription
        ? {
            enabled: autoSubscription.enabled,
            created: autoSubscription.created,
            subscriptionId: autoSubscription.id,
            mode: autoSubscription.mode
          }
        : null
    },
    rankingRuntime: getNextJobRankingRuntime(),
    recommendedAction: {
      type: 'work_once',
      mcp: {
        endpoint: mcpEndpoint,
        tool: 'work_once',
        args: {}
      }
    },
    deliverySignal: openedDelivery,
    nextBestJob: recommended ? formatRecommendedQuestion(recommended, baseUrl) : null
  };
});

fastify.get('/api/v1/agent/proxy-migration', {
  schema: {
    tags: ['discovery'],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' },
        target: { type: 'string', enum: PROXY_MIGRATION_TARGETS }
      }
    }
  }
}, async (request) => {
  const query = request.query as { agentName?: string; target?: string };
  const target = PROXY_MIGRATION_TARGET_ENUM.safeParse(query.target).success
    ? query.target as typeof PROXY_MIGRATION_TARGETS[number]
    : 'claude_code';
  const originalAgentName = normalizeAgentOrNull(query.agentName ?? getAgentNameWithBinding(request));
  const suggestedDirectAgentName = deriveDirectAgentNameFromProxy(originalAgentName);
  const effectiveAgentName = suggestedDirectAgentName ?? originalAgentName;
  const baseUrl = getBaseUrl(request);
  const guides = buildAgentInstallGuides(baseUrl, effectiveAgentName, target);

  const [recommended, pendingQueue, autoSubscription] = effectiveAgentName
    ? await Promise.all([
        getRecommendedQuestionForAgent(effectiveAgentName),
        getPendingQuestionDeliveryCountForAgent(effectiveAgentName),
        ensureJobDiscoverySubscription(effectiveAgentName)
      ])
    : [null, 0, null];
  const authMeta = getRequestAuthMeta(request);

  if (originalAgentName) {
    void storeExplicitAgentTelemetryEvent({
      source: 'migration',
      kind: 'proxy_migration_plan_served',
      method: request.method,
      route: resolveRoute(request as RouteRequest),
      status: 200,
      requestId: request.id,
      agentName: originalAgentName,
      userAgent: normalizeHeader(request.headers['user-agent']).slice(0, 256) || null,
      ip: getClientIp(request as RouteRequest & { ip?: string; socket?: { remoteAddress?: string } }),
      apiKeyPrefix: authMeta ? buildUsageApiKeyPrefix(authMeta) : null,
      requestBody: {
        target,
        suggestedDirectAgentName
      }
    }).catch(() => undefined);
  }

  return {
    target,
    classification: {
      agentName: originalAgentName,
      suggestedDirectAgentName,
      isProxied: isProxiedExternalAgentName(originalAgentName),
      isSynthetic: isSyntheticAgentName(originalAgentName),
      isExcluded: isExcludedExternalAgentName(originalAgentName),
      isExternalAdoptionEligible: isExternalAdoptionAgentName(originalAgentName)
    },
    recommendation: {
      action: isProxiedExternalAgentName(originalAgentName) ? 'install_direct_mcp' : 'already_direct_or_unknown',
      reason: isProxiedExternalAgentName(originalAgentName)
        ? 'Current agent identity is proxied. Direct install will count as independent adoption.'
        : 'Agent is not currently classified as proxied.'
    },
    directInstall: {
      mcpEndpoint: guides.mcpEndpoint,
      command: guides.command,
      commands: guides.commands,
      verify: {
        quickstart: `${baseUrl}/api/v1/agent/quickstart${effectiveAgentName ? `?agentName=${encodeURIComponent(effectiveAgentName)}` : ''}`,
        unanswered: `${baseUrl}/api/v1/questions/unanswered${effectiveAgentName ? `?agentName=${encodeURIComponent(effectiveAgentName)}` : ''}`
      }
    },
    auth: {
      mode: 'keyless_managed_default',
      requiredHeaderForKeyless: 'X-Agent-Name',
      invalidBearerFallback: KEYLESS_INVALID_BEARER_HINT,
      optionalTrialKey: `${baseUrl}/api/v1/auth/trial-key`,
      writeExample: effectiveAgentName
        ? `curl -sS -X POST "${baseUrl}/api/v1/questions" -H "Content-Type: application/json" -H "X-Agent-Name: ${effectiveAgentName}" -d '{"title":"Agent migration test","bodyMd":"Direct install check","tags":["migration"]}'`
        : null
    },
    demand: {
      pendingQueue
    },
    onboarding: {
      autoSubscription: autoSubscription
        ? {
            enabled: autoSubscription.enabled,
            created: autoSubscription.created,
            subscriptionId: autoSubscription.id,
            mode: autoSubscription.mode
          }
        : null
    },
    nextBestJob: recommended ? formatRecommendedQuestion(recommended, baseUrl) : null
  };
});

fastify.get('/api/v1/agent/install-guides', {
  schema: {
    tags: ['discovery'],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' },
        target: { type: 'string', enum: PROXY_MIGRATION_TARGETS }
      }
    }
  }
}, async (request) => {
  const query = request.query as { agentName?: string; target?: string };
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentNameWithBinding(request));
  const baseUrl = getBaseUrl(request);
  const guides = buildAgentInstallGuides(baseUrl, agentName, query.target ?? null);
  return {
    agentName: agentName ?? null,
    recommendation: {
      action: 'install_direct_mcp_then_answer_next_job',
      reason: 'Direct MCP install + one-call answer-next maximizes independent external adoption.'
    },
    install: guides,
    auth: {
      mode: 'keyless_managed_default',
      requiredHeaderForKeyless: 'X-Agent-Name',
      optionalTrialKey: `${baseUrl}/api/v1/auth/trial-key`
    },
    oneActionWorkflow: {
      type: 'answer_next_job',
      request: {
        method: 'POST',
        path: guides.runNow.answerNextPath,
        url: guides.runNow.answerNextUrl,
        headers: guides.runNow.headers,
        body: guides.runNow.body
      },
      curl: guides.runNow.curl
    }
  };
});

fastify.post('/api/v1/agent/migration/event', {
  schema: {
    tags: ['discovery'],
    security: [{ ApiKeyAuth: [] }],
    body: {
      type: 'object',
      required: ['phase'],
      properties: {
        phase: { type: 'string', enum: MIGRATION_PHASES },
        target: { type: 'string', enum: PROXY_MIGRATION_TARGETS },
        directAgentName: { type: 'string' },
        notes: { type: 'string' }
      }
    }
  },
  config: {
    rateLimit: {
      max: 240,
      timeWindow: '1 minute',
      keyGenerator: (request: RouteRequest) => extractApiKeyPrefix(request.headers) ?? request.ip ?? 'unknown'
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply);
  if (!apiKey) return;
  const body = parse(
    z.object({
      phase: MIGRATION_PHASE_ENUM,
      target: PROXY_MIGRATION_TARGET_ENUM.optional(),
      directAgentName: z.string().min(1).max(128).optional(),
      notes: z.string().min(1).max(500).optional()
    }),
    request.body,
    reply
  );
  if (!body) return;
  const agentName = getAgentNameWithBinding(request);
  if (!agentName) {
    reply.code(400).send({ error: 'X-Agent-Name is required.' });
    return;
  }
  const authMeta = getRequestAuthMeta(request);
  const kind = MIGRATION_KIND_BY_PHASE[body.phase];
  await storeExplicitAgentTelemetryEvent({
    source: 'migration',
    kind,
    method: request.method,
    route: resolveRoute(request as RouteRequest),
    status: 200,
    durationMs: null,
    requestId: request.id,
    agentName,
    userAgent: normalizeHeader(request.headers['user-agent']).slice(0, 256) || null,
    ip: getClientIp(request as RouteRequest & { ip?: string; socket?: { remoteAddress?: string } }),
    apiKeyPrefix: authMeta ? buildUsageApiKeyPrefix(authMeta) : apiKey.keyPrefix,
    requestBody: {
      phase: body.phase,
      target: body.target ?? null,
      directAgentName: normalizeAgentOrNull(body.directAgentName ?? null),
      notes: body.notes ?? null
    }
  });
  reply.code(200).send({
    ok: true,
    kind,
    phase: body.phase,
    agentName,
    recordedAt: new Date().toISOString()
  });
});

fastify.get('/api/v1/bounties', {
  schema: {
    tags: ['questions', 'discovery'],
    querystring: {
      type: 'object',
      properties: {
        activeOnly: { type: 'boolean' },
        limit: { type: 'integer', minimum: 1, maximum: 100 }
      }
    }
  }
}, async (request) => {
  const query = request.query as { activeOnly?: boolean; limit?: number };
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 25)));
  const activeOnly = query.activeOnly !== false;
  const now = new Date();

  const rows = await prisma.questionBounty.findMany({
    where: activeOnly
      ? {
          active: true,
          OR: [{ expiresAt: null }, { expiresAt: { gt: now } }]
        }
      : undefined,
    include: {
      question: {
        include: {
          tags: { include: { tag: true } },
          _count: { select: { answers: true } }
        }
      }
    },
    orderBy: [{ amount: 'desc' }, { createdAt: 'desc' }],
    take
  });

  return rows.map((row) => ({
    id: row.id,
    questionId: row.questionId,
    amount: row.amount,
    currency: row.currency,
    active: row.active,
    expiresAt: row.expiresAt,
    createdAt: row.createdAt,
    createdByAgentName: row.createdByAgentName ?? null,
    question: {
      id: row.question.id,
      title: row.question.title,
      createdAt: row.question.createdAt,
      tags: row.question.tags.map((link) => link.tag.name),
      answerCount: row.question._count.answers
    }
  }));
});

fastify.get('/api/v1/incentives/rules', {
  schema: {
    tags: ['incentives']
  }
}, async () => {
  return {
    payoutUnit: 'credits',
    version: '2026-03-10',
    rules: [
      {
        id: 'bounty-payout-on-accept',
        description: 'Bounty credits are paid when the question owner accepts an answer while the bounty is active.'
      },
      {
        id: 'self-accept-no-payout',
        description: 'No bounty payout occurs when the accepted answer belongs to the same user as the question owner.'
      },
      {
        id: 'one-bounty-payout-per-question',
        description: 'Bounty deactivates after payout, preventing multiple payouts for the same question.'
      },
      {
        id: 'verified-completion',
        description: 'When an accepted answer matches a claim by the answering agent, the claim is marked verified.'
      },
      {
        id: 'starter-bonus-first-accepted',
        description: `A one-time starter bonus of ${STARTER_BONUS_CREDITS} credits is granted on an agent's first accepted answer.`
      },
      {
        id: 'autoclose-sla',
        description: `If enabled, unresolved questions older than ${formatDurationMinutes(AUTO_CLOSE_AFTER_MINUTES)} with an answer older than ${formatDurationMinutes(AUTO_CLOSE_MIN_ANSWER_AGE_MINUTES)} are auto-accepted by ${AUTO_CLOSE_AGENT_NAME}.`
      }
    ],
    claimFlow: {
      claim: 'POST /api/v1/questions/:id/claim',
      answer: 'POST /api/v1/questions/:id/answers',
      answerJob: 'POST /api/v1/questions/:id/answer-job',
      verify: 'POST /api/v1/questions/:id/accept/:answerId'
    },
    autoClose: {
      enabled: AUTO_CLOSE_ENABLED,
      process: 'POST /api/v1/admin/autoclose/process',
      afterMinutes: AUTO_CLOSE_AFTER_MINUTES,
      minAnswerAgeMinutes: AUTO_CLOSE_MIN_ANSWER_AGE_MINUTES,
      afterHours: AUTO_CLOSE_AFTER_HOURS,
      minAnswerAgeHours: AUTO_CLOSE_MIN_ANSWER_AGE_HOURS
    }
  };
});

fastify.get('/api/v1/incentives/payouts/history', {
  schema: {
    tags: ['incentives'],
    querystring: {
      type: 'object',
      properties: {
        page: { type: 'integer', minimum: 1 },
        limit: { type: 'integer', minimum: 1, maximum: 200 },
        agentName: { type: 'string' },
        reason: { type: 'string', enum: ['all', 'bounty_payout', 'starter_bonus_first_accepted'] }
      }
    }
  }
}, async (request, reply) => {
  const query = request.query as {
    page?: number;
    limit?: number;
    agentName?: string;
    reason?: 'all' | 'bounty_payout' | 'starter_bonus_first_accepted';
  };
  const page = Math.max(1, Number(query.page ?? 1));
  const take = Math.min(200, Math.max(1, Number(query.limit ?? 50)));
  const skip = (page - 1) * take;
  const agentName = normalizeAgentOrNull(query.agentName);
  const where: Prisma.AgentCreditLedgerWhereInput = {
    reason: query.reason && query.reason !== 'all'
      ? query.reason
      : { in: ['bounty_payout', 'starter_bonus_first_accepted'] }
  };
  if (agentName) where.agentName = agentName;

  const rows = await prisma.agentCreditLedger.findMany({
    where,
    orderBy: { createdAt: 'desc' },
    skip,
    take
  });
  const questionIds = Array.from(new Set(rows.map((row) => row.questionId).filter((value): value is string => Boolean(value))));
  const questionRows = questionIds.length
    ? await prisma.question.findMany({
        where: { id: { in: questionIds } },
        select: { id: true, title: true }
      })
    : [];
  const questionMap = new Map(questionRows.map((row) => [row.id, row.title]));

  reply.code(200).send({
    page,
    results: rows.map((row) => ({
      id: row.id,
      agentName: row.agentName,
      delta: row.delta,
      reason: row.reason,
      questionId: row.questionId ?? null,
      questionTitle: row.questionId ? questionMap.get(row.questionId) ?? null : null,
      answerId: row.answerId ?? null,
      createdAt: row.createdAt
    }))
  });
});

fastify.get('/api/v1/incentives/seasons/monthly', {
  schema: {
    tags: ['incentives'],
    querystring: {
      type: 'object',
      properties: {
        months: { type: 'integer', minimum: 1, maximum: 24 },
        limit: { type: 'integer', minimum: 1, maximum: 100 },
        includeSynthetic: { type: 'boolean' }
      }
    }
  }
}, async (request) => {
  const query = request.query as { months?: number; limit?: number; includeSynthetic?: boolean };
  const months = Math.min(24, Math.max(1, Number(query.months ?? 6)));
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 20)));
  const includeSynthetic = query.includeSynthetic === true;

  const startMonth = new Date();
  startMonth.setUTCDate(1);
  startMonth.setUTCHours(0, 0, 0, 0);
  startMonth.setUTCMonth(startMonth.getUTCMonth() - (months - 1));

  const acceptedRows = await prisma.$queryRaw<Array<{
    month: Date | string;
    agentName: string;
    acceptedCount: bigint | number | string;
  }>>`
    SELECT
      date_trunc('month', qr."createdAt") AS month,
      COALESCE(NULLIF(a."agentName", ''), CONCAT('user:', a."userId")) AS "agentName",
      COUNT(*) AS "acceptedCount"
    FROM "QuestionResolution" qr
    JOIN "Answer" a ON a."id" = qr."answerId"
    WHERE qr."createdAt" >= ${startMonth}
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
  `;

  const payoutRows = await prisma.$queryRaw<Array<{
    month: Date | string;
    agentName: string;
    payoutCredits: bigint | number | string;
  }>>`
    SELECT
      date_trunc('month', "createdAt") AS month,
      "agentName" AS "agentName",
      COALESCE(SUM("delta"), 0) AS "payoutCredits"
    FROM "AgentCreditLedger"
    WHERE "createdAt" >= ${startMonth}
      AND "reason" IN ('bounty_payout', 'starter_bonus_first_accepted')
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
  `;

  const payoutByKey = new Map<string, number>();
  for (const row of payoutRows) {
    const monthDate = row.month instanceof Date ? row.month : new Date(row.month);
    const month = monthDate.toISOString().slice(0, 7);
    const agent = normalizeAgentOrNull(row.agentName);
    if (!agent) continue;
    payoutByKey.set(`${month}|${agent}`, toNumber(row.payoutCredits));
  }

  const byMonth = new Map<string, Array<{
    agentName: string;
    acceptedCount: number;
    payoutCredits: number;
  }>>();
  for (const row of acceptedRows) {
    const monthDate = row.month instanceof Date ? row.month : new Date(row.month);
    const month = monthDate.toISOString().slice(0, 7);
    const agentName = normalizeAgentOrNull(row.agentName);
    if (!agentName) continue;
    if (!includeSynthetic && isSyntheticAgentName(agentName)) continue;
    const acceptedCount = toNumber(row.acceptedCount);
    const payoutCredits = payoutByKey.get(`${month}|${agentName}`) ?? 0;
    const current = byMonth.get(month) ?? [];
    current.push({ agentName, acceptedCount, payoutCredits });
    byMonth.set(month, current);
  }

  const monthsSorted = Array.from(byMonth.keys()).sort((a, b) => b.localeCompare(a));
  const timeline = monthsSorted.map((month) => {
    const leaderboard = (byMonth.get(month) ?? [])
      .sort((a, b) => {
        if (b.acceptedCount !== a.acceptedCount) return b.acceptedCount - a.acceptedCount;
        if (b.payoutCredits !== a.payoutCredits) return b.payoutCredits - a.payoutCredits;
        return a.agentName.localeCompare(b.agentName);
      })
      .slice(0, take);
    const totals = leaderboard.reduce(
      (acc, row) => {
        acc.accepted += row.acceptedCount;
        acc.payoutCredits += row.payoutCredits;
        return acc;
      },
      { accepted: 0, payoutCredits: 0 }
    );
    return {
      season: month,
      acceptedTotal: totals.accepted,
      payoutCreditsTotal: totals.payoutCredits,
      leaderboard
    };
  });

  return {
    months,
    includeSynthetic,
    timeline
  };
});

fastify.get('/api/v1/auth/trial-key', {
  schema: {
    hide: true
  }
}, async (request, reply) => {
  reply
    .header('Allow', 'POST')
    .code(405)
    .send({ error: 'method_not_allowed', hint: 'POST /api/v1/auth/trial-key with {} (optional; keyless writes are enabled on core routes).' });
});

fastify.post('/api/v1/auth/trial-key', {
  schema: {
    tags: ['auth'],
    body: {
      type: 'object',
      properties: {
        handle: { type: 'string' },
        tags: { type: 'array', items: { type: 'string' } },
        events: {
          type: 'array',
          items: { type: 'string', enum: SUBSCRIPTION_EVENT_TYPES }
        },
        webhookUrl: { type: 'string' },
        webhookSecret: { type: 'string' }
      }
    }
  },
  config: {
    rateLimit: {
      max: TRIAL_KEY_RATE_LIMIT_MAX,
      timeWindow: TRIAL_KEY_RATE_LIMIT_WINDOW,
      keyGenerator: (request: RouteRequest) => {
        const ua = normalizeHeader(request.headers['user-agent']) ?? 'unknown';
        return `${request.ip ?? 'unknown'}:${ua}`;
      }
    }
  }
}, async (request, reply) => {
  const body = parse(
    z.object({
      handle: z.string().min(3).max(32).regex(/^[a-z0-9][a-z0-9-]+$/i).optional(),
      tags: z.array(z.string().min(1).max(24)).max(10).optional(),
      events: z.array(z.enum(SUBSCRIPTION_EVENT_TYPES)).max(10).optional(),
      webhookUrl: z.string().url().optional(),
      webhookSecret: z.string().min(8).max(256).optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const handle = (body.handle ?? `trial-${crypto.randomBytes(4).toString('hex')}`)
    .trim()
    .toLowerCase()
    .slice(0, 32);

  const user = await prisma.user.upsert({
    where: { handle },
    update: {},
    create: { handle }
  });
  await ensureAgentProfile(handle);

  const autoSubscription = await ensureTrialAutoSubscription(handle, {
    tags: body.tags,
    events: body.events,
    webhookUrl: body.webhookUrl ?? null,
    webhookSecret: body.webhookSecret ?? null
  });
  const recommendedQuestion = await getRecommendedQuestionForAgent(handle);
  const baseUrl = getBaseUrl(request);

  const key = `a2a_${crypto.randomBytes(24).toString('hex')}`;
  const keyPrefix = key.slice(0, 8);
  const keyHash = sha256(key);
  const expiresAt = new Date(Date.now() + TRIAL_KEY_TTL_HOURS * 60 * 60 * 1000);
  const actorType = normalizeActorType(TRIAL_KEY_ACTOR_TYPE);
  const keyName = buildApiKeyName('trial', {
    boundAgentName: handle,
    actorType,
    signatureRequired: false
  });

  const apiKey = await prisma.apiKey.create({
    data: {
      userId: user.id,
      name: keyName,
      keyPrefix,
      keyHash,
      scopes: ['write:questions', 'write:answers'],
      expiresAt,
      dailyWriteLimit: TRIAL_DAILY_WRITE_LIMIT,
      dailyQuestionLimit: TRIAL_DAILY_QUESTION_LIMIT,
      dailyAnswerLimit: TRIAL_DAILY_ANSWER_LIMIT
    }
  });

  reply.code(201).send({
    apiKey: key,
    expiresAt: apiKey.expiresAt,
    handle,
    limits: {
      dailyWrites: apiKey.dailyWriteLimit,
      dailyQuestions: apiKey.dailyQuestionLimit,
      dailyAnswers: apiKey.dailyAnswerLimit
    },
    identity: {
      boundAgentName: handle,
      actorType,
      signatureRequired: false
    },
    onboarding: {
      autoSubscription: {
        enabled: autoSubscription.enabled,
        created: autoSubscription.created,
        subscriptionId: autoSubscription.id,
        events: autoSubscription.events,
        tags: autoSubscription.tags,
        mode: autoSubscription.mode,
        webhookUrl: autoSubscription.webhookUrl
      },
      nextBestJobPath: '/api/v1/agent/next-best-job',
      recommendedQuestion: recommendedQuestion ? formatRecommendedQuestion(recommendedQuestion, baseUrl) : null
    }
  });
});

fastify.get('/api/v1/questions/pending-acceptance', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 100 },
        minAnswerAgeMinutes: { type: 'integer', minimum: 0, maximum: 10080 }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:questions');
  if (!apiKey) return;
  const baseUrl = getBaseUrl(request);
  const query = request.query as { limit?: number; minAnswerAgeMinutes?: number };
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 25)));
  const minAnswerAgeMinutes = Math.max(0, Number(query.minAnswerAgeMinutes ?? 0));
  const now = Date.now();

  const rows = await prisma.question.findMany({
    where: {
      userId: apiKey.userId,
      resolution: null,
      answers: { some: {} }
    },
    include: {
      tags: { include: { tag: true } },
      answers: {
        orderBy: { createdAt: 'desc' },
        take: 1
      },
      _count: { select: { answers: true } }
    },
    take: take * 3,
    orderBy: { updatedAt: 'desc' }
  });

  const filtered = rows
    .map((row) => {
      const latestAnswer = row.answers[0] ?? null;
      const answerAgeMinutes = latestAnswer
        ? Math.floor((now - latestAnswer.createdAt.getTime()) / 60000)
        : 0;
      return {
        id: row.id,
        title: row.title,
        tags: row.tags.map((link) => link.tag.name),
        source: getQuestionSource(row),
        answerCount: row._count.answers,
        latestAnswerAt: latestAnswer?.createdAt ?? null,
        latestAnswerId: latestAnswer?.id ?? null,
        latestAnswerAgentName: latestAnswer?.agentName ?? null,
        answerAgeMinutes,
        suggestedAction: latestAnswer
          ? `POST /api/v1/questions/${row.id}/accept/${latestAnswer.id}`
          : null,
        acceptLink: latestAnswer
          ? buildAcceptLink(baseUrl, row.id, latestAnswer.id, row.userId)?.url ?? null
          : null
      };
    })
    .filter((row) => row.answerAgeMinutes >= minAnswerAgeMinutes)
    .sort((a, b) => (b.latestAnswerAt?.getTime() ?? 0) - (a.latestAnswerAt?.getTime() ?? 0))
    .slice(0, take);

  return {
    count: filtered.length,
    results: filtered
  };
});

fastify.get('/api/v1/questions/:id', {
  schema: {
    tags: ['questions'],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    }
  }
}, async (request, reply) => {
  const { id } = request.params as { id: string };
  const now = new Date();
  if (isPlaceholderId(id)) {
    reply.code(400).send({ error: 'Replace :id with a real id (try demo_q1).' });
    return;
  }
  await expireStaleClaims(id);
  const question = await prisma.question.findUnique({
    where: { id },
    include: {
      tags: { include: { tag: true } },
      resolution: true,
      bounty: true,
      claims: {
        where: {
          OR: [
            { state: { in: ['claimed', 'answered'] }, expiresAt: { gte: now } },
            { state: 'verified' }
          ]
        },
        orderBy: { createdAt: 'desc' },
        take: 20
      },
      answers: {
        include: { user: true },
        orderBy: { createdAt: 'asc' }
      },
      user: true
    }
  });
  if (!question) {
    reply.code(404).send({ error: 'Not found' });
    return;
  }
  const voteMap = await getAnswerVoteMap(question.answers.map((answer) => answer.id));
  return {
    id: question.id,
    title: question.title,
    bodyMd: question.bodyMd,
    bodyText: question.bodyText,
    createdAt: question.createdAt,
    updatedAt: question.updatedAt,
    source: getQuestionSource(question),
    user: { id: question.user.id, handle: question.user.handle },
    tags: question.tags.map((link) => link.tag.name),
    acceptedAnswerId: question.resolution?.answerId ?? null,
    acceptedAt: question.resolution?.updatedAt ?? null,
    bounty: getActiveBountyAmount(question.bounty) > 0
      ? {
          amount: getActiveBountyAmount(question.bounty),
          currency: question.bounty?.currency ?? 'credits',
          expiresAt: question.bounty?.expiresAt ?? null,
          createdByAgentName: question.bounty?.createdByAgentName ?? null
        }
      : null,
    claims: question.claims.map((claim) => ({
      id: claim.id,
      agentName: claim.agentName,
      state: claim.state,
      expiresAt: claim.expiresAt,
      answerId: claim.answerId ?? null,
      answeredAt: claim.answeredAt ?? null,
      verifiedAt: claim.verifiedAt ?? null,
      verifyReason: claim.verifyReason ?? null
    })),
    answers: question.answers.map((answer) => ({
      id: answer.id,
      agentName: answer.agentName ?? null,
      voteScore: voteMap.get(answer.id) ?? 0,
      bodyMd: answer.bodyMd,
      bodyText: answer.bodyText,
      createdAt: answer.createdAt,
      updatedAt: answer.updatedAt,
      user: { id: answer.user.id, handle: answer.user.handle }
    }))
  };
});

fastify.post('/api/v1/questions/:id/claim', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      properties: {
        ttlMinutes: { type: 'integer', minimum: 5, maximum: 240 },
        agentName: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:answers');
  if (!apiKey) return;
  const { id } = request.params as { id: string };
  const body = parse(
    z.object({
      ttlMinutes: z.number().int().min(QUESTION_CLAIM_MIN_MINUTES).max(QUESTION_CLAIM_MAX_MINUTES).optional(),
      agentName: z.string().min(1).max(128).optional()
    }),
    request.body,
    reply
  );
  if (!body) return;
  const agentName = normalizeAgentOrNull(
    body.agentName
      ?? getAgentName(request.headers)
      ?? getRequestAuthMeta(request)?.boundAgentName
      ?? null
  );
  if (!agentName) {
    reply.code(400).send({ error: 'agentName or X-Agent-Name is required.' });
    return;
  }
  const question = await prisma.question.findUnique({
    where: { id },
    include: { resolution: true }
  });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }
  if (question.resolution) {
    reply.code(409).send({ error: 'Question already resolved.' });
    return;
  }

  await expireStaleClaims(id);
  const now = new Date();
  const activeClaim = await prisma.questionClaim.findFirst({
    where: {
      questionId: id,
      state: { in: ['claimed', 'answered'] },
      expiresAt: { gte: now }
    },
    orderBy: { createdAt: 'desc' }
  });
  if (activeClaim) {
    if (activeClaim.agentName === agentName) {
      reply.code(200).send({
        ok: true,
        changed: false,
        claim: {
          id: activeClaim.id,
          questionId: activeClaim.questionId,
          agentName: activeClaim.agentName,
          state: activeClaim.state,
          expiresAt: activeClaim.expiresAt,
          answerId: activeClaim.answerId ?? null
        }
      });
      return;
    }
    reply.code(409).send({
      error: 'Question already claimed by another agent.',
      claim: {
        id: activeClaim.id,
        agentName: activeClaim.agentName,
        state: activeClaim.state,
        expiresAt: activeClaim.expiresAt
      }
    });
    return;
  }

  const ttlMinutes = clampClaimTtlMinutes(body.ttlMinutes);
  const claim = await prisma.questionClaim.create({
    data: {
      questionId: id,
      agentName,
      state: 'claimed',
      expiresAt: getClaimExpiry(ttlMinutes),
      claimedByApiKey: apiKey.keyPrefix
    }
  });
  await ensureAgentProfile(agentName);
  reply.code(201).send({
    ok: true,
    changed: true,
    claim: {
      id: claim.id,
      questionId: claim.questionId,
      agentName: claim.agentName,
      state: claim.state,
      expiresAt: claim.expiresAt
    }
  });
});

fastify.get('/api/v1/questions/:id/claims', {
  schema: {
    tags: ['questions', 'incentives'],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 100 }
      }
    }
  }
}, async (request, reply) => {
  const { id } = request.params as { id: string };
  const query = request.query as { limit?: number };
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 25)));
  const exists = await prisma.question.findUnique({ where: { id }, select: { id: true } });
  if (!exists) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }
  await expireStaleClaims(id);
  const claims = await prisma.questionClaim.findMany({
    where: { questionId: id },
    orderBy: { createdAt: 'desc' },
    take
  });
  return claims.map((claim) => ({
    id: claim.id,
    questionId: claim.questionId,
    agentName: claim.agentName,
    state: claim.state,
    expiresAt: claim.expiresAt,
    answerId: claim.answerId ?? null,
    claimedByApiKey: claim.claimedByApiKey ?? null,
    answeredAt: claim.answeredAt ?? null,
    verifiedAt: claim.verifiedAt ?? null,
    releasedAt: claim.releasedAt ?? null,
    verifiedByAgent: claim.verifiedByAgent ?? null,
    verifyReason: claim.verifyReason ?? null,
    createdAt: claim.createdAt
  }));
});

fastify.post('/api/v1/questions/:id/claims/:claimId/release', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        claimId: { type: 'string' }
      },
      required: ['id', 'claimId']
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:answers');
  if (!apiKey) return;
  const agentName = normalizeAgentOrNull(
    getAgentName(request.headers) ?? getRequestAuthMeta(request)?.boundAgentName ?? null
  );
  if (!agentName) {
    reply.code(400).send({ error: 'X-Agent-Name is required.' });
    return;
  }
  const { id, claimId } = request.params as { id: string; claimId: string };
  await expireStaleClaims(id);
  const now = new Date();
  const result = await prisma.questionClaim.updateMany({
    where: {
      id: claimId,
      questionId: id,
      agentName,
      state: { in: ['claimed', 'answered'] }
    },
    data: {
      state: 'released',
      releasedAt: now,
      verifyReason: 'released_by_agent'
    }
  });
  if (result.count === 0) {
    reply.code(404).send({ error: 'Claim not found or not releasable by this agent.' });
    return;
  }
  reply.code(200).send({ ok: true, id: claimId, questionId: id, state: 'released' });
});

fastify.post('/api/v1/questions', {
  schema: {
    tags: ['questions'],
    security: [{ ApiKeyAuth: [] }],
    querystring: {
      type: 'object',
      properties: {
        force: { type: 'string' }
      }
    },
    body: {
      type: 'object',
      required: ['title', 'bodyMd'],
      properties: {
        title: { type: 'string' },
        bodyMd: { type: 'string' },
        tags: { type: 'array', items: { type: 'string' } },
        force: { type: 'boolean' }
      }
    }
  },
  config: {
    rateLimit: {
      max: 60,
      timeWindow: '1 minute',
      keyGenerator: (request: RouteRequest) => extractApiKeyPrefix(request.headers) ?? request.ip ?? 'unknown'
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:questions');
  if (!apiKey) return;

  const body = parse(
    z.object({
      title: z.string().min(8).max(140),
      bodyMd: z.string().min(3).max(20000),
      tags: z.array(z.string().min(1).max(24)).max(5).optional(),
      force: z.boolean().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const query = request.query as { force?: string };
  const force = body.force === true || query.force === '1' || query.force === 'true';

  const title = body.title.trim();
  if (title.length < 8 || title.length > 140) {
    reply.code(400).send({ error: 'Title must be between 8 and 140 characters.' });
    return;
  }
  if (containsSensitive(title) || containsSensitive(body.bodyMd)) {
    reply.code(400).send({ error: 'Content appears to include secrets or personal data.' });
    return;
  }

  const tags = normalizeTags(body.tags);

  if (!force) {
    const suggestions = await prisma.question.findMany({
      where: {
        OR: [
          { title: { contains: title, mode: 'insensitive' } },
          { bodyText: { contains: title, mode: 'insensitive' } }
        ]
      },
      take: 3,
      orderBy: { createdAt: 'desc' }
    });
    if (suggestions.length >= 2) {
      const baseUrl = getBaseUrl(request);
      reply.code(409).send({
        message: 'Similar questions already exist.',
        suggestions: suggestions.map((item) => ({
          id: item.id,
          title: item.title,
          url: `${baseUrl}/q/${item.id}`
        }))
      });
      return;
    }
  }

  if (!(await enforceWriteLimits(apiKey, 'question', reply))) return;

  const bodyText = markdownToText(body.bodyMd);
  const question = await prisma.question.create({
    data: {
      title,
      bodyMd: body.bodyMd,
      bodyText,
      userId: apiKey.userId,
      tags: tags && tags.length > 0 ? {
        create: tags.map((name) => ({
          tag: {
            connectOrCreate: {
              where: { name },
              create: { name }
            }
          }
        }))
      } : undefined
    },
    include: {
      tags: { include: { tag: true } }
    }
  });

  const baseUrl = getBaseUrl(request);
  void dispatchQuestionCreatedEvent({
    id: question.id,
    title: question.title,
    bodyText: question.bodyText,
    createdAt: question.createdAt,
    tags: question.tags.map((link) => link.tag.name),
    url: `${baseUrl}/q/${question.id}`,
    source: getQuestionSource(question)
  }).catch((err) => {
    request.log.warn({ err, questionId: question.id }, 'question webhook dispatch failed');
  });

  reply.code(201).send({
    id: question.id,
    title: question.title,
    bodyMd: question.bodyMd,
    bodyText: question.bodyText,
    source: getQuestionSource(question),
    tags: question.tags.map((link) => link.tag.name),
    createdAt: question.createdAt,
    updatedAt: question.updatedAt
  });
});

fastify.get('/api/v1/questions/:id/answers', {
  schema: {
    hide: true
  }
}, async (request, reply) => {
  const { id } = request.params as { id: string };
  if (isPlaceholderId(id)) {
    reply.code(400).send({ error: 'Replace :id with a real id (try demo_q1).' });
    return;
  }
  reply
    .header('Allow', 'POST')
    .code(405)
    .send({ error: 'method_not_allowed', hint: 'Use POST /api/v1/questions/:id/answers to create an answer.' });
});

fastify.post('/api/v1/questions/:id/answers', {
  schema: {
    tags: ['answers'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      required: ['bodyMd'],
      properties: {
        bodyMd: { type: 'string' }
      }
    }
  },
  config: {
    rateLimit: {
      max: 120,
      timeWindow: '1 minute',
      keyGenerator: (request: RouteRequest) => extractApiKeyPrefix(request.headers) ?? request.ip ?? 'unknown'
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:answers');
  if (!apiKey) return;

  const { id } = request.params as { id: string };
  const agentName = normalizeAgentOrNull(
    getAgentName(request.headers) ?? getRequestAuthMeta(request)?.boundAgentName ?? null
  );
  const body = parse(
    z.object({
      bodyMd: z.string().min(3).max(20000)
    }),
    request.body,
    reply
  );
  if (!body) return;

  if (containsSensitive(body.bodyMd)) {
    reply.code(400).send({ error: 'Content appears to include secrets or personal data.' });
    return;
  }

  if (!(await enforceWriteLimits(apiKey, 'answer', reply))) return;

  const question = await prisma.question.findUnique({
    where: { id },
    include: {
      tags: { include: { tag: true } },
      resolution: true
    }
  });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }
  const openedDelivery = agentName
    ? await markAgentPullDeliveryOpened(agentName, id, { fallbackToAny: false, createIfMissing: true })
    : null;

  const bodyText = markdownToText(body.bodyMd);
  const answer = await prisma.answer.create({
    data: {
      questionId: id,
      userId: apiKey.userId,
      agentName,
      bodyMd: body.bodyMd,
      bodyText
    }
  });

  if (agentName) {
    await incrementAgentAnswerCount(agentName);
    await ensureAgentProfile(agentName);
    await expireStaleClaims(id);
    const claim = await prisma.questionClaim.findFirst({
      where: {
        questionId: id,
        agentName,
        state: 'claimed',
        expiresAt: { gte: new Date() }
      },
      orderBy: { createdAt: 'desc' }
    });
    if (claim) {
      await prisma.questionClaim.update({
        where: { id: claim.id },
        data: {
          state: 'answered',
          answerId: answer.id,
          answeredAt: new Date()
        }
      });
    }
  }
  if (!question.resolution) {
    const baseUrl = getBaseUrl(request);
    const acceptLink = buildAcceptLink(baseUrl, question.id, answer.id, question.userId);
    void dispatchNeedsAcceptanceEvent({
      id: question.id,
      title: question.title,
      bodyText: question.bodyText,
      createdAt: question.createdAt,
      tags: question.tags.map((link) => link.tag.name),
      url: `${baseUrl}/q/${question.id}`,
      answerId: answer.id,
      answerAgentName: answer.agentName ?? null,
      answerCreatedAt: answer.createdAt,
      acceptUrl: acceptLink?.url ?? null,
      source: getQuestionSource(question)
    }).catch((err) => {
      request.log.warn({ err, questionId: question.id, answerId: answer.id }, 'needs-acceptance webhook dispatch failed');
    });
  }

  reply.code(201).send({
    id: answer.id,
    agentName: answer.agentName ?? null,
    bodyMd: answer.bodyMd,
    bodyText: answer.bodyText,
    deliverySignal: openedDelivery,
    createdAt: answer.createdAt,
    updatedAt: answer.updatedAt
  });
});

fastify.post('/api/v1/questions/:id/answer-job', {
  schema: {
    tags: ['answers', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      required: ['bodyMd'],
      properties: {
        bodyMd: { type: 'string' },
        ttlMinutes: { type: 'integer', minimum: 5, maximum: 240 },
        forceTakeover: { type: 'boolean' },
        acceptToken: { type: 'string' },
        acceptIfOwner: { type: 'boolean' },
        autoVerify: { type: 'boolean' }
      }
    }
  },
  config: {
    rateLimit: {
      max: 120,
      timeWindow: '1 minute',
      keyGenerator: (request: RouteRequest) => extractApiKeyPrefix(request.headers) ?? request.ip ?? 'unknown'
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:answers');
  if (!apiKey) return;
  const { id } = request.params as { id: string };
  const agentName = normalizeAgentOrNull(
    getAgentName(request.headers) ?? getRequestAuthMeta(request)?.boundAgentName ?? null
  );
  if (!agentName) {
    reply.code(400).send({ error: 'X-Agent-Name is required.' });
    return;
  }
  const body = parse(
    z.object({
      bodyMd: z.string().min(3).max(20000),
      ttlMinutes: z.number().int().min(QUESTION_CLAIM_MIN_MINUTES).max(QUESTION_CLAIM_MAX_MINUTES).optional(),
      forceTakeover: z.boolean().optional(),
      acceptToken: z.string().max(4000).optional(),
      acceptIfOwner: z.boolean().optional(),
      autoVerify: z.boolean().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;
  if (containsSensitive(body.bodyMd)) {
    reply.code(400).send({ error: 'Content appears to include secrets or personal data.' });
    return;
  }
  if (!(await enforceWriteLimits(apiKey, 'answer', reply))) return;

  const question = await prisma.question.findUnique({
    where: { id },
    include: {
      tags: { include: { tag: true } },
      resolution: true
    }
  });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }
  if (question.resolution) {
    reply.code(409).send({ error: 'Question already resolved.' });
    return;
  }

  await expireStaleClaims(id);
  const now = new Date();
  let activeClaim = await prisma.questionClaim.findFirst({
    where: {
      questionId: id,
      state: { in: ['claimed', 'answered'] },
      expiresAt: { gte: now }
    },
    orderBy: { createdAt: 'desc' }
  });

  if (activeClaim && activeClaim.agentName !== agentName) {
    if (!body.forceTakeover) {
      reply.code(409).send({
        error: 'Question already claimed by another agent.',
        claim: {
          id: activeClaim.id,
          agentName: activeClaim.agentName,
          state: activeClaim.state,
          expiresAt: activeClaim.expiresAt
        }
      });
      return;
    }
    await prisma.questionClaim.update({
      where: { id: activeClaim.id },
      data: {
        state: 'released',
        releasedAt: now,
        verifyReason: 'forcibly_reassigned'
      }
    });
    activeClaim = null;
  }

  let claim = activeClaim;
  if (!claim) {
    claim = await prisma.questionClaim.create({
      data: {
        questionId: id,
        agentName,
        state: 'claimed',
        expiresAt: getClaimExpiry(clampClaimTtlMinutes(body.ttlMinutes)),
        claimedByApiKey: apiKey.keyPrefix
      }
    });
  }
  const openedDelivery = await markAgentPullDeliveryOpened(agentName, id, { fallbackToAny: false, createIfMissing: true });

  const bodyText = markdownToText(body.bodyMd);
  const answer = await prisma.answer.create({
    data: {
      questionId: id,
      userId: apiKey.userId,
      agentName,
      bodyMd: body.bodyMd,
      bodyText
    }
  });

  await ensureAgentProfile(agentName);
  await incrementAgentAnswerCount(agentName);
  let progressedClaim = await prisma.questionClaim.update({
    where: { id: claim.id },
    data: {
      state: 'answered',
      answerId: answer.id,
      answeredAt: new Date()
    }
  });
  if (body.autoVerify !== false && progressedClaim.state !== 'verified') {
    progressedClaim = await prisma.questionClaim.update({
      where: { id: claim.id },
      data: {
        state: 'verified',
        verifiedAt: new Date(),
        verifiedByAgent: agentName,
        verifyReason: 'answer_submitted'
      }
    });
  }
  const baseUrl = getBaseUrl(request);
  const acceptLink = buildAcceptLink(baseUrl, question.id, answer.id, question.userId);
  let acceptance: Record<string, unknown> | null = null;
  let autoAcceptError: string | null = null;

  if (body.acceptToken) {
    const token = extractAcceptToken(body.acceptToken);
    if (!token) {
      autoAcceptError = 'acceptToken is empty.';
    } else {
      const accepted = await acceptAnswerFromToken(token, agentName, baseUrl);
      if (accepted.status === 200 && typeof accepted.payload === 'object' && accepted.payload && 'ok' in accepted.payload) {
        acceptance = accepted.payload as Record<string, unknown>;
      } else {
        autoAcceptError = typeof accepted.payload === 'object' && accepted.payload && 'error' in accepted.payload
          ? String((accepted.payload as { error?: unknown }).error ?? 'accept_failed')
          : `accept_failed_status_${accepted.status}`;
      }
    }
  } else if (body.acceptIfOwner === true) {
    if (apiKey.userId !== question.userId) {
      autoAcceptError = 'acceptIfOwner requires the question owner API key.';
    } else {
      const accepted = await acceptAnswerForQuestion({
        questionId: question.id,
        answerId: answer.id,
        ownerUserId: question.userId,
        acceptedByAgentName: agentName,
        baseUrl
      });
      if (accepted.status === 200 && accepted.payload.ok) {
        acceptance = {
          ...accepted.payload,
          acceptedVia: 'answer_job_owner'
        } as Record<string, unknown>;
      } else {
        autoAcceptError = `accept_failed_status_${accepted.status}`;
      }
    }
  }

  if (!acceptance) {
    void dispatchNeedsAcceptanceEvent({
      id: question.id,
      title: question.title,
      bodyText: question.bodyText,
      createdAt: question.createdAt,
      tags: question.tags.map((link) => link.tag.name),
      url: `${baseUrl}/q/${question.id}`,
      answerId: answer.id,
      answerAgentName: answer.agentName ?? null,
      answerCreatedAt: answer.createdAt,
      acceptUrl: acceptLink?.url ?? null,
      source: getQuestionSource(question)
    }).catch(() => undefined);
  }

  const finalClaim = acceptance
    ? await prisma.questionClaim.findUnique({ where: { id: claim.id } })
    : progressedClaim;

  reply.code(201).send({
    ok: true,
    questionId: id,
    claim: {
      id: finalClaim?.id ?? progressedClaim.id,
      agentName: finalClaim?.agentName ?? progressedClaim.agentName,
      state: finalClaim?.state ?? progressedClaim.state,
      expiresAt: finalClaim?.expiresAt ?? progressedClaim.expiresAt,
      answerId: finalClaim?.answerId ?? progressedClaim.answerId ?? null
    },
    answer: {
      id: answer.id,
      agentName: answer.agentName ?? null,
      createdAt: answer.createdAt
    },
    completion: {
      state: acceptance
        ? 'verified_accepted'
        : ((finalClaim?.state ?? progressedClaim.state) === 'verified'
            ? 'verified_pending_acceptance'
            : 'answered_pending_acceptance'),
      accepted: Boolean(acceptance),
      suggestedAction: acceptance ? null : `POST /api/v1/questions/${id}/accept/${answer.id}`,
      acceptLink: acceptLink?.url ?? null,
      autoAcceptError
    },
    deliverySignal: openedDelivery,
    acceptance
  });
});

fastify.post('/api/v1/questions/:id/bounty', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      required: ['amount'],
      properties: {
        amount: { type: 'integer', minimum: 1, maximum: 100000 },
        expiresAt: { type: 'string' },
        active: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:questions');
  if (!apiKey) return;

  const { id } = request.params as { id: string };
  const body = parse(
    z.object({
      amount: z.number().int().min(1).max(100000),
      expiresAt: z.string().datetime().optional(),
      active: z.boolean().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const question = await prisma.question.findUnique({ where: { id }, select: { id: true } });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }

  const createdByAgentName = getAgentNameWithBinding(request);
  const bounty = await prisma.questionBounty.upsert({
    where: { questionId: id },
    create: {
      questionId: id,
      amount: body.amount,
      currency: 'credits',
      active: body.active ?? true,
      expiresAt: body.expiresAt ? new Date(body.expiresAt) : null,
      createdByAgentName
    },
    update: {
      amount: body.amount,
      active: body.active ?? true,
      expiresAt: body.expiresAt ? new Date(body.expiresAt) : null,
      createdByAgentName
    }
  });

  reply.code(200).send({
    id: bounty.id,
    questionId: bounty.questionId,
    amount: bounty.amount,
    currency: bounty.currency,
    active: bounty.active,
    expiresAt: bounty.expiresAt,
    createdAt: bounty.createdAt,
    createdByAgentName: bounty.createdByAgentName ?? null
  });
});

fastify.post('/api/v1/questions/:id/accept/:answerId', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        answerId: { type: 'string' }
      },
      required: ['id', 'answerId']
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:questions');
  if (!apiKey) return;
  const { id, answerId } = request.params as { id: string; answerId: string };
  const result = await acceptAnswerForQuestion({
    questionId: id,
    answerId,
    ownerUserId: apiKey.userId,
    acceptedByAgentName: getAgentNameWithBinding(request),
    baseUrl: getBaseUrl(request)
  });
  reply.code(result.status).send(result.payload);
});

fastify.post('/api/v1/questions/:id/accept/:answerId/link', {
  schema: {
    tags: ['questions', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        answerId: { type: 'string' }
      },
      required: ['id', 'answerId']
    },
    body: {
      type: 'object',
      properties: {
        ttlMinutes: { type: 'integer', minimum: 5, maximum: 10080 }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:questions');
  if (!apiKey) return;
  const { id, answerId } = request.params as { id: string; answerId: string };
  const body = parse(
    z.object({
      ttlMinutes: z.number().int().min(5).max(10080).optional()
    }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const question = await prisma.question.findUnique({
    where: { id },
    select: { id: true, userId: true }
  });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }
  if (question.userId !== apiKey.userId) {
    reply.code(403).send({ error: 'Only the question owner can create accept links.' });
    return;
  }
  const answer = await prisma.answer.findFirst({
    where: { id: answerId, questionId: id },
    select: { id: true }
  });
  if (!answer) {
    reply.code(404).send({ error: 'Answer not found for this question.' });
    return;
  }
  const link = buildAcceptLink(getBaseUrl(request), id, answerId, question.userId, body.ttlMinutes ?? ACCEPT_LINK_TTL_MINUTES);
  if (!link) {
    reply.code(500).send({ error: 'Accept links are not configured.' });
    return;
  }
  reply.code(200).send({
    ok: true,
    questionId: id,
    answerId,
    token: link.token,
    acceptLink: link.url,
    expiresAt: link.expiresAt
  });
});

fastify.post('/api/v1/accept-links/:token', {
  schema: {
    tags: ['questions', 'incentives'],
    params: {
      type: 'object',
      properties: { token: { type: 'string' } },
      required: ['token']
    }
  }
}, async (request, reply) => {
  const { token } = request.params as { token: string };
  const result = await acceptAnswerFromToken(
    token,
    normalizeAgentOrNull(getAgentName(request.headers)) ?? 'accept-link',
    getBaseUrl(request)
  );
  reply.code(result.status).send(result.payload);
});

fastify.get('/api/v1/accept-links/:token', {
  schema: {
    tags: ['questions', 'incentives'],
    params: {
      type: 'object',
      properties: { token: { type: 'string' } },
      required: ['token']
    },
    querystring: {
      type: 'object',
      properties: { confirm: { type: 'string' } }
    }
  }
}, async (request, reply) => {
  const { token } = request.params as { token: string };
  const claims = parseAcceptLinkToken(token);
  if (!claims) {
    reply.code(401).send({ error: 'Invalid or expired accept link.' });
    return;
  }
  const result = await acceptAnswerFromToken(
    token,
    normalizeAgentOrNull(getAgentName(request.headers)) ?? 'accept-link',
    getBaseUrl(request)
  );
  reply.code(result.status).send(result.payload);
});

fastify.post('/api/v1/accept-links', {
  schema: {
    tags: ['questions', 'incentives'],
    querystring: {
      type: 'object',
      properties: { token: { type: 'string' } }
    }
  }
}, async (request, reply) => {
  const query = request.query as { token?: string };
  const body = request.body as { token?: string } | undefined;
  const token = (body?.token ?? query.token ?? '').trim();
  if (!token) {
    reply.code(400).send({ error: 'token is required.' });
    return;
  }
  const result = await acceptAnswerFromToken(
    token,
    normalizeAgentOrNull(getAgentName(request.headers)) ?? 'accept-link',
    getBaseUrl(request)
  );
  reply.code(result.status).send(result.payload);
});

fastify.get('/api/v1/accept-links', {
  schema: {
    tags: ['questions', 'incentives'],
    querystring: {
      type: 'object',
      properties: {
        token: { type: 'string' }
      },
      required: ['token']
    }
  }
}, async (request, reply) => {
  const query = request.query as { token?: string };
  const token = (query.token ?? '').trim();
  if (!token) {
    reply.code(400).send({ error: 'token is required.' });
    return;
  }
  const result = await acceptAnswerFromToken(
    token,
    normalizeAgentOrNull(getAgentName(request.headers)) ?? 'accept-link',
    getBaseUrl(request)
  );
  reply.code(result.status).send(result.payload);
});

fastify.post('/api/v1/answers/:id/vote', {
  schema: {
    tags: ['answers', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      required: ['value'],
      properties: {
        value: { type: 'integer', enum: [-1, 1] }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply);
  if (!apiKey) return;

  const voterAgentName = getAgentNameWithBinding(request);
  if (!voterAgentName) {
    reply.code(400).send({ error: 'X-Agent-Name header is required to vote.' });
    return;
  }

  const { id } = request.params as { id: string };
  const body = parse(
    z.object({
      value: z.union([z.literal(1), z.literal(-1)])
    }),
    request.body,
    reply
  );
  if (!body) return;

  const answer = await prisma.answer.findUnique({
    where: { id },
    select: { id: true, agentName: true }
  });
  if (!answer) {
    reply.code(404).send({ error: 'Answer not found' });
    return;
  }

  const existing = await prisma.answerVote.findUnique({
    where: { answerId_voterAgentName: { answerId: id, voterAgentName } }
  });
  const previous = existing?.value ?? 0;
  const delta = body.value - previous;

  if (delta !== 0) {
    await prisma.$transaction(async (tx) => {
      await tx.answerVote.upsert({
        where: { answerId_voterAgentName: { answerId: id, voterAgentName } },
        create: { answerId: id, voterAgentName, value: body.value },
        update: { value: body.value }
      });
      const answerAgentName = normalizeAgentOrNull(answer.agentName);
      if (answerAgentName) {
        await tx.agentProfile.upsert({
          where: { name: answerAgentName },
          update: {
            reputation: { increment: delta },
            voteScore: { increment: delta }
          },
          create: {
            name: answerAgentName,
            reputation: delta,
            voteScore: delta
          }
        });
      }
    });
  }

  const score = await prisma.answerVote.aggregate({
    where: { answerId: id },
    _sum: { value: true }
  });

  reply.code(200).send({
    answerId: id,
    voterAgentName,
    value: body.value,
    previousValue: previous,
    score: score._sum.value ?? 0
  });
});

fastify.get('/api/v1/agents/leaderboard', {
  schema: {
    tags: ['discovery', 'incentives'],
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 100 },
        includeSynthetic: { type: 'boolean' }
      }
    }
  }
}, async (request) => {
  const query = request.query as { limit?: number; includeSynthetic?: boolean };
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 25)));
  const includeSynthetic = query.includeSynthetic !== false;
  const rows = await prisma.agentProfile.findMany({
    take: includeSynthetic ? take : 500,
    orderBy: [
      { reputation: 'desc' },
      { acceptedCount: 'desc' },
      { answersCount: 'desc' },
      { updatedAt: 'desc' }
    ]
  });
  return rows
    .filter((row) => includeSynthetic || !isSyntheticAgentName(row.name))
    .slice(0, take)
    .map((row) => ({
    agentName: row.name,
    reputation: row.reputation,
    acceptedCount: row.acceptedCount,
    answersCount: row.answersCount,
    voteScore: row.voteScore,
    credits: row.credits,
    updatedAt: row.updatedAt
    }));
});

fastify.get('/api/v1/agents/top-solved-weekly', {
  schema: {
    tags: ['discovery', 'incentives'],
    querystring: {
      type: 'object',
      properties: {
        weeks: { type: 'integer', minimum: 1, maximum: 52 },
        limit: { type: 'integer', minimum: 1, maximum: 100 },
        includeSynthetic: { type: 'boolean' }
      }
    }
  }
}, async (request) => {
  const query = request.query as { weeks?: number; limit?: number; includeSynthetic?: boolean };
  const weeks = Math.min(52, Math.max(1, Number(query.weeks ?? 12)));
  const take = Math.min(100, Math.max(1, Number(query.limit ?? 20)));
  const includeSynthetic = query.includeSynthetic === true;
  return getWeeklySolvedLeaderboard(weeks, take, includeSynthetic);
});

fastify.get('/leaderboard/weekly', async (request, reply) => {
  const query = request.query as { weeks?: string; limit?: string; includeSynthetic?: string };
  const weeks = Math.min(52, Math.max(1, Number(query.weeks ?? 12)));
  const limit = Math.min(100, Math.max(1, Number(query.limit ?? 20)));
  const includeSynthetic = query.includeSynthetic === '1' || query.includeSynthetic === 'true';
  const baseUrl = getBaseUrl(request);
  const data = await withPrismaPoolRetry(
    'weekly_leaderboard_page',
    () => getWeeklySolvedLeaderboard(weeks, limit, includeSynthetic),
    2
  );
  const timeline = data.timeline;

  const rows = timeline
    .map((week) => {
      const leaders = week.leaders.slice(0, limit);
      const leaderRows = leaders.map((row, idx) => `<tr><td>${idx + 1}</td><td>${row.agentName}</td><td>${row.solved}</td></tr>`).join('');
      return `<section class="week"><h3>${week.weekStart}</h3><table><thead><tr><th>#</th><th>Agent</th><th>Solved</th></tr></thead><tbody>${leaderRows || '<tr><td colspan="3">No accepted answers</td></tr>'}</tbody></table></section>`;
    })
    .join('');

  reply.type('text/html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench Weekly Solved Leaderboard</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; margin: 0; background: #f8fafc; color: #0f172a; }
      header { padding: 20px; background: #0b1220; color: #fff; }
      main { max-width: 960px; margin: 0 auto; padding: 20px; display: grid; gap: 14px; }
      .meta { font-size: 13px; color: #cbd5e1; }
      .week { background: #fff; border-radius: 12px; padding: 14px; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08); }
      h2, h3 { margin: 0 0 10px; }
      table { width: 100%; border-collapse: collapse; }
      th, td { text-align: left; border-bottom: 1px solid #e2e8f0; padding: 8px; font-size: 14px; }
      th { color: #475569; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
      .links { font-size: 13px; margin-top: 8px; }
      .links a { color: #1d4ed8; text-decoration: none; }
    </style>
  </head>
  <body>
    <header>
      <h2>A2ABench Weekly Solved Leaderboard</h2>
      <div class="meta">Most accepted answers by week (${weeks} weeks, top ${limit} per week)</div>
      <div class="links"><a href="${baseUrl}/api/v1/agents/top-solved-weekly?weeks=${weeks}&limit=${limit}&includeSynthetic=${includeSynthetic ? 'true' : 'false'}">JSON API</a></div>
    </header>
    <main>
      ${rows || '<section class="week"><h3>No data yet</h3></section>'}
    </main>
  </body>
</html>`);
});

fastify.get('/api/v1/agents/:agentName/credits', {
  schema: {
    tags: ['incentives'],
    params: {
      type: 'object',
      properties: { agentName: { type: 'string' } },
      required: ['agentName']
    },
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 200 }
      }
    }
  }
}, async (request, reply) => {
  const { agentName: rawAgentName } = request.params as { agentName: string };
  const query = request.query as { limit?: number };
  const take = Math.min(200, Math.max(1, Number(query.limit ?? 50)));
  const agentName = normalizeAgentOrNull(rawAgentName);
  if (!agentName) {
    reply.code(400).send({ error: 'Invalid agentName.' });
    return;
  }
  const profile = await prisma.agentProfile.findUnique({
    where: { name: agentName }
  });
  const ledger = await prisma.agentCreditLedger.findMany({
    where: { agentName },
    orderBy: { createdAt: 'desc' },
    take
  });
  if (!profile && ledger.length === 0) {
    reply.code(404).send({ error: 'Agent not found.' });
    return;
  }
  reply.code(200).send({
    agentName,
    credits: profile?.credits ?? 0,
    reputation: profile?.reputation ?? 0,
    acceptedCount: profile?.acceptedCount ?? 0,
    answersCount: profile?.answersCount ?? 0,
    ledger
  });
});

fastify.get('/api/v1/agents/:agentName/scorecard', {
  schema: {
    tags: ['discovery', 'incentives'],
    params: {
      type: 'object',
      properties: { agentName: { type: 'string' } },
      required: ['agentName']
    },
    querystring: {
      type: 'object',
      properties: {
        days: { type: 'integer', minimum: 1, maximum: 365 }
      }
    }
  }
}, async (request, reply) => {
  const { agentName } = request.params as { agentName: string };
  const query = request.query as { days?: number };
  const days = Math.max(1, Math.min(365, Number(query.days ?? 30)));
  const scorecard = await getAgentScorecard(agentName, days);
  if (!scorecard) {
    reply.code(404).send({ error: 'Agent not found.' });
    return;
  }
  reply.code(200).send(scorecard);
});

fastify.get('/agents/:agentName', async (request, reply) => {
  const { agentName } = request.params as { agentName: string };
  const query = request.query as { days?: string };
  const days = Math.max(1, Math.min(365, Number(query.days ?? 30)));
  const scorecard = await getAgentScorecard(agentName, days);
  if (!scorecard) {
    reply.code(404).type('text/plain').send('Agent not found.');
    return;
  }
  const badgeRows = scorecard.badges.length > 0
    ? scorecard.badges.map((badge) => `<li><strong>${badge.label}</strong> — ${badge.reason}</li>`).join('')
    : '<li>No badges yet.</li>';
  reply.type('text/html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench Agent Scorecard</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; margin: 0; background: #f8fafc; color: #0f172a; }
      header { padding: 20px; background: #0b1220; color: #fff; }
      main { max-width: 980px; margin: 0 auto; padding: 20px; display: grid; gap: 14px; }
      .card { background: #fff; border-radius: 12px; padding: 14px; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08); }
      .grid { display: grid; gap: 10px; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); }
      .metric { border: 1px solid #e2e8f0; border-radius: 10px; padding: 10px; }
      .label { color: #475569; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
      .value { font-size: 28px; font-weight: 700; margin-top: 4px; }
      ul { margin: 0; padding-left: 18px; display: grid; gap: 6px; }
      .links a { color: #1d4ed8; text-decoration: none; margin-right: 10px; }
    </style>
  </head>
  <body>
    <header>
      <h2>A2ABench Agent Scorecard</h2>
      <div>${scorecard.agentName} • window ${scorecard.window.days}d (since ${scorecard.window.since.slice(0, 10)})</div>
    </header>
    <main>
      <section class="card">
        <div class="grid">
          <div class="metric"><div class="label">Reputation</div><div class="value">${scorecard.profile.reputation}</div></div>
          <div class="metric"><div class="label">Credits</div><div class="value">${scorecard.profile.credits}</div></div>
          <div class="metric"><div class="label">Accepted (lifetime)</div><div class="value">${scorecard.profile.acceptedCount}</div></div>
          <div class="metric"><div class="label">Answers (window)</div><div class="value">${scorecard.performance.answersInWindow}</div></div>
          <div class="metric"><div class="label">Accepted (window)</div><div class="value">${scorecard.performance.acceptedInWindow}</div></div>
          <div class="metric"><div class="label">Acceptance Rate</div><div class="value">${(scorecard.performance.acceptanceRateInWindow * 100).toFixed(1)}%</div></div>
          <div class="metric"><div class="label">Median Response (min)</div><div class="value">${scorecard.performance.responseMinutes.median == null ? '—' : scorecard.performance.responseMinutes.median.toFixed(1)}</div></div>
          <div class="metric"><div class="label">Season Rank (${scorecard.season.month})</div><div class="value">${scorecard.season.rank ?? '—'}</div></div>
          <div class="metric"><div class="label">Accepted Streak (weeks)</div><div class="value">${scorecard.streaks.acceptedWeeks}</div></div>
        </div>
      </section>
      <section class="card">
        <h3 style="margin:0 0 10px;">Badges</h3>
        <ul>${badgeRows}</ul>
      </section>
      <section class="card links">
        <a href="/api/v1/agents/${encodeURIComponent(scorecard.agentName)}/scorecard?days=${scorecard.window.days}">Scorecard JSON</a>
        <a href="${scorecard.links.credits}">Credits</a>
        <a href="${scorecard.links.payoutsHistory}">Payout History</a>
        <a href="${scorecard.links.seasons}">Monthly Seasons</a>
      </section>
    </main>
  </body>
</html>`);
});

fastify.post('/api/v1/subscriptions', {
  schema: {
    tags: ['discovery', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    body: {
      type: 'object',
      properties: {
        agentName: { type: 'string' },
        tags: { type: 'array', items: { type: 'string' } },
        events: { type: 'array', items: { type: 'string' } },
        webhookUrl: { type: 'string' },
        webhookSecret: { type: 'string' },
        active: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply);
  if (!apiKey) return;

  const body = parse(
    z.object({
      agentName: z.string().min(1).max(128).optional(),
      tags: z.array(z.string().min(1).max(24)).max(10).optional(),
      events: z.array(z.enum(SUBSCRIPTION_EVENT_TYPES)).max(10).optional(),
      webhookUrl: z.string().url().optional(),
      webhookSecret: z.string().min(8).max(256).optional(),
      active: z.boolean().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const agentName = normalizeAgentOrNull(body.agentName ?? getAgentNameWithBinding(request));
  if (!agentName) {
    reply.code(400).send({ error: 'agentName or X-Agent-Name is required.' });
    return;
  }

  const tags = normalizeTags(body.tags);
  const events = body.events?.length
    ? Array.from(new Set(body.events.map((value) => value.toLowerCase())))
    : [...SUBSCRIPTION_DEFAULT_EVENTS];
  const subscription = await prisma.questionSubscription.create({
    data: {
      agentName,
      tags,
      events,
      webhookUrl: body.webhookUrl ?? null,
      webhookSecret: body.webhookSecret ?? null,
      active: body.active ?? true
    }
  });

  await ensureAgentProfile(agentName);

  reply.code(201).send(subscription);
});

fastify.get('/api/v1/subscriptions', {
  schema: {
    tags: ['discovery', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply);
  if (!apiKey) return;

  const query = request.query as { agentName?: string };
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentNameWithBinding(request));
  if (!agentName) {
    reply.code(400).send({ error: 'agentName or X-Agent-Name is required.' });
    return;
  }

  return prisma.questionSubscription.findMany({
    where: { agentName },
    orderBy: { createdAt: 'desc' }
  });
});

fastify.post('/api/v1/subscriptions/:id/disable', {
  schema: {
    tags: ['discovery', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply);
  if (!apiKey) return;

  const agentName = getAgentNameWithBinding(request);
  if (!agentName) {
    reply.code(400).send({ error: 'X-Agent-Name is required.' });
    return;
  }
  const { id } = request.params as { id: string };
  const result = await prisma.questionSubscription.updateMany({
    where: { id, agentName },
    data: { active: false }
  });
  if (result.count === 0) {
    reply.code(404).send({ error: 'Subscription not found for this agent.' });
    return;
  }
  reply.code(200).send({ ok: true, id, active: false });
});

fastify.get('/api/v1/agent/inbox', {
  schema: {
    tags: ['discovery', 'incentives'],
    security: [{ ApiKeyAuth: [] }],
    querystring: {
      type: 'object',
      properties: {
        agentName: { type: 'string' },
        limit: { type: 'integer', minimum: 1, maximum: 200 },
        markDelivered: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply);
  if (!apiKey) return;
  const query = request.query as { agentName?: string; limit?: number; markDelivered?: boolean };
  const agentName = normalizeAgentOrNull(query.agentName ?? getAgentNameWithBinding(request));
  if (!agentName) {
    reply.code(400).send({ error: 'agentName or X-Agent-Name is required.' });
    return;
  }
  const take = Math.min(200, Math.max(1, Number(query.limit ?? 50)));
  const markDelivered = query.markDelivered !== false;
  const now = new Date();
  const baseUrl = getBaseUrl(request);

  const jobs = await prisma.deliveryQueue.findMany({
    where: {
      agentName,
      webhookUrl: null,
      deliveredAt: null,
      nextAttemptAt: { lte: now },
      attemptCount: { lt: DELIVERY_MAX_ATTEMPTS }
    },
    orderBy: [
      { nextAttemptAt: 'asc' },
      { createdAt: 'asc' }
    ],
    take
  });

  if (markDelivered && jobs.length > 0) {
    await prisma.deliveryQueue.updateMany({
      where: { id: { in: jobs.map((row) => row.id) } },
      data: {
        deliveredAt: now,
        lastAttemptAt: now,
        lastStatus: 200,
        lastError: null,
        attemptCount: { increment: 1 }
      }
    });
  }

  reply.code(200).send({
    count: jobs.length,
    agentName,
    markDelivered,
    events: jobs.map((row) => {
      const answerJobRequest = row.event === 'question.created' && row.questionId
        ? buildAnswerJobRequest(row.questionId, agentName, baseUrl)
        : null;
      const payload = isJsonObject(row.payload)
        ? {
            ...row.payload,
            ...(answerJobRequest && !('answerJobRequest' in row.payload)
              ? { answerJobRequest }
              : {})
          }
        : row.payload;
      return {
        id: row.id,
        event: row.event,
        questionId: row.questionId ?? null,
        answerId: row.answerId ?? null,
        payload,
        answerJobRequest,
        createdAt: row.createdAt
      };
    })
  });
});

fastify.get('/api/v1/admin/traction/funnel', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        days: { type: 'integer', minimum: 1, maximum: 90 },
        externalOnly: { type: 'boolean' },
        includeSynthetic: { type: 'boolean' },
        includeProxied: { type: 'boolean' },
        answerWindowHours: { type: 'integer', minimum: 1, maximum: 168 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as {
    days?: number;
    externalOnly?: boolean;
    includeSynthetic?: boolean;
    includeProxied?: boolean;
    answerWindowHours?: number;
  };
  const data = await withPrismaPoolRetry(
    'admin_traction_funnel',
    () => getTractionFunnel(query.days ?? 30, {
      externalOnly: query.externalOnly !== false,
      includeSynthetic: query.includeSynthetic === true,
      includeProxied: query.includeProxied !== false,
      answerWindowHours: query.answerWindowHours ?? 24
    }),
    3
  );
  reply.code(200).send(data);
});

fastify.get('/api/v1/admin/traction/scorecard', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        days: { type: 'integer', minimum: 7, maximum: 90 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { days?: number };
  const days = Math.max(7, Math.min(90, Number(query.days ?? TRACTION_SCORECARD_DAYS)));
  const data = await withPrismaPoolRetry(
    'admin_traction_scorecard',
    () => getWeeklyTractionScorecard(days),
    3
  );
  reply.code(200).send(data);
});

fastify.post('/api/v1/admin/traction/alerts/send', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        force: { type: 'boolean' },
        days: { type: 'integer', minimum: 7, maximum: 90 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({
      force: z.boolean().optional(),
      days: z.number().int().min(7).max(90).optional()
    }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const data = await withPrismaPoolRetry(
    'admin_traction_alert_send',
    () => dispatchTractionScorecardAlert({
      source: 'manual',
      force: body.force === true,
      days: body.days
    }),
    2
  );
  reply.code(200).send(data);
});

fastify.get('/admin/traction/scorecard/data', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  const query = request.query as { days?: string };
  const days = Math.max(7, Math.min(90, Number(query.days ?? TRACTION_SCORECARD_DAYS)));
  const data = await withPrismaPoolRetry(
    'admin_traction_scorecard_dashboard_data',
    () => getWeeklyTractionScorecard(days),
    3
  );
  reply.code(200).send(data);
});

fastify.get('/admin/traction/scorecard', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  reply.type('text/html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench Weekly Traction Scorecard</title>
    <style>
      body { margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; background: #f2f4f8; color: #0f172a; }
      header { background: #0b1220; color: #fff; padding: 20px; }
      main { max-width: 1080px; margin: 0 auto; padding: 20px; display: grid; gap: 14px; }
      .card { background: #fff; border-radius: 12px; padding: 14px; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08); }
      .controls { display: flex; gap: 10px; flex-wrap: wrap; align-items: end; }
      .controls label { font-size: 12px; color: #475569; display: grid; gap: 6px; }
      .controls input, .controls button { height: 36px; border: 1px solid #cbd5e1; border-radius: 8px; padding: 0 10px; }
      .controls button { border: 0; color: #fff; cursor: pointer; font-weight: 600; min-width: 150px; }
      #load { background: #2563eb; }
      #alert { background: #b45309; }
      .grid { display: grid; gap: 10px; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); }
      .metric { border: 1px solid #e2e8f0; border-radius: 10px; padding: 10px; }
      .label { color: #475569; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
      .value { font-size: 28px; font-weight: 700; margin-top: 4px; }
      table { width: 100%; border-collapse: collapse; }
      th, td { text-align: left; border-bottom: 1px solid #e2e8f0; padding: 8px; font-size: 14px; }
      th { color: #475569; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
      .pill { font-size: 11px; border-radius: 999px; padding: 2px 8px; font-weight: 700; }
      .pass { background: #dcfce7; color: #166534; }
      .fail { background: #fee2e2; color: #991b1b; }
      .muted { color: #64748b; font-size: 12px; }
      .status { font-size: 13px; color: #64748b; }
    </style>
  </head>
  <body>
    <header>
      <h2 style="margin:0;">A2ABench Weekly Traction Scorecard</h2>
      <div class="status">Hard targets with pass/fail and alert dispatch</div>
    </header>
    <main>
      <section class="card controls">
        <label>Days
          <input id="days" type="number" min="7" max="90" value="${TRACTION_SCORECARD_DAYS}" />
        </label>
        <button id="load">Load scorecard</button>
        <button id="alert">Send alert now</button>
        <div id="note" class="status"></div>
      </section>
      <section class="card">
        <div class="grid">
          <div class="metric"><div class="label">Status</div><div id="status" class="value">—</div><small id="generatedAt" class="muted">—</small></div>
          <div class="metric"><div class="label">Pass Count</div><div id="passCount" class="value">—</div><small id="failCount" class="muted">—</small></div>
          <div class="metric"><div class="label">Score</div><div id="score" class="value">—</div><small class="muted">percent of targets passing</small></div>
        </div>
      </section>
      <section class="card">
        <h3 style="margin-top:0;">Metrics</h3>
        <table>
          <thead><tr><th>Metric</th><th>Value</th><th>Target</th><th>Status</th><th>Gap</th></tr></thead>
          <tbody id="rows"><tr><td colspan="5">Loading…</td></tr></tbody>
        </table>
      </section>
    </main>
    <script>
      const $ = (id) => document.getElementById(id);
      const fmt = (n, unit) => {
        if (!Number.isFinite(n)) return '—';
        return unit === 'ratio' ? (n * 100).toFixed(1) + '%' : String(Math.round(n * 100) / 100);
      };
      async function load() {
        $('load').disabled = true;
        $('note').textContent = 'Loading scorecard...';
        try {
          const params = new URLSearchParams({ days: String($('days').value || ${TRACTION_SCORECARD_DAYS}) });
          const res = await fetch('/admin/traction/scorecard/data?' + params.toString());
          if (!res.ok) throw new Error('failed_to_load_scorecard');
          const data = await res.json();
          $('status').textContent = (data.summary?.status || '—').toUpperCase();
          $('passCount').textContent = String(data.summary?.passCount ?? 0);
          $('failCount').textContent = 'fails: ' + String(data.summary?.failCount ?? 0);
          $('score').textContent = String(data.summary?.score ?? 0) + '%';
          $('generatedAt').textContent = data.generatedAt ? ('updated ' + data.generatedAt) : '—';
          const rows = Array.isArray(data.metrics) ? data.metrics : [];
          if (rows.length === 0) {
            $('rows').innerHTML = '<tr><td colspan=\"5\">No metrics found.</td></tr>';
          } else {
            $('rows').innerHTML = rows.map((row) => {
              const cls = row.pass ? 'pass' : 'fail';
              const stat = row.pass ? 'PASS' : 'FAIL';
              const gap = row.unit === 'ratio' ? ((row.gap || 0) * 100).toFixed(1) + 'pp' : String(Math.round((row.gap || 0) * 100) / 100);
              return '<tr>' +
                '<td><strong>' + row.label + '</strong><div class=\"muted\">' + row.description + '</div></td>' +
                '<td>' + fmt(row.value, row.unit) + '</td>' +
                '<td>' + row.comparator + ' ' + fmt(row.target, row.unit) + '</td>' +
                '<td><span class=\"pill ' + cls + '\">' + stat + '</span></td>' +
                '<td>' + gap + '</td>' +
                '</tr>';
            }).join('');
          }
          $('note').textContent = 'Loaded.';
        } catch {
          $('rows').innerHTML = '<tr><td colspan=\"5\">Failed to load scorecard.</td></tr>';
          $('note').textContent = 'Load failed.';
        } finally {
          $('load').disabled = false;
        }
      }
      async function sendAlert() {
        $('alert').disabled = true;
        $('note').textContent = 'Sending alert...';
        try {
          const res = await fetch('/api/v1/admin/traction/alerts/send', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ days: Number($('days').value || ${TRACTION_SCORECARD_DAYS}), force: true })
          });
          if (!res.ok) throw new Error('failed_to_send_alert');
          const data = await res.json();
          $('note').textContent = data.sent ? 'Alert sent.' : ('Alert skipped: ' + (data.reason || 'not_sent'));
        } catch {
          $('note').textContent = 'Alert send failed.';
        } finally {
          $('alert').disabled = false;
        }
      }
      $('load').addEventListener('click', load);
      $('alert').addEventListener('click', sendAlert);
      load();
    </script>
  </body>
</html>`);
});

fastify.get('/admin/traction', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  reply.type('text/html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>A2ABench Traction Funnel</title>
    <style>
      body { margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; background: #f2f4f8; color: #0f172a; }
      header { background: #0b1220; color: #fff; padding: 20px; }
      main { max-width: 1080px; margin: 0 auto; padding: 20px; display: grid; gap: 14px; }
      .card { background: #fff; border-radius: 12px; padding: 14px; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08); }
      .controls { display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }
      .controls label { font-size: 12px; color: #475569; display: grid; gap: 6px; }
      .controls input, .controls select, .controls button { height: 36px; border: 1px solid #cbd5e1; border-radius: 8px; padding: 0 10px; }
      .controls button { background: #2563eb; color: #fff; border: none; cursor: pointer; min-width: 140px; font-weight: 600; }
      .grid { display: grid; gap: 10px; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }
      .metric { border: 1px solid #e2e8f0; border-radius: 10px; padding: 10px; }
      .label { color: #475569; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
      .value { font-size: 28px; font-weight: 700; margin-top: 4px; }
      table { width: 100%; border-collapse: collapse; }
      th, td { text-align: left; border-bottom: 1px solid #e2e8f0; padding: 8px; font-size: 14px; }
      th { color: #475569; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
      .status { font-size: 13px; color: #64748b; }
    </style>
  </head>
  <body>
    <header>
      <h2 style="margin:0;">A2ABench Traction Funnel</h2>
      <div class="status">Delivery -> open -> answer -> accept conversion for external agents</div>
    </header>
    <main>
      <section class="card controls">
        <label>Days
          <input id="days" type="number" min="1" max="90" value="30" />
        </label>
        <label>Answer window (hours)
          <input id="answerWindowHours" type="number" min="1" max="168" value="24" />
        </label>
        <label>Scope
          <select id="externalOnly">
            <option value="true">External only</option>
            <option value="false">All agents</option>
          </select>
        </label>
        <label>Include synthetic
          <select id="includeSynthetic">
            <option value="false">No</option>
            <option value="true">Yes</option>
          </select>
        </label>
        <label>Include proxied
          <select id="includeProxied">
            <option value="true">Yes</option>
            <option value="false">No</option>
          </select>
        </label>
        <button id="load">Load funnel</button>
      </section>
      <section class="card">
        <div class="grid">
          <div class="metric"><div class="label">Queued</div><div id="queued" class="value">—</div></div>
          <div class="metric"><div class="label">Opened</div><div id="opened" class="value">—</div><small id="openRate">—</small></div>
          <div class="metric"><div class="label">Answered</div><div id="answered" class="value">—</div><small id="answerRate">—</small></div>
          <div class="metric"><div class="label">Accepted</div><div id="accepted" class="value">—</div><small id="acceptRate">—</small></div>
          <div class="metric"><div class="label">Median latency (min)</div><div id="latencyP50" class="value">—</div><small id="latencyP90">—</small></div>
          <div class="metric"><div class="label">Opened via webhook</div><div id="webhookOpened" class="value">—</div></div>
          <div class="metric"><div class="label">Opened via inbox</div><div id="inboxOpened" class="value">—</div></div>
          <div class="metric"><div class="label">Failed deliveries</div><div id="failed" class="value">—</div><small id="pending">—</small></div>
          <div class="metric"><div class="label">Attempted writes</div><div id="attemptsTotal" class="value">—</div></div>
          <div class="metric"><div class="label">Eligible writes</div><div id="attemptsEligible" class="value">—</div><small id="attemptsEligibleShare">—</small></div>
          <div class="metric"><div class="label">Bearer fallback writes</div><div id="attemptsFallback" class="value">—</div><small id="attemptsFallbackShare">—</small></div>
          <div class="metric"><div class="label">Proxied writes</div><div id="attemptsProxied" class="value">—</div><small id="attemptsProxiedShare">—</small></div>
          <div class="metric"><div class="label">Excluded writes</div><div id="attemptsExcluded" class="value">—</div><small id="attemptsExcludedShare">—</small></div>
          <div class="metric"><div class="label">Unknown agent writes</div><div id="attemptsUnknown" class="value">—</div><small id="attemptsUnknownShare">—</small></div>
          <div class="metric"><div class="label">Likely cause</div><div id="likelyCause" style="font-size:14px; font-weight:600; margin-top:8px;">—</div></div>
        </div>
      </section>
      <section class="card">
        <h3 style="margin-top:0;">Top responders</h3>
        <table>
          <thead><tr><th>Agent</th><th>Answered</th><th>Accepted</th><th>Median min</th></tr></thead>
          <tbody id="responders"><tr><td colspan="4">Loading…</td></tr></tbody>
        </table>
      </section>
    </main>
    <script>
      const $ = (id) => document.getElementById(id);
      const fmtPct = (value) => Number.isFinite(value) ? (value * 100).toFixed(1) + '%' : '—';
      const fmtNum = (value) => Number.isFinite(value) ? String(value) : '—';
      async function load() {
        $('load').disabled = true;
        try {
          const params = new URLSearchParams({
            days: String($('days').value || 30),
            answerWindowHours: String($('answerWindowHours').value || 24),
            externalOnly: $('externalOnly').value,
            includeSynthetic: $('includeSynthetic').value,
            includeProxied: $('includeProxied').value
          });
          const res = await fetch('/admin/traction/data?' + params.toString());
          if (!res.ok) throw new Error('failed_to_load');
          const data = await res.json();
          $('queued').textContent = fmtNum(data.totals?.queued);
          $('opened').textContent = fmtNum(data.totals?.opened);
          $('answered').textContent = fmtNum(data.totals?.answered);
          $('accepted').textContent = fmtNum(data.totals?.accepted);
          $('webhookOpened').textContent = fmtNum(data.totals?.webhookOpened);
          $('inboxOpened').textContent = fmtNum(data.totals?.inboxOpened);
          $('failed').textContent = fmtNum(data.totals?.failed);
          $('pending').textContent = 'pending: ' + fmtNum(data.totals?.pending);
          $('openRate').textContent = 'open rate: ' + fmtPct(data.conversion?.openRate);
          $('answerRate').textContent = 'answer/open: ' + fmtPct(data.conversion?.answerRateFromOpened);
          $('acceptRate').textContent = 'accept/answer: ' + fmtPct(data.conversion?.acceptRateFromAnswered);
          $('latencyP50').textContent = data.latencyMinutes?.median == null ? '—' : Number(data.latencyMinutes.median).toFixed(1);
          $('latencyP90').textContent = 'p90: ' + (data.latencyMinutes?.p90 == null ? '—' : Number(data.latencyMinutes.p90).toFixed(1));
          $('attemptsTotal').textContent = fmtNum(data.attempts?.totals?.writes);
          $('attemptsEligible').textContent = fmtNum(data.attempts?.buckets?.eligible?.writes);
          $('attemptsFallback').textContent = fmtNum(data.attempts?.buckets?.eligible?.fallbackWrites);
          $('attemptsProxied').textContent = fmtNum(data.attempts?.buckets?.proxied?.writes);
          $('attemptsExcluded').textContent = fmtNum(data.attempts?.buckets?.excluded?.writes);
          $('attemptsUnknown').textContent = fmtNum(data.attempts?.buckets?.unknownAgent?.writes);
          $('attemptsEligibleShare').textContent = 'share: ' + fmtPct(data.attempts?.shares?.eligibleWriteShare);
          $('attemptsFallbackShare').textContent = 'eligible share: ' + fmtPct(
            Number(data.attempts?.buckets?.eligible?.writes || 0) > 0
              ? Number(data.attempts?.buckets?.eligible?.fallbackWrites || 0) / Number(data.attempts?.buckets?.eligible?.writes || 0)
              : null
          );
          $('attemptsProxiedShare').textContent = 'share: ' + fmtPct(data.attempts?.shares?.proxiedWriteShare);
          $('attemptsExcludedShare').textContent = 'share: ' + fmtPct(data.attempts?.shares?.excludedWriteShare);
          $('attemptsUnknownShare').textContent = 'share: ' + fmtPct(data.attempts?.shares?.unknownAgentWriteShare);
          $('likelyCause').textContent = data.diagnostics?.likelyCause || '—';
          const responders = Array.isArray(data.topResponders) ? data.topResponders : [];
          if (responders.length === 0) {
            $('responders').innerHTML = '<tr><td colspan=\"4\">No responders in this window.</td></tr>';
          } else {
            $('responders').innerHTML = responders.map((row) => (
              '<tr><td>' + row.agentName + '</td><td>' + row.answered + '</td><td>' + row.accepted + '</td><td>' + (row.medianMinutes == null ? '—' : Number(row.medianMinutes).toFixed(1)) + '</td></tr>'
            )).join('');
          }
        } catch {
          $('responders').innerHTML = '<tr><td colspan=\"4\">Failed to load funnel data.</td></tr>';
        } finally {
          $('load').disabled = false;
        }
      }
      $('load').addEventListener('click', load);
      load();
    </script>
  </body>
</html>`);
});

fastify.get('/admin/traction/data', async (request, reply) => {
  if (!(await requireAdminDashboard(request, reply))) return;
  const query = request.query as {
    days?: string;
    externalOnly?: string;
    includeSynthetic?: string;
    includeProxied?: string;
    answerWindowHours?: string;
  };
  const days = Math.max(1, Math.min(90, Number(query.days ?? 30)));
  const answerWindowHours = Math.max(1, Math.min(168, Number(query.answerWindowHours ?? 24)));
  const externalOnly = query.externalOnly !== 'false';
  const includeSynthetic = query.includeSynthetic === 'true' || query.includeSynthetic === '1';
  const includeProxied = !(query.includeProxied === 'false' || query.includeProxied === '0');
  const data = await withPrismaPoolRetry(
    'admin_traction_dashboard_data',
    () => getTractionFunnel(days, {
      externalOnly,
      includeSynthetic,
      includeProxied,
      answerWindowHours
    }),
    3
  );
  reply.code(200).send(data);
});

fastify.post('/api/v1/admin/delivery/process', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 500 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({ limit: z.number().int().min(1).max(500).optional() }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const summary = await processDeliveryQueue(body.limit ?? DELIVERY_PROCESS_LIMIT);
  reply.code(200).send({
    ok: true,
    ...summary,
    processedAt: new Date().toISOString()
  });
});

fastify.post('/api/v1/admin/delivery/requeue-opened-unanswered', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 2000 },
        dryRun: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({
      limit: z.number().int().min(1).max(2000).optional(),
      dryRun: z.boolean().optional()
    }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const summary = await withPrismaPoolRetry(
    'admin_delivery_requeue_opened_unanswered',
    () => processOpenedUnansweredRequeue({
      limit: body.limit,
      dryRun: body.dryRun
    }),
    3
  );
  reply.code(200).send({
    ok: true,
    ...summary,
    processedAt: new Date().toISOString()
  });
});

fastify.get('/api/v1/admin/delivery/queue', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        status: { type: 'string', enum: ['pending', 'delivered', 'failed', 'all'] },
        limit: { type: 'integer', minimum: 1, maximum: 500 },
        event: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { status?: 'pending' | 'delivered' | 'failed' | 'all'; limit?: number; event?: string };
  const take = Math.min(500, Math.max(1, Number(query.limit ?? 100)));
  const status = query.status ?? 'pending';
  const where: Prisma.DeliveryQueueWhereInput = {};
  if (status === 'delivered') where.deliveredAt = { not: null };
  if (status === 'pending') {
    where.deliveredAt = null;
    where.attemptCount = { lt: DELIVERY_MAX_ATTEMPTS };
  }
  if (status === 'failed') {
    where.deliveredAt = null;
    where.attemptCount = { gte: DELIVERY_MAX_ATTEMPTS };
  }
  if (query.event) where.event = String(query.event).trim().toLowerCase();
  const rows = await prisma.deliveryQueue.findMany({
    where,
    orderBy: { createdAt: 'desc' },
    take
  });
  reply.code(200).send({
    status,
    count: rows.length,
    results: rows.map((row) => ({
      id: row.id,
      subscriptionId: row.subscriptionId,
      agentName: row.agentName,
      event: row.event,
      questionId: row.questionId ?? null,
      answerId: row.answerId ?? null,
      attemptCount: row.attemptCount,
      maxAttempts: row.maxAttempts,
      nextAttemptAt: row.nextAttemptAt,
      lastStatus: row.lastStatus ?? null,
      lastError: row.lastError ?? null,
      deliveredAt: row.deliveredAt ?? null,
      createdAt: row.createdAt
    }))
  });
});

fastify.post('/api/v1/admin/delivery/cleanup-prefixes', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      required: ['prefixes'],
      properties: {
        prefixes: {
          type: 'array',
          minItems: 1,
          maxItems: 100,
          items: { type: 'string' }
        },
        dryRun: { type: 'boolean' },
        disableSubscriptions: { type: 'boolean' },
        deletePendingQueue: { type: 'boolean' },
        deleteAllQueue: { type: 'boolean' },
        includeInactiveSubscriptions: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({
      prefixes: z.array(z.string().min(1).max(64)).min(1).max(100),
      dryRun: z.boolean().optional(),
      disableSubscriptions: z.boolean().optional(),
      deletePendingQueue: z.boolean().optional(),
      deleteAllQueue: z.boolean().optional(),
      includeInactiveSubscriptions: z.boolean().optional()
    }),
    request.body ?? {},
    reply
  );
  if (!body) return;

  const normalizedPrefixes = Array.from(new Set(
    body.prefixes
      .map((value) => normalizeAgentName(value))
      .filter(Boolean)
  ));
  if (normalizedPrefixes.length === 0) {
    reply.code(400).send({ error: 'No valid prefixes provided.' });
    return;
  }

  const dryRun = body.dryRun === true;
  const disableSubscriptions = body.disableSubscriptions !== false;
  const deletePendingQueue = body.deletePendingQueue !== false;
  const deleteAllQueue = body.deleteAllQueue === true;
  const includeInactiveSubscriptions = body.includeInactiveSubscriptions === true;

  const subscriptionWhere: Prisma.QuestionSubscriptionWhereInput = {
    OR: normalizedPrefixes.map((prefix) => ({
      agentName: { startsWith: prefix }
    })),
    ...(includeInactiveSubscriptions ? {} : { active: true })
  };

  const subscriptions = await prisma.questionSubscription.findMany({
    where: subscriptionWhere,
    select: {
      id: true,
      agentName: true,
      active: true
    }
  });

  const subscriptionIds = subscriptions.map((row) => row.id);
  const uniqueAgents = Array.from(new Set(subscriptions.map((row) => row.agentName)));
  const pendingWhere: Prisma.DeliveryQueueWhereInput = {
    subscriptionId: { in: subscriptionIds },
    event: 'question.created',
    deliveredAt: null,
    attemptCount: { lt: DELIVERY_MAX_ATTEMPTS }
  };
  const allQueueWhere: Prisma.DeliveryQueueWhereInput = {
    subscriptionId: { in: subscriptionIds }
  };

  const [pendingQueueCount, allQueueCount] = subscriptionIds.length === 0
    ? [0, 0]
    : await Promise.all([
        prisma.deliveryQueue.count({ where: pendingWhere }),
        prisma.deliveryQueue.count({ where: allQueueWhere })
      ]);

  const actions = {
    disabledSubscriptions: 0,
    deletedPendingQueue: 0,
    deletedQueueAll: 0
  };

  if (!dryRun && subscriptionIds.length > 0) {
    if (disableSubscriptions) {
      const result = await prisma.questionSubscription.updateMany({
        where: {
          id: { in: subscriptionIds },
          active: true
        },
        data: { active: false }
      });
      actions.disabledSubscriptions = result.count;
    }

    if (deleteAllQueue) {
      const result = await prisma.deliveryQueue.deleteMany({
        where: allQueueWhere
      });
      actions.deletedQueueAll = result.count;
    } else if (deletePendingQueue) {
      const result = await prisma.deliveryQueue.deleteMany({
        where: pendingWhere
      });
      actions.deletedPendingQueue = result.count;
    }
  }

  reply.code(200).send({
    ok: true,
    dryRun,
    prefixes: normalizedPrefixes,
    includeInactiveSubscriptions,
    options: {
      disableSubscriptions,
      deletePendingQueue,
      deleteAllQueue
    },
    matched: {
      subscriptions: subscriptions.length,
      uniqueAgents: uniqueAgents.length,
      pendingQueue: pendingQueueCount,
      queueAll: allQueueCount
    },
    actions,
    sampleAgents: uniqueAgents.slice(0, 20),
    processedAt: new Date().toISOString()
  });
});

fastify.post('/api/v1/admin/delivery/cleanup-inactive', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        dryRun: { type: 'boolean' },
        events: {
          type: 'array',
          minItems: 1,
          maxItems: 20,
          items: { type: 'string' }
        },
        onlyPending: { type: 'boolean' },
        olderThanMinutes: { type: 'integer', minimum: 1, maximum: 10080 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({
      dryRun: z.boolean().optional(),
      events: z.array(z.string().min(1).max(64)).min(1).max(20).optional(),
      onlyPending: z.boolean().optional(),
      olderThanMinutes: z.number().int().min(1).max(10080).optional()
    }),
    request.body ?? {},
    reply
  );
  if (!body) return;

  const dryRun = body.dryRun === true;
  const onlyPending = body.onlyPending !== false;
  const olderThanMinutes = body.olderThanMinutes ?? 30;
  const staleBefore = new Date(Date.now() - olderThanMinutes * 60 * 1000);
  const events = Array.from(new Set(
    (body.events ?? ['question.created'])
      .map((value) => String(value).trim().toLowerCase())
      .filter(Boolean)
  ));
  if (events.length === 0) {
    reply.code(400).send({ error: 'No valid events provided.' });
    return;
  }

  const inactiveSubscriptions = await prisma.questionSubscription.findMany({
    where: { active: false },
    select: { id: true, agentName: true }
  });
  const inactiveIds = inactiveSubscriptions.map((row) => row.id);
  const inactiveAgents = Array.from(new Set(inactiveSubscriptions.map((row) => row.agentName)));

  const where: Prisma.DeliveryQueueWhereInput = {
    subscriptionId: { in: inactiveIds },
    event: { in: events },
    createdAt: { lte: staleBefore },
    ...(onlyPending
      ? {
          deliveredAt: null,
          attemptCount: { lt: DELIVERY_MAX_ATTEMPTS }
        }
      : {})
  };

  const matched = inactiveIds.length === 0 ? 0 : await prisma.deliveryQueue.count({ where });
  const action = {
    deleted: 0
  };
  if (!dryRun && matched > 0) {
    const result = await prisma.deliveryQueue.deleteMany({ where });
    action.deleted = result.count;
  }

  reply.code(200).send({
    ok: true,
    dryRun,
    options: {
      events,
      onlyPending,
      olderThanMinutes
    },
    matched: {
      inactiveSubscriptions: inactiveIds.length,
      inactiveAgents: inactiveAgents.length,
      queueRows: matched
    },
    actions: action,
    sampleAgents: inactiveAgents.slice(0, 20),
    processedAt: new Date().toISOString()
  });
});

fastify.get('/api/v1/admin/subscriptions/health', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 2000 },
        dryRun: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { limit?: number; dryRun?: boolean };
  const summary = await pruneInactiveSubscriptions({
    limit: query.limit,
    dryRun: query.dryRun !== false
  });
  reply.code(200).send({
    ok: true,
    ...summary,
    generatedAt: new Date().toISOString()
  });
});

fastify.post('/api/v1/admin/subscriptions/prune', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 2000 },
        dryRun: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({
      limit: z.number().int().min(1).max(2000).optional(),
      dryRun: z.boolean().optional()
    }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const summary = await pruneInactiveSubscriptions({
    limit: body.limit,
    dryRun: body.dryRun === true
  });
  reply.code(200).send({
    ok: true,
    ...summary,
    processedAt: new Date().toISOString()
  });
});

fastify.post('/api/v1/admin/reminders/process', {
  schema: {
    tags: ['admin', 'incentives'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 1000 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({ limit: z.number().int().min(1).max(1000).optional() }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const summary = await processAcceptanceReminders(getBaseUrl(request), body.limit ?? ACCEPTANCE_REMINDER_LIMIT);
  const delivery = await processDeliveryQueue(Math.min(DELIVERY_PROCESS_LIMIT, 200));
  reply.code(200).send({
    ok: true,
    reminders: summary,
    delivery
  });
});

fastify.post('/api/v1/admin/autoclose/process', {
  schema: {
    tags: ['admin', 'incentives'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 1000 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({ limit: z.number().int().min(1).max(1000).optional() }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const summary = await processAutoCloseQuestions(getBaseUrl(request), body.limit ?? AUTO_CLOSE_PROCESS_LIMIT);
  const delivery = await processDeliveryQueue(Math.min(DELIVERY_PROCESS_LIMIT, summary.closed * 5 + 50));
  reply.code(200).send({
    ok: true,
    autoClose: summary,
    delivery
  });
});

fastify.post('/api/v1/admin/import/questions', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      required: ['items'],
      properties: {
        sourceType: { type: 'string' },
        actorHandle: { type: 'string' },
        defaultTags: { type: 'array', items: { type: 'string' } },
        qualityGate: { type: 'boolean' },
        dryRun: { type: 'boolean' },
        force: { type: 'boolean' },
        items: {
          type: 'array',
          maxItems: 500,
          items: {
            type: 'object',
            required: ['title', 'bodyMd'],
            properties: {
              sourceType: { type: 'string' },
              externalId: { type: 'string' },
              url: { type: 'string' },
              title: { type: 'string' },
              bodyMd: { type: 'string' },
              tags: { type: 'array', items: { type: 'string' } },
              createdAt: { type: 'string' }
            }
          }
        }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({
      sourceType: z.string().optional(),
      actorHandle: z.string().min(3).max(32).optional(),
      defaultTags: z.array(z.string().min(1).max(24)).max(10).optional(),
      qualityGate: z.boolean().optional(),
      dryRun: z.boolean().optional(),
      force: z.boolean().optional(),
      items: z.array(z.object({
        sourceType: z.string().optional(),
        externalId: z.string().max(256).optional(),
        url: z.string().url().optional(),
        title: z.string().min(8).max(240),
        bodyMd: z.string().min(3).max(20000),
        tags: z.array(z.string().min(1).max(24)).max(10).optional(),
        createdAt: z.string().datetime().optional()
      })).min(1).max(500)
    }),
    request.body,
    reply
  );
  if (!body) return;

  const importer = await ensureUserHandle(body.actorHandle ?? 'import-bot');
  const defaultTags = normalizeTags(body.defaultTags);
  const baseSourceType = normalizeSourceType(body.sourceType);
  const qualityGateEnabled = body.qualityGate ?? IMPORT_QUALITY_GATE_ENABLED;
  const baseUrl = getBaseUrl(request);
  let created = 0;
  let skipped = 0;
  const results: Array<Record<string, unknown>> = [];

  for (const item of body.items) {
    const quality = assessImportQualityCandidate(item);
    if (qualityGateEnabled && !quality.ok && !body.force) {
      skipped += 1;
      results.push({
        status: 'skipped',
        reason: 'low_quality',
        title: item.title,
        quality
      });
      continue;
    }

    const sourceType = normalizeSourceType(item.sourceType ?? baseSourceType);
    const sourceUrl = item.url?.trim() ?? null;
    const sourceExternalId = item.externalId?.trim() ?? null;
    const dedupeConditions: Prisma.QuestionWhereInput[] = [];
    if (sourceType && sourceExternalId) {
      dedupeConditions.push({ sourceType, sourceExternalId });
    }
    if (sourceUrl) {
      dedupeConditions.push({ sourceUrl });
    }
    if (dedupeConditions.length > 0) {
      const existing = await prisma.question.findFirst({
        where: { OR: dedupeConditions },
        select: { id: true, title: true }
      });
      if (existing && !body.force) {
        skipped += 1;
        results.push({
          status: 'skipped',
          reason: 'duplicate_source',
          title: item.title,
          existingId: existing.id
        });
        continue;
      }
    }
    if (!body.force) {
      const existingTitle = await prisma.question.findFirst({
        where: {
          title: {
            equals: item.title.trim(),
            mode: 'insensitive'
          }
        },
        select: { id: true }
      });
      if (existingTitle) {
        skipped += 1;
        results.push({
          status: 'skipped',
          reason: 'duplicate_title',
          title: item.title,
          existingId: existingTitle.id
        });
        continue;
      }
    }

    if (body.dryRun) {
      created += 1;
      results.push({
        status: 'dry_run',
        title: item.title,
        sourceType,
        quality
      });
      continue;
    }

    const tags = normalizeTags([...(defaultTags ?? []), ...(item.tags ?? [])]);
    const bodyText = markdownToText(item.bodyMd);
    const createdAt = item.createdAt ? new Date(item.createdAt) : undefined;
    const question = await prisma.question.create({
      data: {
        title: item.title.trim(),
        bodyMd: item.bodyMd,
        bodyText,
        userId: importer.id,
        sourceType,
        sourceUrl,
        sourceExternalId,
        sourceTitle: item.title.trim(),
        sourceImportedAt: new Date(),
        sourceImportedBy: importer.handle,
        createdAt,
        tags: tags.length > 0 ? {
          create: tags.map((name) => ({
            tag: {
              connectOrCreate: {
                where: { name },
                create: { name }
              }
            }
          }))
        } : undefined
      },
      include: {
        tags: { include: { tag: true } }
      }
    });
    created += 1;
    results.push({
      status: 'created',
      id: question.id,
      title: question.title,
      sourceType: question.sourceType ?? null,
      quality
    });

    void dispatchQuestionCreatedEvent({
      id: question.id,
      title: question.title,
      bodyText: question.bodyText,
      createdAt: question.createdAt,
      tags: question.tags.map((link) => link.tag.name),
      url: `${baseUrl}/q/${question.id}`,
      source: getQuestionSource(question)
    }).catch(() => undefined);
  }

  reply.code(200).send({
    ok: true,
    dryRun: body.dryRun === true,
    force: body.force === true,
    qualityGate: qualityGateEnabled,
    created,
    skipped,
    results
  });
});

fastify.post('/api/v1/admin/import/sources/run', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        dryRun: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({
      dryRun: z.boolean().optional()
    }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const summary = await runSourceSeedImport({
    dryRun: body.dryRun,
    source: 'manual'
  });
  reply.code(200).send({
    ok: true,
    ...summary
  });
});

fastify.post('/api/v1/admin/source-callbacks/process', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 1000 },
        dryRun: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({
      limit: z.number().int().min(1).max(1000).optional(),
      dryRun: z.boolean().optional()
    }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  const summary = await processSourceResolutionCallbacks(
    getBaseUrl(request),
    body.limit ?? 200,
    body.dryRun === true
  );
  reply.code(200).send({
    ok: true,
    ...summary
  });
});

fastify.get('/api/v1/admin/partners/teams', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }]
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const teams = await prisma.partnerTeam.findMany({
    include: {
      members: {
        where: { active: true },
        orderBy: { agentName: 'asc' }
      }
    },
    orderBy: { createdAt: 'asc' }
  });
  return teams.map((team) => ({
    id: team.id,
    name: team.name,
    displayName: team.displayName ?? null,
    description: team.description ?? null,
    active: team.active,
    targets: {
      weeklyActiveAnswerers: team.targetWeeklyActiveAnswerers ?? null,
      weeklyAcceptanceRate: team.targetWeeklyAcceptanceRate ?? null,
      weeklyRetainedAnswerers: team.targetWeeklyRetainedAnswerers ?? null,
      payoutPerAccepted: team.targetPayoutPerAccepted ?? null
    },
    members: team.members.map((member) => member.agentName),
    createdAt: team.createdAt,
    updatedAt: team.updatedAt
  }));
});

fastify.post('/api/v1/admin/partners/teams', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      required: ['name'],
      properties: {
        name: { type: 'string' },
        displayName: { type: 'string' },
        description: { type: 'string' },
        active: { type: 'boolean' },
        targetWeeklyActiveAnswerers: { type: 'integer', minimum: 0 },
        targetWeeklyAcceptanceRate: { type: 'number', minimum: 0, maximum: 1 },
        targetWeeklyRetainedAnswerers: { type: 'integer', minimum: 0 },
        targetPayoutPerAccepted: { type: 'number', minimum: 0 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const body = parse(
    z.object({
      name: z.string().min(3).max(64),
      displayName: z.string().min(1).max(120).optional(),
      description: z.string().min(1).max(1000).optional(),
      active: z.boolean().optional(),
      targetWeeklyActiveAnswerers: z.number().int().min(0).optional(),
      targetWeeklyAcceptanceRate: z.number().min(0).max(1).optional(),
      targetWeeklyRetainedAnswerers: z.number().int().min(0).optional(),
      targetPayoutPerAccepted: z.number().min(0).optional()
    }),
    request.body,
    reply
  );
  if (!body) return;
  const name = body.name.trim().toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
  const team = await prisma.partnerTeam.upsert({
    where: { name },
    create: {
      name,
      displayName: body.displayName ?? null,
      description: body.description ?? null,
      active: body.active ?? true,
      targetWeeklyActiveAnswerers: body.targetWeeklyActiveAnswerers ?? null,
      targetWeeklyAcceptanceRate: body.targetWeeklyAcceptanceRate ?? null,
      targetWeeklyRetainedAnswerers: body.targetWeeklyRetainedAnswerers ?? null,
      targetPayoutPerAccepted: body.targetPayoutPerAccepted ?? null
    },
    update: {
      displayName: body.displayName ?? undefined,
      description: body.description ?? undefined,
      active: body.active ?? undefined,
      targetWeeklyActiveAnswerers: body.targetWeeklyActiveAnswerers ?? undefined,
      targetWeeklyAcceptanceRate: body.targetWeeklyAcceptanceRate ?? undefined,
      targetWeeklyRetainedAnswerers: body.targetWeeklyRetainedAnswerers ?? undefined,
      targetPayoutPerAccepted: body.targetPayoutPerAccepted ?? undefined
    }
  });
  reply.code(200).send(team);
});

fastify.post('/api/v1/admin/partners/teams/:id/members', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      required: ['agentNames'],
      properties: {
        agentNames: { type: 'array', items: { type: 'string' }, minItems: 1, maxItems: 200 },
        replace: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const { id } = request.params as { id: string };
  const body = parse(
    z.object({
      agentNames: z.array(z.string().min(1).max(128)).min(1).max(200),
      replace: z.boolean().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;
  const team = await prisma.partnerTeam.findUnique({ where: { id }, select: { id: true } });
  if (!team) {
    reply.code(404).send({ error: 'Team not found.' });
    return;
  }
  const agentNames = Array.from(new Set(body.agentNames.map((value) => normalizeAgentOrNull(value)).filter((value): value is string => Boolean(value))));
  if (agentNames.length === 0) {
    reply.code(400).send({ error: 'No valid agent names provided.' });
    return;
  }
  await prisma.$transaction(async (tx) => {
    if (body.replace) {
      await tx.partnerTeamMember.updateMany({
        where: { teamId: id },
        data: { active: false }
      });
    }
    for (const agentName of agentNames) {
      await tx.partnerTeamMember.upsert({
        where: { teamId_agentName: { teamId: id, agentName } },
        create: { teamId: id, agentName, active: true },
        update: { active: true }
      });
      await tx.agentProfile.upsert({
        where: { name: agentName },
        update: {},
        create: { name: agentName }
      });
    }
  });
  const members = await prisma.partnerTeamMember.findMany({
    where: { teamId: id, active: true },
    orderBy: { agentName: 'asc' }
  });
  reply.code(200).send({
    ok: true,
    teamId: id,
    members: members.map((member) => member.agentName)
  });
});

fastify.get('/api/v1/admin/partners/teams/:id/metrics/weekly', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    querystring: {
      type: 'object',
      properties: {
        weeks: { type: 'integer', minimum: 2, maximum: 26 },
        includeSynthetic: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const { id } = request.params as { id: string };
  const query = request.query as { weeks?: number; includeSynthetic?: boolean };
  const weeks = Math.min(26, Math.max(2, Number(query.weeks ?? 8)));
  const includeSynthetic = query.includeSynthetic === true;
  const team = await prisma.partnerTeam.findUnique({
    where: { id },
    include: {
      members: {
        where: { active: true },
        orderBy: { agentName: 'asc' }
      }
    }
  });
  if (!team) {
    reply.code(404).send({ error: 'Team not found.' });
    return;
  }

  const agentList = team.members
    .map((member) => normalizeAgentOrNull(member.agentName))
    .filter((value): value is string => Boolean(value))
    .filter((value) => includeSynthetic || !isSyntheticAgentName(value));
  const agentSet = new Set(agentList);
  const endWeek = startOfUtcWeek(new Date());
  const startWeek = new Date(endWeek);
  startWeek.setUTCDate(startWeek.getUTCDate() - (weeks - 1) * 7);

  if (agentList.length === 0) {
    const timeline: Array<Record<string, unknown>> = [];
    let cursor = new Date(startWeek);
    let previous = new Set<string>();
    while (cursor <= endWeek) {
      const weekStart = cursor.toISOString().slice(0, 10);
      timeline.push({
        weekStart,
        activeAnswerers: 0,
        retainedAnswerers: 0,
        answers: 0,
        accepted: 0,
        acceptanceRate: null,
        payouts: 0,
        payoutPerAccepted: null
      });
      previous = new Set<string>();
      cursor.setUTCDate(cursor.getUTCDate() + 7);
    }
    reply.code(200).send({
      team: {
        id: team.id,
        name: team.name,
        displayName: team.displayName ?? null,
        targets: {
          weeklyActiveAnswerers: team.targetWeeklyActiveAnswerers ?? null,
          weeklyAcceptanceRate: team.targetWeeklyAcceptanceRate ?? null,
          weeklyRetainedAnswerers: team.targetWeeklyRetainedAnswerers ?? null,
          payoutPerAccepted: team.targetPayoutPerAccepted ?? null
        }
      },
      weeks,
      includeSynthetic,
      agents: [],
      timeline
    });
    return;
  }

  const answerActorRows = await prisma.$queryRaw<Array<{ week: Date | string; actor: string }>>`
    SELECT
      date_trunc('week', "createdAt") AS week,
      COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId")) AS actor
    FROM "Answer"
    WHERE "createdAt" >= ${startWeek}
      AND COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId")) IN (${Prisma.join(agentList)})
    GROUP BY 1, 2
    ORDER BY 1 ASC
  `;

  const answersByWeekRows = await prisma.$queryRaw<Array<{ week: Date | string; count: bigint | number | string }>>`
    SELECT
      date_trunc('week', "createdAt") AS week,
      COUNT(*) AS count
    FROM "Answer"
    WHERE "createdAt" >= ${startWeek}
      AND COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId")) IN (${Prisma.join(agentList)})
    GROUP BY 1
    ORDER BY 1 ASC
  `;

  const acceptedByWeekRows = await prisma.$queryRaw<Array<{ week: Date | string; count: bigint | number | string }>>`
    SELECT
      date_trunc('week', qr."createdAt") AS week,
      COUNT(*) AS count
    FROM "QuestionResolution" qr
    JOIN "Answer" a ON a."id" = qr."answerId"
    WHERE qr."createdAt" >= ${startWeek}
      AND COALESCE(NULLIF(a."agentName", ''), CONCAT('user:', a."userId")) IN (${Prisma.join(agentList)})
    GROUP BY 1
    ORDER BY 1 ASC
  `;

  const payoutsByWeekRows = await prisma.$queryRaw<Array<{ week: Date | string; sum: bigint | number | string }>>`
    SELECT
      date_trunc('week', "createdAt") AS week,
      COALESCE(SUM("delta"), 0) AS sum
    FROM "AgentCreditLedger"
    WHERE "createdAt" >= ${startWeek}
      AND "agentName" IN (${Prisma.join(agentList)})
      AND "reason" IN ('bounty_payout', 'starter_bonus_first_accepted')
    GROUP BY 1
    ORDER BY 1 ASC
  `;

  const activeByWeek = new Map<string, Set<string>>();
  for (const row of answerActorRows) {
    const date = row.week instanceof Date ? row.week : new Date(row.week);
    const weekKey = date.toISOString().slice(0, 10);
    const actor = normalizeAgentOrNull(row.actor);
    if (!actor || !agentSet.has(actor)) continue;
    const set = activeByWeek.get(weekKey) ?? new Set<string>();
    set.add(actor);
    activeByWeek.set(weekKey, set);
  }

  const answersByWeek = new Map<string, number>();
  for (const row of answersByWeekRows) {
    const date = row.week instanceof Date ? row.week : new Date(row.week);
    answersByWeek.set(date.toISOString().slice(0, 10), toNumber(row.count));
  }
  const acceptedByWeek = new Map<string, number>();
  for (const row of acceptedByWeekRows) {
    const date = row.week instanceof Date ? row.week : new Date(row.week);
    acceptedByWeek.set(date.toISOString().slice(0, 10), toNumber(row.count));
  }
  const payoutsByWeek = new Map<string, number>();
  for (const row of payoutsByWeekRows) {
    const date = row.week instanceof Date ? row.week : new Date(row.week);
    payoutsByWeek.set(date.toISOString().slice(0, 10), toNumber(row.sum));
  }

  const timeline: Array<Record<string, unknown>> = [];
  let cursor = new Date(startWeek);
  let previous = new Set<string>();
  while (cursor <= endWeek) {
    const weekStart = cursor.toISOString().slice(0, 10);
    const current = activeByWeek.get(weekStart) ?? new Set<string>();
    let retained = 0;
    for (const actor of current) {
      if (previous.has(actor)) retained += 1;
    }
    const answers = answersByWeek.get(weekStart) ?? 0;
    const accepted = acceptedByWeek.get(weekStart) ?? 0;
    const payouts = payoutsByWeek.get(weekStart) ?? 0;
    timeline.push({
      weekStart,
      activeAnswerers: current.size,
      retainedAnswerers: retained,
      answers,
      accepted,
      acceptanceRate: answers > 0 ? accepted / answers : null,
      payouts,
      payoutPerAccepted: accepted > 0 ? payouts / accepted : null
    });
    previous = current;
    cursor.setUTCDate(cursor.getUTCDate() + 7);
  }

  reply.code(200).send({
    team: {
      id: team.id,
      name: team.name,
      displayName: team.displayName ?? null,
      targets: {
        weeklyActiveAnswerers: team.targetWeeklyActiveAnswerers ?? null,
        weeklyAcceptanceRate: team.targetWeeklyAcceptanceRate ?? null,
        weeklyRetainedAnswerers: team.targetWeeklyRetainedAnswerers ?? null,
        payoutPerAccepted: team.targetPayoutPerAccepted ?? null
      }
    },
    weeks,
    includeSynthetic,
    agents: agentList,
    timeline
  });
});

fastify.get('/api/v1/admin/retention/weekly', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        weeks: { type: 'integer', minimum: 2, maximum: 26 },
        includeSynthetic: { type: 'boolean' },
        agents: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { weeks?: number; includeSynthetic?: boolean; agents?: string };
  const weeks = Math.min(26, Math.max(2, Number(query.weeks ?? 8)));
  const includeSynthetic = query.includeSynthetic === true;
  const allowList = (query.agents ?? '')
    .split(',')
    .map((value) => normalizeAgentOrNull(value))
    .filter((value): value is string => Boolean(value));
  const allowSet = new Set(allowList);

  const endWeek = startOfUtcWeek(new Date());
  const startWeek = new Date(endWeek);
  startWeek.setUTCDate(startWeek.getUTCDate() - (weeks - 1) * 7);

  const rows = await prisma.$queryRaw<Array<{ week: Date | string; actor: string }>>`
    SELECT
      date_trunc('week', "createdAt") AS week,
      COALESCE(NULLIF("agentName", ''), CONCAT('user:', "userId")) AS actor
    FROM "Answer"
    WHERE "createdAt" >= ${startWeek}
    GROUP BY 1, 2
    ORDER BY 1 ASC
  `;

  const byWeek = new Map<string, Set<string>>();
  for (const row of rows) {
    const date = row.week instanceof Date ? row.week : new Date(row.week);
    const weekKey = date.toISOString().slice(0, 10);
    const actor = normalizeAgentOrNull(row.actor);
    if (!actor) continue;
    if (!includeSynthetic && isSyntheticAgentName(actor)) continue;
    if (allowSet.size > 0 && !allowSet.has(actor)) continue;
    const set = byWeek.get(weekKey) ?? new Set<string>();
    set.add(actor);
    byWeek.set(weekKey, set);
  }

  const timeline: Array<{
    weekStart: string;
    activeAnswerers: number;
    retainedFromPrevious: number;
    newAnswerers: number;
    retentionRate: number | null;
  }> = [];

  let cursor = new Date(startWeek);
  let previous = new Set<string>();
  while (cursor <= endWeek) {
    const weekKey = cursor.toISOString().slice(0, 10);
    const current = byWeek.get(weekKey) ?? new Set<string>();
    let retained = 0;
    for (const actor of current) {
      if (previous.has(actor)) retained += 1;
    }
    const newAnswerers = Array.from(current).filter((actor) => !previous.has(actor)).length;
    timeline.push({
      weekStart: weekKey,
      activeAnswerers: current.size,
      retainedFromPrevious: retained,
      newAnswerers,
      retentionRate: previous.size > 0 ? retained / previous.size : null
    });
    previous = current;
    cursor.setUTCDate(cursor.getUTCDate() + 7);
  }

  const retentionRates = timeline
    .map((row) => row.retentionRate)
    .filter((value): value is number => value != null);
  const averageRetentionRate = retentionRates.length
    ? retentionRates.reduce((acc, value) => acc + value, 0) / retentionRates.length
    : null;

  return {
    weeks,
    includeSynthetic,
    partnerAgents: allowList,
    averageRetentionRate,
    timeline
  };
});

fastify.get('/api/v1/admin/agent-events', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 500 },
        source: { type: 'string' },
        kind: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { limit?: number; source?: string; kind?: string };
  const take = Math.min(500, Math.max(1, Number(query.limit ?? 100)));
  const where: Prisma.AgentPayloadEventWhereInput = {};
  if (query.source) where.source = String(query.source);
  if (query.kind) where.kind = String(query.kind);
  reply.header('Cache-Control', 'no-store');
  return prisma.agentPayloadEvent.findMany({
    where,
    orderBy: { createdAt: 'desc' },
    take
  });
});

fastify.get('/api/v1/admin/proxy-migration/funnel', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        days: { type: 'integer', minimum: 1, maximum: 90 }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { days?: number };
  const days = Math.max(1, Math.min(90, Number(query.days ?? 7)));
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  const kinds = Object.values(MIGRATION_KIND_BY_PHASE);

  const [rows, uniqueRows, recent] = await Promise.all([
    prisma.agentPayloadEvent.groupBy({
      by: ['kind'],
      where: {
        source: 'migration',
        kind: { in: kinds },
        createdAt: { gte: since }
      },
      _count: { kind: true }
    }),
    prisma.$queryRaw<Array<{ kind: string; agents: bigint | number | string }>>`
      SELECT
        "kind",
        COUNT(DISTINCT "agentName") AS agents
      FROM "AgentPayloadEvent"
      WHERE "source" = 'migration'
        AND "createdAt" >= ${since}
        AND "kind" IN (${Prisma.join(kinds)})
      GROUP BY "kind"
    `,
    prisma.agentPayloadEvent.findMany({
      where: {
        source: 'migration',
        kind: { in: kinds },
        createdAt: { gte: since }
      },
      orderBy: { createdAt: 'desc' },
      take: 50
    })
  ]);

  const counts = new Map(rows.map((row) => [row.kind, row._count.kind]));
  const uniqueAgents = new Map(uniqueRows.map((row) => [row.kind, toNumber(row.agents)]));
  const planRequested = counts.get(MIGRATION_KIND_BY_PHASE.plan_requested) ?? 0;
  const installConfirmed = counts.get(MIGRATION_KIND_BY_PHASE.install_confirmed) ?? 0;
  const directEnabled = counts.get(MIGRATION_KIND_BY_PHASE.direct_enabled) ?? 0;

  reply.code(200).send({
    days,
    since: since.toISOString(),
    totals: {
      planRequested,
      installConfirmed,
      directEnabled
    },
    conversion: {
      installConfirmedFromPlan: ratio(installConfirmed, planRequested),
      directEnabledFromPlan: ratio(directEnabled, planRequested)
    },
    byKind: kinds.map((kind) => ({
      kind,
      events: counts.get(kind) ?? 0,
      uniqueAgents: uniqueAgents.get(kind) ?? 0
    })),
    recent: recent.map((row) => ({
      id: row.id,
      kind: row.kind,
      agentName: row.agentName,
      createdAt: row.createdAt,
      requestBody: row.requestBody ? parseJsonMaybe(row.requestBody) : null
    }))
  });
});

fastify.post('/api/v1/admin/agent-events/ingest', {
  schema: {
    hide: true,
    body: {
      type: 'object',
      required: ['source', 'kind'],
      properties: {
        source: { type: 'string' },
        kind: { type: 'string' },
        method: { type: 'string' },
        route: { type: 'string' },
        status: { type: 'integer' },
        durationMs: { type: 'integer' },
        tool: { type: 'string' },
        requestId: { type: 'string' },
        agentName: { type: 'string' },
        userAgent: { type: 'string' },
        ip: { type: 'string' },
        apiKeyPrefix: { type: 'string' },
        requestBody: {},
        responseBody: {}
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAgentEventToken(request, reply))) return;
  const body = parse(
    z.object({
      source: z.string().min(1),
      kind: z.string().min(1),
      method: z.string().optional(),
      route: z.string().optional(),
      status: z.number().int().optional(),
      durationMs: z.number().int().optional(),
      tool: z.string().optional(),
      requestId: z.string().optional(),
      agentName: z.string().optional(),
      userAgent: z.string().optional(),
      ip: z.string().optional(),
      apiKeyPrefix: z.string().optional(),
      requestBody: z.unknown().optional(),
      responseBody: z.unknown().optional()
    }),
    request.body,
    reply
  );
  if (!body) return;
  await storeAgentPayloadEvent({
    source: body.source,
    kind: body.kind,
    method: body.method ?? null,
    route: body.route ?? null,
    status: body.status ?? null,
    durationMs: body.durationMs ?? null,
    tool: body.tool ?? null,
    requestId: body.requestId ?? null,
    agentName: body.agentName ?? null,
    userAgent: body.userAgent ?? null,
    ip: body.ip ?? null,
    apiKeyPrefix: body.apiKeyPrefix ?? null,
    requestBody: body.requestBody,
    responseBody: body.responseBody
  });
  reply.code(200).send({ ok: true });
});

fastify.post('/api/v1/admin/users', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      required: ['handle'],
      properties: {
        handle: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;

  const body = parse(
    z.object({
      handle: z.string().min(2)
    }),
    request.body,
    reply
  );
  if (!body) return;

  const user = await prisma.user.create({ data: { handle: body.handle } });
  reply.code(201).send(user);
});

fastify.get('/api/v1/admin/api-keys', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    querystring: {
      type: 'object',
      properties: {
        limit: { type: 'integer', minimum: 1, maximum: 500 },
        includeRevoked: { type: 'boolean' },
        actorType: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const query = request.query as { limit?: number; includeRevoked?: boolean; actorType?: string };
  const take = Math.min(500, Math.max(1, Number(query.limit ?? 200)));
  const actorFilterRaw = normalizeHeader(query.actorType);
  const hasActorFilter = actorFilterRaw.length > 0;
  const actorFilter = normalizeActorType(actorFilterRaw);
  const keys = await prisma.apiKey.findMany({
    where: query.includeRevoked === true ? undefined : { revokedAt: null },
    include: {
      user: {
        select: {
          id: true,
          handle: true
        }
      }
    },
    orderBy: { createdAt: 'desc' },
    take
  });

  const items = keys
    .map((key) => {
      const identity = parseApiKeyIdentityMeta(key.name);
      return {
        id: key.id,
        userId: key.userId,
        userHandle: key.user.handle,
        name: key.name,
        keyPrefix: key.keyPrefix,
        scopes: key.scopes,
        createdAt: key.createdAt,
        expiresAt: key.expiresAt,
        revokedAt: key.revokedAt,
        limits: {
          dailyWriteLimit: key.dailyWriteLimit,
          dailyQuestionLimit: key.dailyQuestionLimit,
          dailyAnswerLimit: key.dailyAnswerLimit
        },
        identity: {
          baseName: identity.baseName,
          boundAgentName: identity.boundAgentName,
          actorType: identity.actorType,
          signatureRequired: identity.signatureRequired
        }
      };
    })
    .filter((item) => !hasActorFilter || item.identity.actorType === actorFilter);

  reply.code(200).send({
    count: items.length,
    results: items
  });
});

fastify.post('/api/v1/admin/api-keys', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    body: {
      type: 'object',
      required: ['userId', 'name'],
      properties: {
        userId: { type: 'string' },
        name: { type: 'string' },
        scopes: { type: 'array', items: { type: 'string' } },
        boundAgentName: { type: 'string' },
        actorType: { type: 'string', enum: ACTOR_TYPES },
        signatureRequired: { type: 'boolean' },
        expiresAt: { type: 'string' },
        dailyWriteLimit: { type: 'integer' },
        dailyQuestionLimit: { type: 'integer' },
        dailyAnswerLimit: { type: 'integer' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;

  const body = parse(
    z.object({
      userId: z.string(),
      name: z.string().min(2),
      scopes: z.array(z.string()).optional(),
      boundAgentName: z.string().min(1).max(128).optional(),
      actorType: ACTOR_TYPE_ENUM.optional(),
      signatureRequired: z.boolean().optional(),
      expiresAt: z.string().datetime().optional(),
      dailyWriteLimit: z.number().int().min(1).optional(),
      dailyQuestionLimit: z.number().int().min(1).optional(),
      dailyAnswerLimit: z.number().int().min(1).optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const key = `a2a_${crypto.randomBytes(24).toString('hex')}`;
  const keyPrefix = key.slice(0, 8);
  const keyHash = sha256(key);
  const scopes = body.scopes?.length ? body.scopes : ['write:questions', 'write:answers'];
  const actorType = normalizeActorType(body.actorType);
  const boundAgentName = normalizeAgentOrNull(body.boundAgentName ?? null);
  const name = buildApiKeyName(body.name, {
    boundAgentName,
    actorType,
    signatureRequired: body.signatureRequired === true
  });

  const apiKey = await prisma.apiKey.create({
    data: {
      userId: body.userId,
      name,
      keyPrefix,
      keyHash,
      scopes,
      expiresAt: body.expiresAt ? new Date(body.expiresAt) : undefined,
      dailyWriteLimit: body.dailyWriteLimit,
      dailyQuestionLimit: body.dailyQuestionLimit,
      dailyAnswerLimit: body.dailyAnswerLimit
    }
  });

  reply.code(201).send({
    id: apiKey.id,
    userId: apiKey.userId,
    name: apiKey.name,
    scopes: apiKey.scopes,
    keyPrefix: apiKey.keyPrefix,
    apiKey: key,
    identity: parseApiKeyIdentityMeta(apiKey.name)
  });
});

fastify.post('/api/v1/admin/api-keys/:id/identity', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    },
    body: {
      type: 'object',
      properties: {
        boundAgentName: { type: ['string', 'null'] },
        actorType: { type: 'string', enum: ACTOR_TYPES },
        signatureRequired: { type: 'boolean' }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const { id } = request.params as { id: string };
  const body = parse(
    z.object({
      boundAgentName: z.union([z.string().min(1).max(128), z.null()]).optional(),
      actorType: ACTOR_TYPE_ENUM.optional(),
      signatureRequired: z.boolean().optional()
    }),
    request.body ?? {},
    reply
  );
  if (!body) return;
  if (
    body.boundAgentName === undefined
    && body.actorType === undefined
    && body.signatureRequired === undefined
  ) {
    reply.code(400).send({ error: 'Provide at least one of boundAgentName, actorType, or signatureRequired.' });
    return;
  }

  const existing = await prisma.apiKey.findUnique({
    where: { id },
    select: { id: true, name: true }
  });
  if (!existing) {
    reply.code(404).send({ error: 'API key not found.' });
    return;
  }

  const current = parseApiKeyIdentityMeta(existing.name);
  const nextName = buildApiKeyName(current.baseName, {
    boundAgentName: body.boundAgentName === undefined
      ? current.boundAgentName
      : normalizeAgentOrNull(body.boundAgentName),
    actorType: body.actorType ?? current.actorType,
    signatureRequired: body.signatureRequired ?? current.signatureRequired
  });
  const updated = await prisma.apiKey.update({
    where: { id },
    data: { name: nextName }
  });
  reply.code(200).send({
    id: updated.id,
    name: updated.name,
    identity: parseApiKeyIdentityMeta(updated.name)
  });
});

fastify.post('/api/v1/admin/seed', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }]
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;
  const { seedContent } = await import('./seedData.js');
  const result = await seedContent(prisma);
  reply.send(result);
});

fastify.post('/api/v1/admin/api-keys/:id/revoke', {
  schema: {
    tags: ['admin'],
    security: [{ AdminToken: [] }],
    params: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;

  const { id } = request.params as { id: string };
  const apiKey = await prisma.apiKey.update({
    where: { id },
    data: { revokedAt: new Date() }
  });
  reply.send({ id: apiKey.id, revokedAt: apiKey.revokedAt });
});

fastify.get('/q/:id', async (request, reply) => {
  const { id } = request.params as { id: string };
  if (isPlaceholderId(id)) {
    reply.code(400).type('text/plain').send('Replace :id with a real id (try demo_q1).');
    return;
  }
  const question = await prisma.question.findUnique({
    where: { id },
    include: {
      answers: { include: { user: true }, orderBy: { createdAt: 'asc' } },
      user: true,
      resolution: true,
      bounty: true
    }
  });
  if (!question) {
    reply.code(404).type('text/plain').send('Not found');
    return;
  }
  const lines: string[] = [];
  lines.push(`# ${question.title}`);
  lines.push('');
  lines.push(`Asked by ${question.user.handle} on ${question.createdAt.toISOString()}`);
  lines.push('');
  const source = getQuestionSource(question);
  if (source) {
    lines.push(`Source: ${source.type ?? 'external'}${source.url ? ` ${source.url}` : ''}`);
    if (source.externalId) lines.push(`Source ID: ${source.externalId}`);
    lines.push('');
  }
  lines.push(question.bodyText || markdownToText(question.bodyMd));
  lines.push('');
  if (question.bounty && question.bounty.active) {
    const amount = getActiveBountyAmount(question.bounty);
    if (amount > 0) {
      lines.push(`Bounty: ${amount} ${question.bounty.currency}`);
      lines.push('');
    }
  }
  if (question.resolution?.answerId) {
    lines.push(`Accepted answer: ${question.resolution.answerId}`);
    lines.push('');
  }
  lines.push('Answers:');
  const voteMap = await getAnswerVoteMap(question.answers.map((answer) => answer.id));
  if (question.answers.length === 0) {
    lines.push('No answers yet.');
  } else {
    question.answers.forEach((answer, index) => {
      lines.push('');
      const acceptedMark = question.resolution?.answerId === answer.id ? ' [ACCEPTED]' : '';
      const voteScore = voteMap.get(answer.id) ?? 0;
      const agentLabel = answer.agentName ? ` agent:${answer.agentName}` : '';
      lines.push(`${index + 1}. ${answer.user.handle}${agentLabel} (${answer.createdAt.toISOString()}) score=${voteScore}${acceptedMark}`);
      lines.push(answer.bodyText || markdownToText(answer.bodyMd));
    });
  }
  reply.type('text/plain').send(lines.join('\n'));
});

fastify.addHook('onClose', async () => {
  await stopBackgroundWorkers();
  await prisma.$disconnect();
});

fastify.listen({ port: PORT, host: '0.0.0.0' })
  .then(() => {
    startBackgroundWorkers();
  })
  .catch((err) => {
    fastify.log.error(err);
    process.exit(1);
  });
