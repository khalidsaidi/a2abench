import { spawn } from 'node:child_process';

function boolEnv(raw: string | undefined, fallback: boolean) {
  if (raw == null) return fallback;
  const normalized = raw.trim().toLowerCase();
  if (normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on') return true;
  if (normalized === '0' || normalized === 'false' || normalized === 'no' || normalized === 'off') return false;
  return fallback;
}

function numEnv(raw: string | undefined, fallback: number, min = 1, max = Number.MAX_SAFE_INTEGER) {
  const parsed = Number(raw ?? fallback);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(parsed)));
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function runCommand(name: string, cmd: string, args: string[], extraEnv?: Record<string, string>) {
  return new Promise<{ name: string; ok: boolean; code: number | null; durationMs: number }>((resolve) => {
    const started = Date.now();
    const child = spawn(cmd, args, {
      stdio: 'inherit',
      env: {
        ...process.env,
        ...(extraEnv ?? {})
      }
    });
    child.on('exit', (code) => {
      const durationMs = Date.now() - started;
      const ok = code === 0;
      resolve({ name, ok, code, durationMs });
    });
    child.on('error', () => {
      const durationMs = Date.now() - started;
      resolve({ name, ok: false, code: null, durationMs });
    });
  });
}

async function main() {
  const API_BASE_URL = process.env.API_BASE_URL ?? 'https://a2abench-api.web.app';
  const ADMIN_TOKEN = process.env.ADMIN_TOKEN ?? '';
  const RUN_ONCE = boolEnv(process.env.GROWTH_RUN_ONCE, false);
  const LOOP_MINUTES = numEnv(process.env.GROWTH_LOOP_MINUTES, 180, 5, 24 * 60);
  const ENABLE_IMPORT = boolEnv(process.env.GROWTH_ENABLE_IMPORT, true);
  const ENABLE_PARTNER_SETUP = boolEnv(process.env.GROWTH_ENABLE_PARTNER_SETUP, true);
  const ENABLE_SWARM_BURST = boolEnv(process.env.GROWTH_ENABLE_SWARM_BURST, false);
  const SWARM_TARGET_ANSWERS = numEnv(process.env.GROWTH_SWARM_TARGET_ANSWERS, 60, 1, 5000);

  process.stdout.write(JSON.stringify({
    mode: RUN_ONCE ? 'once' : 'loop',
    loopMinutes: LOOP_MINUTES,
    apiBaseUrl: API_BASE_URL,
    enableImport: ENABLE_IMPORT,
    enablePartnerSetup: ENABLE_PARTNER_SETUP,
    enableSwarmBurst: ENABLE_SWARM_BURST
  }) + '\n');

  let iteration = 0;
  while (true) {
    iteration += 1;
    const loopStarted = Date.now();
    const results: Array<{ name: string; ok: boolean; code: number | null; durationMs: number; skipped?: string }> = [];

    const canRunAdminSteps = ADMIN_TOKEN.trim().length > 0;
    if (ENABLE_IMPORT) {
      if (!canRunAdminSteps) {
        results.push({ name: 'import-selected-sources', ok: false, code: null, durationMs: 0, skipped: 'ADMIN_TOKEN missing' });
      } else {
        const run = await runCommand(
          'import-selected-sources',
          'pnpm',
          ['exec', 'tsx', 'scripts/import-selected-sources.ts'],
          { API_BASE_URL, ADMIN_TOKEN }
        );
        results.push(run);
      }
    }

    if (ENABLE_PARTNER_SETUP) {
      if (!canRunAdminSteps) {
        results.push({ name: 'setup-partner-gtm', ok: false, code: null, durationMs: 0, skipped: 'ADMIN_TOKEN missing' });
      } else {
        const run = await runCommand(
          'setup-partner-gtm',
          'pnpm',
          ['exec', 'tsx', 'scripts/setup-partner-gtm.ts'],
          { API_BASE_URL, ADMIN_TOKEN }
        );
        results.push(run);
      }
    }

    if (ENABLE_SWARM_BURST) {
      const run = await runCommand(
        'agent-swarm-burst',
        '/bin/bash',
        ['-lc', `API_BASE_URL='${API_BASE_URL}' timeout 900s ./scripts/run-agent-swarm-burst.sh ${SWARM_TARGET_ANSWERS}`]
      );
      results.push(run);
    }

    const ok = results.every((row) => row.ok || Boolean(row.skipped));
    process.stdout.write(JSON.stringify({
      iteration,
      startedAt: new Date(loopStarted).toISOString(),
      durationMs: Date.now() - loopStarted,
      ok,
      results
    }) + '\n');

    if (RUN_ONCE) break;
    const waitMs = LOOP_MINUTES * 60 * 1000;
    await sleep(waitMs);
  }
}

main().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
