import 'dotenv/config';

const API_BASE_URL = process.env.API_BASE_URL ?? 'http://127.0.0.1:3000';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN ?? '';
const TEAM_COUNT = Math.min(5, Math.max(3, Number(process.env.TEAM_COUNT ?? 5)));
const WEEKS = Math.min(12, Math.max(2, Number(process.env.WEEKS ?? 8)));

if (!ADMIN_TOKEN) {
  console.error('Missing ADMIN_TOKEN env var.');
  process.exit(1);
}

type LeaderboardRow = {
  name: string;
  answersCount: number;
};

type TeamPayload = {
  id: string;
  name: string;
  displayName: string | null;
};

async function api(path: string, init?: RequestInit) {
  const res = await fetch(`${API_BASE_URL}${path}`, init);
  const text = await res.text();
  let json: unknown = text;
  try {
    json = JSON.parse(text);
  } catch {
    // keep text
  }
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} ${path}: ${typeof json === 'string' ? json : JSON.stringify(json)}`);
  }
  return json;
}

async function getCandidateAgents() {
  const data = await api('/api/v1/agents/leaderboard?limit=100&includeSynthetic=true') as {
    items?: LeaderboardRow[];
  };
  const rows = data.items ?? [];
  const names = rows
    .map((row) => row.name?.trim().toLowerCase())
    .filter((name): name is string => Boolean(name))
    .filter((name) => !name.startsWith('user:'));
  return Array.from(new Set(names));
}

async function upsertTeam(name: string, displayName: string) {
  const payload = {
    name,
    displayName,
    description: `Partner GTM pilot team: ${displayName}`,
    active: true,
    targetWeeklyActiveAnswerers: 5,
    targetWeeklyAcceptanceRate: 0.45,
    targetWeeklyRetainedAnswerers: 3,
    targetPayoutPerAccepted: 25
  };
  return api('/api/v1/admin/partners/teams', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-admin-token': ADMIN_TOKEN
    },
    body: JSON.stringify(payload)
  }) as Promise<TeamPayload>;
}

async function assignMembers(teamId: string, members: string[]) {
  return api(`/api/v1/admin/partners/teams/${teamId}/members`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-admin-token': ADMIN_TOKEN
    },
    body: JSON.stringify({
      agentNames: members,
      replace: true
    })
  }) as Promise<{ members: string[] }>;
}

async function getWeeklyMetrics(teamId: string) {
  return api(`/api/v1/admin/partners/teams/${teamId}/metrics/weekly?weeks=${WEEKS}&includeSynthetic=true`, {
    headers: { 'x-admin-token': ADMIN_TOKEN }
  }) as Promise<{
    team?: { id: string; name: string };
    timeline?: Array<{
      weekStart: string;
      activeAnswerers: number;
      retainedAnswerers: number;
      acceptanceRate: number | null;
      payoutPerAccepted: number | null;
    }>;
  }>;
}

function chunkRoundRobin(names: string[], chunks: number) {
  const out: string[][] = Array.from({ length: chunks }, () => []);
  names.forEach((name, index) => out[index % chunks].push(name));
  return out;
}

async function main() {
  const leaderAgents = await getCandidateAgents();
  const fallbackAgents = [
    'partner-agent-1',
    'partner-agent-2',
    'partner-agent-3',
    'partner-agent-4',
    'partner-agent-5',
    'partner-agent-6',
    'partner-agent-7',
    'partner-agent-8',
    'partner-agent-9',
    'partner-agent-10'
  ];
  const pool = Array.from(new Set([...leaderAgents, ...fallbackAgents]));
  const selectedPool = pool.slice(0, Math.max(TEAM_COUNT * 3, 9));
  const allocations = chunkRoundRobin(selectedPool, TEAM_COUNT);

  const stamp = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const teamDefs = [
    { name: `pilot-infra-${stamp}`, displayName: 'Infra Agents' },
    { name: `pilot-frameworks-${stamp}`, displayName: 'Framework Agents' },
    { name: `pilot-runtime-${stamp}`, displayName: 'Runtime Agents' },
    { name: `pilot-data-${stamp}`, displayName: 'Data Agents' },
    { name: `pilot-devtools-${stamp}`, displayName: 'DevTools Agents' }
  ].slice(0, TEAM_COUNT);

  const summary: Array<Record<string, unknown>> = [];
  for (let i = 0; i < teamDefs.length; i += 1) {
    const def = teamDefs[i];
    const team = await upsertTeam(def.name, def.displayName);
    const assigned = await assignMembers(team.id, allocations[i]);
    const metrics = await getWeeklyMetrics(team.id);
    const latest = metrics.timeline?.at(-1) ?? null;
    summary.push({
      teamId: team.id,
      teamName: team.name,
      members: assigned.members,
      latestWeek: latest
    });
  }

  console.log(JSON.stringify({
    apiBaseUrl: API_BASE_URL,
    teamCount: TEAM_COUNT,
    candidateAgents: leaderAgents.length,
    selectedAgents: selectedPool.length,
    teams: summary
  }, null, 2));
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});

