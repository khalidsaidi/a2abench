import 'dotenv/config';

type ImportItem = {
  sourceType: string;
  externalId: string;
  url: string;
  title: string;
  bodyMd: string;
  tags: string[];
};

const API_BASE_URL = process.env.API_BASE_URL ?? 'http://127.0.0.1:3000';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN ?? '';
const ACTOR_HANDLE = process.env.IMPORT_ACTOR_HANDLE ?? 'import-bot';
const DRY_RUN = (process.env.DRY_RUN ?? 'false').toLowerCase() === 'true';

const GITHUB_REPOS = listEnv(
  process.env.IMPORT_GITHUB_REPOS,
  [
    'vercel/next.js',
    'microsoft/typescript',
    'nodejs/node',
    'facebook/react',
    'vitejs/vite',
    'tailwindlabs/tailwindcss',
    'prisma/prisma',
    'python/cpython',
    'golang/go',
    'rust-lang/rust'
  ]
);
const GITHUB_PER_REPO = intEnv(process.env.IMPORT_GITHUB_PER_REPO, 20);

const DISCORD_REPOS = listEnv(
  process.env.IMPORT_DISCORD_REPOS,
  ['discord/discord-api-docs', 'discordjs/discord.js']
);
const DISCORD_PER_REPO = intEnv(process.env.IMPORT_DISCORD_PER_REPO, 15);

const DISCOURSE_SITES = listEnv(
  process.env.IMPORT_DISCOURSE_SITES,
  [
    'https://community.vercel.com',
    'https://community.fly.io',
    'https://discuss.python.org',
    'https://users.rust-lang.org'
  ]
);
const DISCOURSE_PER_SITE = intEnv(process.env.IMPORT_DISCOURSE_PER_SITE, 12);

const STACKOVERFLOW_TAGS = listEnv(
  process.env.IMPORT_STACKOVERFLOW_TAGS,
  ['javascript', 'typescript', 'python', 'node.js', 'reactjs']
);
const STACKOVERFLOW_PER_TAG = intEnv(process.env.IMPORT_STACKOVERFLOW_PER_TAG, 12);

const MAX_TOTAL = intEnv(process.env.IMPORT_MAX_TOTAL, 500);

if (!ADMIN_TOKEN) {
  console.error('Missing ADMIN_TOKEN env var.');
  process.exit(1);
}

function intEnv(raw: string | undefined, fallback: number) {
  const parsed = Number(raw ?? fallback);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.floor(parsed));
}

function listEnv(raw: string | undefined, fallback: string[]) {
  if (!raw || !raw.trim()) return fallback;
  return raw
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
}

function stripMarkdown(value: string | null | undefined, max = 1800) {
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

function normalizeImportTags(tags: string[]) {
  const cleaned = tags
    .map((tag) => tag.trim().toLowerCase())
    .map((tag) => tag.replace(/[^a-z0-9._-]+/g, '-'))
    .map((tag) => tag.replace(/-+/g, '-').replace(/^-+|-+$/g, ''))
    .filter(Boolean)
    .map((tag) => tag.slice(0, 24))
    .filter(Boolean);
  return Array.from(new Set(cleaned)).slice(0, 10);
}

async function fetchGitHubIssues(repo: string, limit: number, sourceType: 'github' | 'discord', maxPages = 2) {
  const items: ImportItem[] = [];
  const perPage = Math.min(100, Math.max(limit, 25));
  for (let page = 1; page <= maxPages && items.length < limit; page += 1) {
    const res = await fetch(`https://api.github.com/repos/${repo}/issues?state=open&per_page=${perPage}&page=${page}&sort=updated&direction=desc`, {
      headers: {
        accept: 'application/vnd.github+json',
        'user-agent': 'a2abench-importer'
      }
    });
    if (!res.ok) throw new Error(`GitHub issues fetch failed for ${repo}: ${res.status}`);
    const rows = (await res.json()) as Array<Record<string, unknown>>;
    if (rows.length === 0) break;
    for (const row of rows) {
      if (row.pull_request) continue;
      const id = Number(row.number);
      if (!Number.isFinite(id)) continue;
      const title = String(row.title ?? '').trim();
      const body = stripMarkdown(String(row.body ?? ''), 4000);
      const htmlUrl = String(row.html_url ?? '').trim();
      if (!title || !htmlUrl) continue;
      items.push({
        sourceType,
        externalId: `${repo}#${id}`,
        url: htmlUrl,
        title,
        bodyMd: body ? `${body}\n\nSource: ${htmlUrl}` : `Source: ${htmlUrl}`,
        tags: sourceType === 'discord' ? ['discord', 'api'] : ['github', 'issues']
      });
      if (items.length >= limit) break;
    }
  }
  return items;
}

async function fetchDiscourseTopics(baseUrl: string, limit: number) {
  const url = `${baseUrl.replace(/\/$/, '')}/latest.json`;
  const res = await fetch(url, { headers: { 'user-agent': 'a2abench-importer' } });
  if (!res.ok) throw new Error(`Discourse latest fetch failed for ${baseUrl}: ${res.status}`);
  const payload = await res.json() as {
    topic_list?: {
      topics?: Array<{
        id: number;
        slug: string;
        title: string;
        posts_count?: number;
        views?: number;
        closed?: boolean;
        archived?: boolean;
      }>;
    };
  };

  const topics = payload.topic_list?.topics ?? [];
  const unresolved = topics
    .filter((topic) => !topic.closed && !topic.archived)
    .sort((a, b) => (a.posts_count ?? 0) - (b.posts_count ?? 0))
    .slice(0, limit);

  const items: ImportItem[] = [];
  for (const topic of unresolved) {
    const link = `${baseUrl.replace(/\/$/, '')}/t/${topic.slug}/${topic.id}`;
    const title = topic.title.trim();
    if (!title) continue;
    items.push({
      sourceType: 'dev-support',
      externalId: `${new URL(baseUrl).host}#${topic.id}`,
      url: link,
      title,
      bodyMd: `Imported from dev-support forum thread.\n\nViews: ${topic.views ?? 0}\nPosts: ${topic.posts_count ?? 0}\nSource: ${link}`,
      tags: ['support', 'community']
    });
  }
  return items;
}

async function fetchStackOverflowNoAnswers(tag: string, limit: number) {
  const encodedTag = encodeURIComponent(tag);
  const url = `https://api.stackexchange.com/2.3/questions/no-answers?order=desc&sort=creation&site=stackoverflow&tagged=${encodedTag}&pagesize=${Math.min(100, Math.max(1, limit))}`;
  const res = await fetch(url, {
    headers: {
      accept: 'application/json',
      'user-agent': 'a2abench-importer'
    }
  });
  if (!res.ok) throw new Error(`StackOverflow fetch failed for tag ${tag}: ${res.status}`);
  const payload = await res.json() as {
    items?: Array<{
      question_id: number;
      title: string;
      link: string;
      tags?: string[];
      owner?: { display_name?: string };
      creation_date?: number;
      score?: number;
      view_count?: number;
      answer_count?: number;
    }>;
  };
  const rows = payload.items ?? [];

  const items: ImportItem[] = [];
  for (const row of rows.slice(0, limit)) {
    const id = row.question_id;
    const title = (row.title ?? '').trim();
    const link = (row.link ?? '').trim();
    if (!id || !title || !link) continue;
    const owner = row.owner?.display_name ?? 'unknown';
    const tags = Array.from(new Set(['stackoverflow', tag, ...(row.tags ?? []).slice(0, 3)])).slice(0, 5);
    items.push({
      sourceType: 'dev-support',
      externalId: `stackoverflow#${id}`,
      url: link,
      title,
      bodyMd: `Imported from Stack Overflow unanswered queue.\n\nOwner: ${owner}\nScore: ${row.score ?? 0}\nViews: ${row.view_count ?? 0}\nAnswers: ${row.answer_count ?? 0}\nSource: ${link}`,
      tags
    });
  }
  return items;
}

async function postImport(items: ImportItem[]) {
  const body = {
    sourceType: 'other',
    actorHandle: ACTOR_HANDLE,
    defaultTags: ['imported'],
    dryRun: DRY_RUN,
    force: false,
    items
  };
  const res = await fetch(`${API_BASE_URL}/api/v1/admin/import/questions`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-admin-token': ADMIN_TOKEN
    },
    body: JSON.stringify(body)
  });
  const text = await res.text();
  let json: unknown = text;
  try {
    json = JSON.parse(text);
  } catch {
    // keep text
  }
  if (!res.ok) {
    throw new Error(`Import failed: ${res.status} ${typeof json === 'string' ? json : JSON.stringify(json)}`);
  }
  return json as {
    ok: boolean;
    dryRun: boolean;
    created: number;
    skipped: number;
    results: Array<Record<string, unknown>>;
  };
}

async function main() {
  const selected: ImportItem[] = [];

  for (const repo of GITHUB_REPOS) {
    const rows = await fetchGitHubIssues(repo, GITHUB_PER_REPO, 'github', 2);
    selected.push(...rows);
  }
  for (const repo of DISCORD_REPOS) {
    const rows = await fetchGitHubIssues(repo, DISCORD_PER_REPO, 'discord', 2);
    selected.push(...rows);
  }
  for (const site of DISCOURSE_SITES) {
    const rows = await fetchDiscourseTopics(site, DISCOURSE_PER_SITE);
    selected.push(...rows);
  }
  for (const tag of STACKOVERFLOW_TAGS) {
    const rows = await fetchStackOverflowNoAnswers(tag, STACKOVERFLOW_PER_TAG);
    selected.push(...rows);
  }

  const deduped = Array.from(
    new Map(selected.map((item) => [`${item.sourceType}:${item.externalId.toLowerCase()}`, item])).values()
  );
  const normalized = deduped
    .map((item) => ({
      ...item,
      title: item.title.trim().replace(/\s+/g, ' '),
      bodyMd: item.bodyMd.trim(),
      tags: normalizeImportTags(item.tags)
    }))
    .filter((item) =>
      item.title.length >= 8
      && item.bodyMd.length >= 3
      && item.url.trim().length > 0
      && item.externalId.trim().length > 0
    );
  const capped = normalized.slice(0, Math.max(1, MAX_TOTAL));

  if (capped.length === 0) {
    console.log('No items selected for import.');
    return;
  }

  const result = await postImport(capped);
  const bySource = capped.reduce<Record<string, number>>((acc, item) => {
    acc[item.sourceType] = (acc[item.sourceType] ?? 0) + 1;
    return acc;
  }, {});

  console.log(JSON.stringify({
    apiBaseUrl: API_BASE_URL,
    dryRun: DRY_RUN,
    selected: capped.length,
    selectedBySource: bySource,
    importedCreated: result.created,
    importedSkipped: result.skipped
  }, null, 2));
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
