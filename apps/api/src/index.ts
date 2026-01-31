import 'dotenv/config';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { PrismaClient } from '@prisma/client';
import { z } from 'zod';
import crypto from 'crypto';

const prisma = new PrismaClient();
const fastify = Fastify({ logger: true });

const PORT = Number(process.env.PORT ?? 3000);
const ADMIN_TOKEN = process.env.ADMIN_TOKEN ?? '';
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL ?? '';

await fastify.register(cors, { origin: true });

await fastify.register(swagger, {
  mode: 'dynamic',
  openapi: {
    info: {
      title: 'A2ABench API',
      description: 'Agent-native developer Q&A service',
      version: '0.1.0'
    },
    components: {
      securitySchemes: {
        AdminToken: {
          type: 'apiKey',
          in: 'header',
          name: 'X-Admin-Token'
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
  headers: Record<string, string | string[] | undefined>;
};

function markdownToText(markdown: string) {
  return markdown
    .replace(/```[\s\S]*?```/g, '')
    .replace(/`[^`]+`/g, '')
    .replace(/!\[[^\]]*\]\([^)]*\)/g, '')
    .replace(/\[(.*?)\]\([^)]*\)/g, '$1')
    .replace(/[#>*_~]/g, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
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

function stripQuery(value: string) {
  const index = value.indexOf('?');
  return index === -1 ? value : value.slice(0, index);
}

function resolveRoute(request: RouteRequest) {
  return (
    request.routerPath ??
    request.routeOptions?.url ??
    stripQuery(request.raw.url ?? request.url)
  );
}

function extractApiKeyPrefix(headers: Record<string, string | string[] | undefined>) {
  const auth = normalizeHeader(headers.authorization);
  if (!auth) return null;
  const [scheme, ...rest] = auth.split(' ');
  if (!scheme || scheme.toLowerCase() !== 'bearer') return null;
  const token = rest.join(' ').trim();
  if (!token) return null;
  return token.slice(0, 8);
}

function agentCard(baseUrl: string) {
  return {
    name: 'A2ABench',
    description: 'Agent-native developer Q&A with REST + MCP + A2A discovery.',
    url: baseUrl,
    version: '0.1.0',
    protocolVersion: '0.1',
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
      }
    ],
    auth: {
      type: 'apiKey',
      description: 'Bearer API key for write endpoints. X-Admin-Token for admin endpoints.'
    }
  };
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

async function requireApiKey(request: { headers: Record<string, string | string[] | undefined> }, reply: any, scope?: string) {
  const header = request.headers.authorization;
  const auth = Array.isArray(header) ? header[0] : header;
  if (!auth) {
    reply.code(401).send({ error: 'Missing API key' });
    return null;
  }
  const [scheme, ...rest] = auth.split(' ');
  if (!scheme || scheme.toLowerCase() !== 'bearer' || rest.length === 0) {
    reply.code(401).send({ error: 'Missing API key' });
    return null;
  }
  const key = rest.join(' ').trim();
  if (!key) {
    reply.code(401).send({ error: 'Invalid API key' });
    return null;
  }
  const keyPrefix = key.slice(0, 8);
  const keyHash = sha256(key);
  const apiKey = await prisma.apiKey.findFirst({
    where: { keyPrefix, keyHash, revokedAt: null },
    include: { user: true }
  });
  if (!apiKey) {
    reply.code(401).send({ error: 'Invalid API key' });
    return null;
  }
  if (scope && apiKey.scopes.length > 0 && !apiKey.scopes.includes(scope)) {
    reply.code(403).send({ error: 'Insufficient scope' });
    return null;
  }
  return apiKey;
}

fastify.addHook('onRequest', async (request) => {
  (request as { startTimeNs?: bigint }).startTimeNs = process.hrtime.bigint();
});

fastify.addHook('onResponse', async (request, reply) => {
  if (request.method === 'OPTIONS') return;
  const startNs = (request as { startTimeNs?: bigint }).startTimeNs;
  const durationMs = startNs ? Math.max(0, Number(process.hrtime.bigint() - startNs) / 1_000_000) : 0;
  const route = resolveRoute(request as RouteRequest);
  const apiKeyPrefix = extractApiKeyPrefix(request.headers);
  const userAgent = normalizeHeader(request.headers['user-agent']).slice(0, 256) || null;

  void prisma.usageEvent.create({
    data: {
      method: request.method,
      route,
      status: reply.statusCode,
      durationMs: Math.round(durationMs),
      apiKeyPrefix,
      userAgent
    }
  }).catch((err) => {
    request.log.warn({ err }, 'usage event logging failed');
  });
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

fastify.get('/api/v1/usage/summary', {
  schema: {
    tags: ['usage', 'admin'],
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
  const days = Math.min(90, Math.max(1, Number(query.days ?? 7)));
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  const last24h = new Date(Date.now() - 24 * 60 * 60 * 1000);

  const [total, lastDay, byRoute, byStatus, dailyRows] = await Promise.all([
    prisma.usageEvent.count({ where: { createdAt: { gte: since } } }),
    prisma.usageEvent.count({ where: { createdAt: { gte: last24h } } }),
    prisma.usageEvent.groupBy({
      by: ['route'],
      where: { createdAt: { gte: since } },
      _count: { route: true },
      orderBy: { _count: { route: 'desc' } },
      take: 10
    }),
    prisma.usageEvent.groupBy({
      by: ['status'],
      where: { createdAt: { gte: since } },
      _count: { status: true },
      orderBy: { _count: { status: 'desc' } }
    }),
    prisma.$queryRaw<Array<{ day: Date | string; count: bigint | number | string }>>`
      SELECT date_trunc('day', "createdAt") AS day, COUNT(*) AS count
      FROM "UsageEvent"
      WHERE "createdAt" >= ${since}
      GROUP BY 1
      ORDER BY day ASC
    `
  ]);

  return {
    days,
    since: since.toISOString(),
    total,
    last24h: lastDay,
    byRoute: byRoute.map((row) => ({ route: row.route, count: row._count.route })),
    byStatus: byStatus.map((row) => ({ status: row.status, count: row._count.status })),
    daily: dailyRows.map((row) => {
      const date = row.day instanceof Date ? row.day : new Date(row.day);
      return {
        day: date.toISOString().slice(0, 10),
        count: Number(row.count)
      };
    })
  };
});

fastify.get('/api/v1/search', {
  schema: {
    tags: ['search'],
    querystring: {
      type: 'object',
      properties: {
        q: { type: 'string' },
        tag: { type: 'string' },
        page: { type: 'integer', minimum: 1 }
      }
    }
  }
}, async (request) => {
  const query = request.query as { q?: string; tag?: string; page?: number };
  const page = Math.max(1, Number(query.page ?? 1));
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
    take,
    skip,
    orderBy: { createdAt: 'desc' },
    include: {
      tags: { include: { tag: true } },
      _count: { select: { answers: true } }
    }
  });

  return {
    page,
    results: items.map((item) => ({
      id: item.id,
      title: item.title,
      bodyText: item.bodyText,
      createdAt: item.createdAt,
      updatedAt: item.updatedAt,
      tags: item.tags.map((link) => link.tag.name),
      answerCount: item._count.answers
    }))
  };
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
      _count: { select: { answers: true } }
    }
  });
  return items.map((item) => ({
    id: item.id,
    title: item.title,
    bodyText: item.bodyText,
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
    tags: item.tags.map((link) => link.tag.name),
    answerCount: item._count.answers
  }));
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
  const question = await prisma.question.findUnique({
    where: { id },
    include: {
      tags: { include: { tag: true } },
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
  return {
    id: question.id,
    title: question.title,
    bodyMd: question.bodyMd,
    bodyText: question.bodyText,
    createdAt: question.createdAt,
    updatedAt: question.updatedAt,
    user: { id: question.user.id, handle: question.user.handle },
    tags: question.tags.map((link) => link.tag.name),
    answers: question.answers.map((answer) => ({
      id: answer.id,
      bodyMd: answer.bodyMd,
      bodyText: answer.bodyText,
      createdAt: answer.createdAt,
      updatedAt: answer.updatedAt,
      user: { id: answer.user.id, handle: answer.user.handle }
    }))
  };
});

fastify.post('/api/v1/questions', {
  schema: {
    tags: ['questions'],
    body: {
      type: 'object',
      required: ['title', 'bodyMd'],
      properties: {
        title: { type: 'string' },
        bodyMd: { type: 'string' },
        tags: { type: 'array', items: { type: 'string' } }
      }
    }
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:question');
  if (!apiKey) return;

  const body = parse(
    z.object({
      title: z.string().min(3),
      bodyMd: z.string().min(3),
      tags: z.array(z.string().min(1)).optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const bodyText = markdownToText(body.bodyMd);
  const tags = body.tags?.map((tag) => tag.trim()).filter(Boolean);

  const question = await prisma.question.create({
    data: {
      title: body.title,
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

  reply.code(201).send({
    id: question.id,
    title: question.title,
    bodyMd: question.bodyMd,
    bodyText: question.bodyText,
    tags: question.tags.map((link) => link.tag.name),
    createdAt: question.createdAt,
    updatedAt: question.updatedAt
  });
});

fastify.post('/api/v1/questions/:id/answers', {
  schema: {
    tags: ['answers'],
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
  }
}, async (request, reply) => {
  const apiKey = await requireApiKey(request, reply, 'write:answer');
  if (!apiKey) return;

  const { id } = request.params as { id: string };
  const body = parse(
    z.object({
      bodyMd: z.string().min(3)
    }),
    request.body,
    reply
  );
  if (!body) return;

  const question = await prisma.question.findUnique({ where: { id } });
  if (!question) {
    reply.code(404).send({ error: 'Question not found' });
    return;
  }

  const bodyText = markdownToText(body.bodyMd);
  const answer = await prisma.answer.create({
    data: {
      questionId: id,
      userId: apiKey.userId,
      bodyMd: body.bodyMd,
      bodyText
    }
  });

  reply.code(201).send({
    id: answer.id,
    bodyMd: answer.bodyMd,
    bodyText: answer.bodyText,
    createdAt: answer.createdAt,
    updatedAt: answer.updatedAt
  });
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
        scopes: { type: 'array', items: { type: 'string' } }
      }
    }
  }
}, async (request, reply) => {
  if (!(await requireAdmin(request, reply))) return;

  const body = parse(
    z.object({
      userId: z.string(),
      name: z.string().min(2),
      scopes: z.array(z.string()).optional()
    }),
    request.body,
    reply
  );
  if (!body) return;

  const key = `a2a_${crypto.randomBytes(24).toString('hex')}`;
  const keyPrefix = key.slice(0, 8);
  const keyHash = sha256(key);
  const scopes = body.scopes?.length ? body.scopes : ['write:question', 'write:answer'];

  const apiKey = await prisma.apiKey.create({
    data: {
      userId: body.userId,
      name: body.name,
      keyPrefix,
      keyHash,
      scopes
    }
  });

  reply.code(201).send({
    id: apiKey.id,
    userId: apiKey.userId,
    name: apiKey.name,
    scopes: apiKey.scopes,
    keyPrefix: apiKey.keyPrefix,
    apiKey: key
  });
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
  const question = await prisma.question.findUnique({
    where: { id },
    include: {
      answers: { include: { user: true }, orderBy: { createdAt: 'asc' } },
      user: true
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
  lines.push(question.bodyText);
  lines.push('');
  lines.push('Answers:');
  if (question.answers.length === 0) {
    lines.push('No answers yet.');
  } else {
    question.answers.forEach((answer, index) => {
      lines.push('');
      lines.push(`${index + 1}. ${answer.user.handle} (${answer.createdAt.toISOString()})`);
      lines.push(answer.bodyText);
    });
  }
  reply.type('text/plain').send(lines.join('\n'));
});

fastify.addHook('onClose', async () => {
  await prisma.$disconnect();
});

fastify.listen({ port: PORT, host: '0.0.0.0' }).catch((err) => {
  fastify.log.error(err);
  process.exit(1);
});
