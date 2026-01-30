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

await fastify.register(cors, { origin: true });

await fastify.register(swagger, {
  mode: 'dynamic',
  openapi: {
    info: {
      title: 'A2ABench API',
      description: 'Agent-native developer Q&A service',
      version: '0.1.0'
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

fastify.get('/api/openapi.json', async () => {
  return fastify.swagger();
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
