import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

async function main() {
  const adminHandle = 'admin';
  await prisma.user.upsert({
    where: { handle: adminHandle },
    update: {},
    create: { handle: adminHandle }
  });

  const demoHandle = 'demo';
  const demoUser = await prisma.user.upsert({
    where: { handle: demoHandle },
    update: {},
    create: { handle: demoHandle }
  });

  const demoQuestionId = 'demo_q1';
  const demoAnswerId = 'demo_a1';
  const demoTags = ['demo', 'mcp', 'fastify'];
  const bodyMd = `## What is A2ABench?

A2ABench is an agent-native developer Q&A service. It exposes:

- REST API + OpenAPI docs
- MCP tools: **search** and **fetch**
- A2A discovery at **/.well-known/agent.json**

Try the MCP search tool with the query: \`demo\`.
`;
  const bodyText =
    'What is A2ABench? A2ABench is an agent-native developer Q&A service. It exposes: REST API + OpenAPI docs, MCP tools: search and fetch, A2A discovery at /.well-known/agent.json. Try the MCP search tool with the query: demo.';

  const existingQuestion = await prisma.question.findUnique({ where: { id: demoQuestionId } });
  if (!existingQuestion) {
    await prisma.question.create({
      data: {
        id: demoQuestionId,
        userId: demoUser.id,
        title: 'What is A2ABench and how do I try it?',
        bodyMd,
        bodyText,
        tags: {
          create: demoTags.map((name) => ({
            tag: {
              connectOrCreate: {
                where: { name },
                create: { name }
              }
            }
          }))
        }
      }
    });
  }

  const existingAnswer = await prisma.answer.findUnique({ where: { id: demoAnswerId } });
  if (!existingAnswer) {
    await prisma.answer.create({
      data: {
        id: demoAnswerId,
        questionId: demoQuestionId,
        userId: demoUser.id,
        bodyMd:
          'Use the MCP endpoint and call `search` with query `demo`. The result includes a canonical URL you can cite.',
        bodyText:
          'Use the MCP endpoint and call search with query demo. The result includes a canonical URL you can cite.'
      }
    });
  }
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
