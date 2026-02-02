import { PrismaClient } from '@prisma/client';
import { seedContent } from '../src/seedData.js';

const prisma = new PrismaClient();

async function main() {
  await seedContent(prisma);
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
