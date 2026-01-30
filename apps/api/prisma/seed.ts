import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

async function main() {
  const handle = 'admin';
  await prisma.user.upsert({
    where: { handle },
    update: {},
    create: { handle }
  });
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
