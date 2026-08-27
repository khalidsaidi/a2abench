import 'dotenv/config';
import { defineConfig } from 'prisma/config';

export default defineConfig({
  schema: 'prisma/schema.prisma',
  migrations: {
    path: 'prisma/migrations',
    seed: 'tsx prisma/seed.ts',
  },
  // Optional: only migrate/introspect commands need a connection URL.
  // `prisma generate` (run in CI) works without DATABASE_URL being set.
  datasource: {
    url: process.env.DATABASE_URL,
  },
});
