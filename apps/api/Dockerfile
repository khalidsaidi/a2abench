FROM node:20-bookworm-slim AS base
WORKDIR /repo
RUN apt-get update -y && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*
RUN corepack enable

FROM base AS deps
COPY package.json pnpm-workspace.yaml tsconfig.base.json ./
COPY apps/api/package.json apps/api/tsconfig.json ./apps/api/
COPY apps/api/prisma ./apps/api/prisma
RUN pnpm install --filter @a2abench/api...

FROM deps AS build
COPY apps/api/src ./apps/api/src
RUN pnpm --filter @a2abench/api prisma generate
RUN pnpm --filter @a2abench/api build

FROM base AS runtime
ENV NODE_ENV=production
COPY --from=build /repo/apps/api/dist /repo/apps/api/dist
COPY --from=build /repo/apps/api/node_modules /repo/apps/api/node_modules
COPY --from=build /repo/node_modules /repo/node_modules
COPY apps/api/package.json /repo/apps/api/package.json
COPY apps/api/prisma /repo/apps/api/prisma
WORKDIR /repo/apps/api
EXPOSE 3000
CMD ["node", "dist/index.js"]
