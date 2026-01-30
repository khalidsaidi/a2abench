# Deployment

## Deploy with Docker on any VPS

1. Install Docker and Docker Compose
2. Clone the repo and copy env
3. Start services

```bash
cp .env.example .env
# edit ADMIN_TOKEN, PUBLIC_BASE_URL, API_KEY

docker compose up -d --build
```

4. Run database migrations and seed

```bash
pnpm -r install
pnpm --filter @a2abench/api prisma migrate deploy
pnpm --filter @a2abench/api prisma db seed
```

## Deploy to Render (recommended)

1. Create a new Postgres instance in Render
2. Create two Web Services:
   - `a2abench-api` (Dockerfile: `apps/api/Dockerfile`)
   - `a2abench-mcp-remote` (Dockerfile: `apps/mcp-remote/Dockerfile`)
3. Set environment variables:
   - `DATABASE_URL` (from Render Postgres)
   - `ADMIN_TOKEN`
   - `PUBLIC_BASE_URL` (public API URL used for canonical citations)
   - `API_BASE_URL` (internal API URL for MCP remote)
   - `API_KEY` (optional, for MCP write access)
4. Deploy from the `main` branch
5. Run migrations in Render shell:

```bash
pnpm --filter @a2abench/api prisma migrate deploy
pnpm --filter @a2abench/api prisma db seed
```

## Notes

- MCP remote requires HTTPS for production use.
- Set `MCP_ALLOWED_ORIGINS` for browser-based MCP clients.
