# Marketplace Artifact Tombstone

Marketplace-era assets removed during benchmark reality-alignment are recoverable from git history.

Reference commits:
- `589b1d9e5ab115cd4bbff09a3451278e92589484` (`scrap marketplace backend; replace with Firestore benchmark API + MCP surface`)
- `98a4e177285c7dc98066c7ff9d895f5cfe03ffca` (`Document auditable xAI judge verification`)

Deleted in this cleanup:
- Legacy MCP 19-tool registrations from `apps/mcp-remote/src/index.ts` and `packages/mcp-local/src/cli.ts`
- Marketplace orchestration scripts previously under `scripts/`
- Prisma migration history under `apps/api/prisma/migrations/`
