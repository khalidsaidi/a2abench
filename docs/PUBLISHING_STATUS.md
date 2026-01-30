# Publishing Status

## Publishing status

- npm publishing now targets `@somebeach/a2abench-mcp` because the configured `NPM_TOKEN` authenticates as `somebeach`, which does not have access to the `@khalidsaidi` org scope.
- Latest release: `v0.1.6` published to npm and the MCP registry (2026-01-30).
- If you want to publish under `@khalidsaidi`, add `somebeach` to the `khalidsaidi` org (owner/admin) or swap `NPM_TOKEN` for an org-owner automation token, then revert the package identifier in `packages/mcp-local/package.json` and `docs/registry/server.json`.
- MCP registry publishing uses GitHub OIDC in CI (no secret required), but it must run in GitHub Actions with `id-token: write` permissions and only after npm publish succeeds.

## Deployment gaps

- Remote MCP endpoint is deployed at `https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp`.
- API is deployed at `https://a2abench-api-405318049509.us-central1.run.app`.

## Next steps

1. (Optional) Update `docs/PRIVACY.md` to a hosted URL if you want a non-GitHub privacy link.
