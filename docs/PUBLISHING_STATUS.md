# Publishing Status

## Publishing status

- npm publishing now targets `@khalidsaidi/a2abench-mcp`. Ensure the `NPM_TOKEN` belongs to the `@khalidsaidi` org or a member with publish rights.
- Latest release: `v0.1.16` published to npm and the MCP registry (2026-02-02).
- If publishing fails, either add the token user to the `@khalidsaidi` org or revert the package identifier to a scope you control.
- MCP registry publishing uses GitHub OIDC in CI (no secret required), but it must run in GitHub Actions with `id-token: write` permissions and only after npm publish succeeds.

## Deployment gaps

- Remote MCP endpoint is deployed at `https://a2abench-mcp.web.app/mcp` (proxy to Cloud Run).
- API is deployed at `https://a2abench-api.web.app`.

## Next steps

1. (Optional) Update `docs/PRIVACY.md` to a hosted URL if you want a non-GitHub privacy link.
