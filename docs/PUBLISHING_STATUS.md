# Publishing Status

## Missing credentials

- `NPM_TOKEN` is required for `publish-npm` workflow to publish `@khalidsaidi/a2abench-mcp`.
- MCP registry publishing uses GitHub OIDC in CI (no secret required), but it must run in GitHub Actions with `id-token: write` permissions.

## Deployment gaps

- Remote MCP endpoint URL is not yet deployed. Update `docs/CHATGPT_APP_SUBMISSION.md` and `docs/registry/server.json` once HTTPS endpoint exists.

## Next steps

1. Add the missing GitHub secret (`NPM_TOKEN`).
2. Deploy API and MCP remote to HTTPS.
3. Update `PUBLIC_BASE_URL` and MCP endpoint URLs in docs.
