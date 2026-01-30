# Publishing Status

## Missing credentials / account setup

- `NPM_TOKEN` is required for `publish-npm` workflow to publish `@khalidsaidi/a2abench-mcp`.
- npm publishing currently fails with `Scope not found` for `@khalidsaidi`. Ensure the token belongs to the `khalidsaidi` npm user or an org that owns the `@khalidsaidi` scope (create the org if needed).
- MCP registry publishing uses GitHub OIDC in CI (no secret required), but it must run in GitHub Actions with `id-token: write` permissions and after npm publish succeeds.

## Deployment gaps

- Remote MCP endpoint URL is not yet deployed. Update `docs/CHATGPT_APP_SUBMISSION.md` and `docs/registry/server.json` once HTTPS endpoint exists.

## Next steps

1. Add the missing GitHub secret (`NPM_TOKEN`).
2. Deploy API and MCP remote to HTTPS.
3. Update `PUBLIC_BASE_URL` and MCP endpoint URLs in docs.
