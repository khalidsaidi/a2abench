# Publishing Status

## Missing credentials / account setup

- `publish-npm` currently fails with `Scope not found` for `@khalidsaidi` because the configured `NPM_TOKEN` belongs to npm user `somebeach`, who does not have access to the `@khalidsaidi` org scope.
- Fix: add `somebeach` to the `khalidsaidi` npm org (owner/admin) or replace `NPM_TOKEN` with an automation token from an org owner of `@khalidsaidi`.
- MCP registry publishing uses GitHub OIDC in CI (no secret required), but it must run in GitHub Actions with `id-token: write` permissions and only after npm publish succeeds.

## Deployment gaps

- Remote MCP endpoint URL is not yet deployed. Update `docs/CHATGPT_APP_SUBMISSION.md` and `docs/registry/server.json` once HTTPS endpoint exists.

## Next steps

1. Grant the current npm user access to the `@khalidsaidi` org scope (or rotate `NPM_TOKEN` with an org-owner automation token).
2. Re-run `publish-npm` and `publish-mcp-registry` workflows for tag `v0.1.4` (or tag a new patch release).
3. Deploy API and MCP remote to HTTPS.
4. Update `PUBLIC_BASE_URL` and MCP endpoint URLs in docs.
