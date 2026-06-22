# Confluent MCP Server Setup

This project ships a shared [`.mcp.json`](.mcp.json) that configures the
`confluent-mcp-global` MCP server for the whole team. To use it, each person
just needs to provide their own Confluent Cloud credentials via an environment
variable — no secrets are stored in source control.

## 1. Get a Confluent Cloud API key

Create an API key and secret in the
[Confluent Cloud Console](https://confluent.cloud) (or with the Confluent CLI).
Keep the key and secret handy.

## 2. Export your credentials

Set the key and secret, then build the base64-encoded value the MCP server
expects:

```bash
export CONFLUENT_API_KEY="<your-api-key>"
export CONFLUENT_API_SECRET="<your-api-secret>"
export CONFLUENT_API_KEY_SECRET_BASE64=$(echo -n "${CONFLUENT_API_KEY}:${CONFLUENT_API_SECRET}" | base64)
```

To make this permanent, add the lines above to your shell profile
(`~/.zshrc`, `~/.bashrc`, etc.) so the variable is available every time you
start Claude Code.

## 3. Start Claude Code

Launch Claude Code from this project directory. On first use it will ask you to
approve the project-scoped `confluent-mcp-global` server — approve it once and
you're ready to go.

> **Note:** Claude Code reads `${CONFLUENT_API_KEY_SECRET_BASE64}` from your
> environment when loading `.mcp.json`, so the credential never lives in the
> repository.
