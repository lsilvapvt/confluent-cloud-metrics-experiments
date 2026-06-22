# Set up the `connector-lag` GitHub Copilot prompt

Instead of pasting the prompt from [`mcp-prompt.md`](mcp-prompt.md) each time,
GitHub Copilot users can install it as a reusable **prompt file** and invoke it
with parameters in Copilot Chat: `/connector-lag connector=... env=... cluster=...`.

The prompt file is in this repo: [`.github/prompts/connector-lag.prompt.md`](../../.github/prompts/connector-lag.prompt.md).
It's the Copilot counterpart of the Claude Code skill
([`connector-lag-SKILL.md`](connector-lag-SKILL.md) /
[`connector-lag-skill-setup.md`](connector-lag-skill-setup.md)).

## What a prompt file is

A Copilot prompt file is a single `*.prompt.md` Markdown file:

- **YAML frontmatter** — `mode` (`agent` for multi-step tool use, `ask`, or
  `edit`), `description`, optional `model`, and `tools` (which tools / tool sets
  Copilot may use, including MCP servers).
- **Markdown body** — the instructions Copilot follows, including how to parse
  invocation arguments and which MCP tools to call.

Like the Claude skill, it takes **free-form arguments**, so this one defines a
`key=value` convention (`connector=`, `env=`, `cluster=`, `window=`,
`granularity=`, …) and the body tells Copilot how to parse them and apply
defaults.

> Note vs. Claude skills: Copilot prompt files are **user-invoked** (you type
> `/connector-lag` in chat) rather than auto-triggered from a description. Use
> **Agent mode** so Copilot can chain the `get_connector_config` → `query_metrics`
> tool calls; *Ask* mode won't reliably run the multi-step flow.

## Install it

1. **Choose a scope:**
   - **Workspace** (shared with the repo / your team): `.github/prompts/` — this
     is where the file already lives in this repo, so no copy is needed.
   - **User** (just you, all workspaces): your VS Code user prompts folder. In
     VS Code, run **Chat: New Prompt File** and choose the user location, then
     paste the body in.

2. **Enable prompt files** (if not already): VS Code setting
   `chat.promptFiles` must be `true`. Workspace `.github/prompts/*.prompt.md`
   files are then discovered automatically.

3. **Reload the window** (or start a new chat) so Copilot discovers the prompt.

> The file must end in `.prompt.md`. The base name (`connector-lag`) becomes the
> slash command: `/connector-lag`.

## Prerequisites

- The **Confluent MCP server** is configured for Copilot. Add it to
  `.vscode/mcp.json` (workspace) or your VS Code user settings, mirroring the
  credentials used by the repo [`.mcp.json`](../../.mcp.json). Example:

  ```jsonc
  {
    "servers": {
      "confluent": {
        "type": "stdio",
        "command": "npx",
        "args": ["-y", "@confluentinc/mcp-confluent"],
        "env": {
          "CONFLUENT_CLOUD_API_KEY": "${env:CONFLUENT_CLOUD_API_KEY}",
          "CONFLUENT_CLOUD_API_SECRET": "${env:CONFLUENT_CLOUD_API_SECRET}"
        }
      }
    }
  }
  ```

- The `tools` entry in the prompt file's frontmatter (`tools: ['confluent']`)
  must match the **server name** you register above. If you name the server
  something else, update the frontmatter to match. The prompt uses the MCP tools
  `get_connector_config`, `list_connectors`, `query_metrics`, and
  `get_connector_offsets`.

## Use it

In Copilot Chat (Agent mode):

```
/connector-lag connector=ldasilva-httpsink-v2-demo env=env-kwyxd6 cluster=lkc-jk7kdp
/connector-lag connector=my-sink env=env-abc123 cluster=lkc-def456 window=PT24H/now granularity=PT15M
```

Required parameters: `connector`, `env`, `cluster`. Everything else has a
default (topic and task count are read from the connector config; partition
count is inferred from the lag metric; window defaults to `PT6H/now`,
granularity to `PT5M`).

## Customize

Edit the `.prompt.md` body to change defaults, the parameter list, or the output
format. Keep the `mode: agent` and `tools` frontmatter intact so Copilot can run
the multi-step analysis against the Confluent MCP server.
