# Set up the `connector-lag` Claude Skill

Instead of pasting the prompt from [`mcp-prompt.md`](mcp-prompt.md) each time,
you can install it as a reusable **Claude Code skill** and invoke it with
parameters: `/connector-lag connector=... env=... cluster=...`.

The skill definition is in this folder: [`connector-lag-SKILL.md`](connector-lag-SKILL.md).

## What a skill is

A Claude Code skill is a folder containing a `SKILL.md` file:

- **YAML frontmatter** — `name`, `description` (what Claude matches on to
  auto-trigger), and optional `allowed-tools` (scopes which tools it may use).
- **Markdown body** — the instructions Claude follows, including how to parse
  invocation arguments and which MCP tools to call.

Skills take **free-form arguments**, so this one defines a `key=value`
convention (`connector=`, `env=`, `cluster=`, `window=`, `granularity=`, …) and
the body tells Claude how to parse them and apply defaults.

## Install it

1. **Choose a scope:**
   - **Project** (shared with the repo / your team): `.claude/skills/`
   - **User** (just you, all projects): `~/.claude/skills/`

2. **Create the skill folder and copy the file in, renaming it to `SKILL.md`.**
   The folder name (`connector-lag`) becomes the skill name:

   ```bash
   # Project scope (run from the repo root)
   mkdir -p .claude/skills/connector-lag
   cp setup/metrics/connector-lag-SKILL.md .claude/skills/connector-lag/SKILL.md
   ```

   ```bash
   # …or user scope
   mkdir -p ~/.claude/skills/connector-lag
   cp setup/metrics/connector-lag-SKILL.md ~/.claude/skills/connector-lag/SKILL.md
   ```

3. **Restart / start a new Claude Code session** so the skill is discovered.

> Note: the file must be named exactly `SKILL.md` inside the
> `connector-lag/` folder. The copy in this folder is suffixed
> (`connector-lag-SKILL.md`) only so it can live alongside the docs — rename it
> on install.

## Prerequisites

- The **Confluent MCP server** is configured in Claude Code (see the repo
  [`.mcp.json`](../../.mcp.json)). The skill calls the MCP tools
  `get_connector_config`, `list_connectors`, `query_metrics`, and
  `get_connector_offsets`.

## Use it

```
/connector-lag connector=ldasilva-httpsink-v2-demo env=env-kwyxd6 cluster=lkc-jk7kdp
/connector-lag connector=my-sink env=env-abc123 cluster=lkc-def456 window=PT24H/now granularity=PT15M
```

Required parameters: `connector`, `env`, `cluster`. Everything else has a
default (topic and task count are read from the connector config; partition
count is inferred from the lag metric; window defaults to `PT6H/now`,
granularity to `PT5M`). You can also just ask in plain language — e.g. "check
the lag for connector X in env Y / cluster Z" — and Claude will match the skill
via its description.

## Customize

Edit the `SKILL.md` body to change defaults, the parameter list, or the output
format. Keep the `name`/`description` frontmatter intact so Claude can still
discover and trigger it.
