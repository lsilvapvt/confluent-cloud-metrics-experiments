---
name: connector-lag
description: >
  Analyze consumer lag and estimate the time lag for a Confluent Cloud sink
  connector (e.g. HttpSinkV2) using the Confluent MCP server. Use when the user
  asks to check a connector's consumer lag, lag per task, production rate, or
  "how far behind" a connector is. Accepts parameters: connector, env, cluster,
  topic, window, granularity, tasks, partitions.
allowed-tools:
  - mcp__confluent-mcp-global__get_connector_config
  - mcp__confluent-mcp-global__list_connectors
  - mcp__confluent-mcp-global__query_metrics
  - mcp__confluent-mcp-global__get_connector_offsets
---

# Connector Lag Analysis

Analyze a Confluent Cloud sink connector's consumer lag via the Confluent MCP
server and estimate how far behind it is in time. This is the skill version of
`setup/metrics/mcp-prompt.md`.

## Parameters

The invocation arguments are free-form `key=value` pairs. Parse them from the
text the skill was invoked with. Apply these rules and defaults:

| Parameter     | Required | Default                                                |
| ------------- | -------- | ------------------------------------------------------ |
| `connector`   | yes      | —                                                      |
| `env`         | yes      | — (environment ID, e.g. `env-xxxxxx`)                  |
| `cluster`     | yes      | — (Kafka cluster ID, e.g. `lkc-xxxxxx`)                |
| `topic`       | no       | the connector's configured topic (read from config)    |
| `window`      | no       | `PT6H/now` (ISO 8601 Metrics API interval)             |
| `granularity` | no       | `PT5M` (ISO 8601 bucket size)                          |
| `tasks`       | no       | the connector's `tasks.max` (read from config)         |
| `partitions`  | no       | count of distinct partitions seen in the lag metric    |

If any **required** parameter is missing, ask the user for it before querying
(or, if only `connector` + `env` + `cluster` are given, proceed — everything
else has a default).

**Validate the point budget:** if `window ÷ granularity` exceeds ~1000 buckets,
warn and suggest a coarser granularity instead of running the query. (See the
granularity/time-window notes in `setup/metrics/mcp-prompt.md`.)

## Steps (show your work)

1. Call `get_connector_config` for `connector` in `env`/`cluster`. Extract its
   topic(s) and `tasks.max`. If `topic`/`tasks` were not passed, use these.
2. Determine the connector ID (the `lcc-...` id; from the config/`list_connectors`)
   and derive the consumer group as `connect-<connector-id>`.
3. `query_metrics` for `io.confluent.kafka.server/consumer_lag_offsets` on
   `cluster`, filtered to that consumer group, grouped by `metric.partition`,
   over `window`/`granularity`. Sum across partitions per timestamp = total lag.
   Distinct partition count = partition count (unless `partitions` was passed).
4. `query_metrics` for `io.confluent.kafka.server/received_records` on `cluster`,
   filtered to `topic`, over the same `window`/`granularity` = production rate.
5. Compute:
   - `avg message lag per task = total lag / tasks`
   - `produced records/sec/partition = (received_records / granularity_seconds) / partitions`
   - `estimated time lag (s) = (avg message lag per task) / (produced/sec/partition)`
     — show human-readable (s/min/h/d).

## Output

Present:
- A one-line **status** (caught up / behind, with the estimated time lag).
- A **summary table**: avg lag/task, produced/sec/partition, estimated time lag.
- A **trend table**: min/avg/max for total lag, lag per task, and production
  rate (rec/s) over the window.
- **Caveats**: large backlog inflating the estimate; partial/empty latest bucket
  from Metrics API ingestion delay; lag history limited if the consumer group is
  new (e.g. after a connector recreate).

## Example invocations

```
/connector-lag connector=ldasilva-httpsink-v2-demo env=env-kwyxd6 cluster=lkc-jk7kdp
/connector-lag connector=my-sink env=env-abc123 cluster=lkc-def456 window=PT24H/now granularity=PT15M
/connector-lag connector=my-sink env=env-abc123 cluster=lkc-def456 topic=orders tasks=6 partitions=12
```
