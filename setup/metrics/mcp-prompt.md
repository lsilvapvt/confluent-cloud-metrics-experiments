# Connector Lag via the Confluent MCP Server (no script)

If you have the **Confluent MCP server** configured in your LLM
(see the repo [`.mcp.json`](../../.mcp.json)), you can get the average message lag per task, production rate, and an estimated time lag just by asking, with no code to run.

This works because the MCP server exposes the tools that an LLM needs:
`list_connectors` / `get_connector_config` (topic, task count), `query_metrics`
(`consumer_lag_offsets` and `received_records`), and `get_connector_offsets`.


## Prerequisites

- The Confluent MCP server is connected in your IDE/LLM - [documentation](https://docs.confluent.io/cloud/current/ai/ai-tools/managed-mcp-server.html)

- You know your **environment ID**, **Kafka cluster ID**, and the **connector name** (or its ID) you want to inspect.

## Parameterized prompt

Copy the prompt below, replace the parameter values, and paste it into your LLM/IDE of choice configured with the Confluent MCP server. 


```text
Using the Confluent MCP server, analyze the consumer lag for my HttpSinkV2
connector and estimate how far behind it is in time.

Parameters:
- Environment ID:   env-xxxxxx
- Kafka cluster ID: lkc-xxxxxx
- Connector name:   my-httpsink-v2
- Topic:            my-topic-name
- Time window:      PT24H/now
- Granularity:      PT5M

Do the following and show your work:
1. Read the connector config to get its topic(s) and task count (tasks.max).
2. Derive the consumer group id as "connect-<connector-id>" (the connector ID
   is the lcc-... id; get it from the connector list/config if needed).
3. Query metric io.confluent.kafka.server/consumer_lag_offsets for that cluster
   and consumer group over the window/granularity, grouped by partition. Sum
   across partitions per timestamp to get total lag, and use the number of
   distinct partitions as the partition count.
4. Query metric io.confluent.kafka.server/received_records for the topic over
   the same window/granularity to get the production rate.
5. Compute and report:
   - average message lag per task = total lag / task count
   - produced records/sec/partition =
       (received_records / granularity_in_seconds) / partition count
   - estimated time lag (seconds, shown human-readable) =
       (avg message lag per task) / (produced records/sec/partition)
6. Present a short summary table of those three numbers, plus the lag and
   production-rate trend over the window (min/avg/max per series). Note any
   caveats (e.g. a large backlog inflating the time-lag estimate, or the
   Metrics API's short ingestion delay).
```

## Notes

- **What's the same / different vs. the script:** the math is identical
  (`lag/task`, `produced/sec/partition`, `time lag = lag-per-task ÷
  produced/sec/partition`). The MCP route is conversational and great for
  ad-hoc questions; the script is better for repeatable collection, CSV output,
  and the [HTML dashboard](README.md#dashboard---output-html).

- **Auth:** the MCP server uses the credentials configured in `.mcp.json`
  (a Confluent Cloud key). You do **not** need the separate
  `CONFLUENT_CLOUD_API_KEY`/`SECRET` env vars that the script requires.

- **Follow-ups you can just ask:** "break the lag down by partition",
  "chart the last 24h at 1-hour granularity", "compare this connector's lag to
  <other connector>", or "alert me if avg lag per task exceeds N".

---

## Notes on Granularity

Allowed granularity values

| Value  | Bucket size                      | Good for                                          |
| ------ | -------------------------------- | ------------------------------------------------- |
| `PT1M` | 1 minute                         | Fine-grained. May be truncated by the API though, not advised.  |
| `PT5M` | 5 minutes                        | Default here — nice balance for an hour or a few hours |
| `PT15M`| 15 minutes                       | A half-day to a day                               |
| `PT30M`| 30 minutes                       | A day or so                                       |
| `PT1H` | 1 hour                           | Multi-day trends                                  |
| `PT4H` | 4 hours                          | A week-ish                                        |
| `PT6H` | 6 hours                          | A week+                                           |
| `PT12H`| 12 hours                         | Weeks                                             |
| `P1D`  | 1 day                            | Long-range trend (note: `P1D`, not `PT1D`)        |
| `ALL`  | the whole interval as one bucket | A single aggregate number over the window         |

### How to choose it

The granularity must be paired sensibly with your interval because of three constraints:

- Point budget — interval ÷ granularity is the number of buckets returned, and the API caps results (the script also defaults --limit 1000). So PT1H interval at PT1M = 60 points (fine); P7D at PT1M = 10,080 points (too many — use PT1H or coarser).
- Retention by granularity — fine granularities can't look back as far. Roughly: PT1M data is retained only for a recent window (hours/a few days), while coarser buckets (PT1H, P1D) are available much further back. If you query a long-ago window at PT1M you'll get empty results.
- Resolution vs. noise — finer buckets show short-lived spikes but are noisier; coarser buckets smooth the trend. For the time-lag estimate, coarser granularity averages out per-bucket jitter in the production rate (a steadier denominator).

Rules of thumb for this prompt:

- Last 1–6 hours → PT5M
- Last 1–2 days → PT15M or PT1H
- A week or more → PT1H or P1D
- "Just give me one number for the window" → ALL

---

## Notes on Time Window

The **Time window** parameter maps to the Metrics API `interval` — an ISO 8601
time interval written as `start/end`. Either side can be an absolute UTC
timestamp (ending in `Z`) or an ISO 8601 duration, and the keyword `now` is
allowed as the end. The durations use the same tokens as granularity
(`PT1H` = 1 hour, `PT30M` = 30 min, `P1D` = 1 day, `P7D` = 7 days, combinable
like `P1DT12H`).

Allowed forms:

| Form                  | Example                                     | Meaning                       |
| --------------------- | ------------------------------------------- | ----------------------------- |
| `duration/now`        | `PT6H/now`                                   | Last 6 hours up to now        |
| `duration/timestamp`  | `PT6H/2026-06-22T21:00:00Z`                  | 6 hours ending at that time   |
| `timestamp/timestamp` | `2026-06-22T00:00:00Z/2026-06-22T12:00:00Z`  | Explicit start → end          |
| `timestamp/duration`  | `2026-06-22T00:00:00Z/PT6H`                  | 6 hours starting at that time |

Common windows:

| Window         | Value                                       |
| -------------- | ------------------------------------------- |
| Last hour      | `PT1H/now`                                   |
| Last 6 hours   | `PT6H/now`                                   |
| Last 24 hours  | `P1D/now` (or `PT24H/now`)                    |
| Last 7 days    | `P7D/now`                                     |
| A specific day | `2026-06-21T00:00:00Z/2026-06-22T00:00:00Z`   |

### How to choose it

The window interacts with `granularity` and with data retention:

- **Pair it with granularity (point budget)** — the number of buckets is
  `interval ÷ granularity`, and the API caps results. Keep it reasonable:
  `PT1H/now` at `PT1M` = 60 points; `P7D/now` at `PT1M` would be ~10,080 (too
  many) → use `PT1H` or coarser for week-long windows.
- **Retention / lookback** — queryable history is limited (fine-grained data
  for a recent window, coarser data further back). If the window's start is
  older than what's retained for your chosen granularity, you'll get empty
  results — widen the granularity for older ranges.
- **`now` and partial buckets** — `now` is the easiest end; absolute timestamps
  must be UTC (`...Z`). The most recent bucket may be partial or briefly empty
  due to the Metrics API's short ingestion delay (seen as a low/zero latest
  point in these analyses).

Rule of thumb: state the window relative to `now` (`PT1H/now`, `PT6H/now`,
`P1D/now`) for live monitoring, and use absolute `timestamp/timestamp` only
when you need to inspect a specific past incident.

---