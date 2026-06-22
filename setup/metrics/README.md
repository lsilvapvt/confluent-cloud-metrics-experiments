# Consumer Lag Metrics

Collect and visualize the HttpSinkV2 demo connector's consumer lag using the
Confluent Cloud Metrics (Telemetry) API. The script [`consumer_lag.py`](consumer_lag.py)
(pure Python standard library — no `pip install`) can:

- **Dump the raw lag series** (`io.confluent.kafka.server/consumer_lag_offsets`)
  as a table, CSV, or JSON.
- **Render an HTML dashboard** that combines lag with the topic's production
  rate (`io.confluent.kafka.server/received_records`) to compute the
  **average message lag per task** and an **estimated time lag**.

Demo target (defaults baked into the script):

| Setting        | Value                        |
| -------------- | ---------------------------- |
| Connector      | `httpsink-v2-demo` (`lcc-xxxxxxx`) |
| Consumer group | `connect-lcc-xxxxxxx` (pattern `connect-<connector-id>`) |
| Cluster        | `lkc-xxxxxx`                 |
| Topic          | `my-topic-name` (3 partitions) |
| Tasks          | `3`                          |

Override any of these with the flags in the [Options](#options) table.

## 1. Create a Cloud API key

The Metrics API authenticates with a **Cloud-scoped** API key — this is
different from the Kafka cluster key the connector uses.

```bash
confluent api-key create --resource cloud --description "metrics-reader"
```

## 2. Export credentials

```bash
export CONFLUENT_CLOUD_API_KEY="<cloud-api-key>"
export CONFLUENT_CLOUD_API_SECRET="<cloud-api-secret>"
```

## 3. Run

```bash
# Last hour, 1-minute buckets, total lag for the connector group (table)
python setup/metrics/consumer_lag.py

# Last 6 hours, broken down by topic
python setup/metrics/consumer_lag.py --interval PT6H/now --group-by topic

# CSV for the last 24h at 1-hour granularity (e.g. pipe to a file)
python setup/metrics/consumer_lag.py \
  --interval PT24H/now --granularity PT1H --output csv > lag.csv
```

### Dashboard (`--output html`)

`--output html` writes a self-contained HTML dashboard rendered with
[Chart.js](https://www.chartjs.org/) (loaded from a CDN) and opens it in your
browser — no packages to install:

```bash
python setup/metrics/consumer_lag.py \
  --interval PT6H/now --granularity PT5M --output html
```

It writes `consumer_lag.html` by default (override `--out-file`; `--no-open`
skips launching the browser). The page contains:

- **Summary cards** at the top:
  - **Avg message lag per task** — total consumer lag ÷ `--tasks`.
  - **Produced / sec / partition** — topic production rate
    (`received_records` ÷ granularity) ÷ partition count.
  - **Estimated time lag** — `(lag per task) ÷ (produced/sec/partition)`,
    shown in human units (s/min/h/d).
- **Chart 1** — average message lag per task over time.
- **Chart 2** — messages produced per second over time.

The time-lag estimate answers "roughly how far behind in *time* is each task?":

```
avg message lag per task   = total consumer lag / tasks
produced/sec/partition     = (received_records / granularity_seconds) / partitions
estimated time lag (s)     = (avg message lag per task) / (produced/sec/partition)
```

It is the number of seconds of production the consumer would need to catch up
on at the current production rate. Note this is dominated by any backlog: a
large lag combined with a low production rate (e.g. the datagen source at a few
records/sec) produces a large time-lag figure that shrinks quickly once the
backlog drains.

Extra flags for the dashboard:

| Flag           | Default          | Description                                       |
| -------------- | ---------------- | ------------------------------------------------- |
| `--topic`      | `mytopic` | Topic used for the production-rate metric.        |
| `--tasks`      | `3`              | Connector task count (lag is divided by this).    |
| `--partitions` | `0` (auto)       | Partition count; `0` auto-detects from the metric.|

> The table/csv/json outputs still emit the raw `consumer_lag_offsets` series
> (and honor `--group-by`); the dashboard math only applies to `--output html`.

### Options

| Flag               | Default              | Description                                   |
| ------------------ | -------------------- | --------------------------------------------- |
| `--cluster`        | `lkc-xxxxxx`         | Kafka cluster ID.                             |
| `--consumer-group` | `connect-lcc-xxxxxxx`| Consumer group to measure.                    |
| `--interval`       | `PT1H/now`           | ISO 8601 interval (e.g. `PT6H/now`).          |
| `--granularity`    | `PT1M`               | Bucket size (`PT1M`, `PT5M`, `PT1H`, …).      |
| `--group-by`       | `none`               | Add a `topic` or `partition` breakdown.       |
| `--output`         | `table`              | `table`, `csv`, `json`, or `html` (dashboard). |
| `--out-file`       | `consumer_lag.html`  | Output path when `--output html`.             |
| `--no-open`        | off                  | With `html`, write the file but don't open it.|
| `--limit`          | `1000`               | Max rows returned.                            |

## Collecting over time

The script is a single point-in-time query. To collect a trend, run it on a
schedule:

- **cron** — e.g. every 5 minutes appending CSV:
  ```bash
  */5 * * * * cd /path/to/repo && \
    python setup/metrics/consumer_lag.py --interval PT5M/now --output csv \
    >> /var/log/connector_lag.csv 2>&1
  ```
- **Claude Code `/loop`** — for interactive monitoring with commentary:
  `/loop 5m python setup/metrics/consumer_lag.py`
- **Claude Code `/schedule`** — to run a remote agent on a cron schedule.

## Notes

- `consumer_lag_offsets` is a **gauge** (current lag), aggregated with `SUM`
  across partitions so each data point is the group's total lag.
- A high value means the connector is behind. In this demo the lag was reset by
  recreating the connector under a new name (new consumer group starting at the
  latest offset), so it should sit near zero while the connector keeps up.
- The Metrics API has a short ingestion delay; the most recent minute or two may
  be missing or revised.
- For ad-hoc spot checks you can skip the script entirely and use the Confluent
  MCP `query_metrics` tool from Claude Code.
