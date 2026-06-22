#!/usr/bin/env python3
"""Collect consumer_lag_offsets for the HttpSinkV2 demo connector.

Queries the Confluent Cloud Metrics (Telemetry) API for the metric
``io.confluent.kafka.server/consumer_lag_offsets`` and prints the lag for a
given consumer group, optionally broken down by topic/partition.

Auth uses a Confluent Cloud API key (basic auth) read from the environment:

    export CONFLUENT_CLOUD_API_KEY=...
    export CONFLUENT_CLOUD_API_SECRET=...

NOTE: this must be a *Cloud* API key (``confluent api-key create --resource
cloud``), NOT the Kafka cluster key used by the connector.

Examples:
    # Last hour, 1-minute granularity, total lag for the connector group
    python consumer_lag.py

    # Last 6 hours, broken down by topic, as CSV
    python consumer_lag.py --interval PT6H/now --group-by topic --output csv
"""

import argparse
import base64
import csv
import json
import os
import re
import sys
import urllib.error
import urllib.request
import webbrowser

METRICS_URL = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
METRIC = "io.confluent.kafka.server/consumer_lag_offsets"
METRIC_PRODUCED = "io.confluent.kafka.server/received_records"

# Demo defaults (override on the command line).
DEFAULT_CLUSTER = "lkc-xxxxxx"
DEFAULT_GROUP = "connect-lcc-xxxxxxx"
DEFAULT_TOPIC = "mytopic"
DEFAULT_TASKS = 3


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cluster", default=DEFAULT_CLUSTER,
                   help=f"Kafka cluster ID (default: {DEFAULT_CLUSTER})")
    p.add_argument("--consumer-group", default=DEFAULT_GROUP,
                   help=f"Consumer group ID (default: {DEFAULT_GROUP})")
    p.add_argument("--interval", default="PT1H/now",
                   help="ISO 8601 interval (default: PT1H/now)")
    p.add_argument("--granularity", default="PT1M",
                   help="Bucket size, e.g. PT1M, PT5M, PT1H (default: PT1M)")
    p.add_argument("--group-by", choices=["none", "topic", "partition"],
                   default="none",
                   help="Extra breakdown dimension (default: none). "
                        "Used by table/csv/json output only.")
    p.add_argument("--topic", default=DEFAULT_TOPIC,
                   help=f"Topic for the production-rate metric "
                        f"(default: {DEFAULT_TOPIC})")
    p.add_argument("--tasks", type=int, default=DEFAULT_TASKS,
                   help=f"Number of connector tasks (default: {DEFAULT_TASKS})")
    p.add_argument("--partitions", type=int, default=0,
                   help="Topic partition count. 0 = auto-detect from the lag "
                        "metric (default: 0)")
    p.add_argument("--output", choices=["table", "csv", "json", "html"],
                   default="table", help="Output format (default: table)")
    p.add_argument("--out-file", default="consumer_lag.html",
                   help="HTML file to write when --output html "
                        "(default: consumer_lag.html)")
    p.add_argument("--no-open", action="store_true",
                   help="With --output html, don't auto-open the browser.")
    p.add_argument("--limit", type=int, default=1000,
                   help="Max rows to return (default: 1000)")
    return p.parse_args()


def build_query(args):
    group_by = ["metric.consumer_group_id"]
    if args.group_by == "topic":
        group_by.append("metric.topic")
    elif args.group_by == "partition":
        group_by += ["metric.topic", "metric.partition"]

    return {
        "aggregations": [{"metric": METRIC, "agg": "SUM"}],
        "filter": {
            "op": "AND",
            "filters": [
                {"field": "resource.kafka.id", "op": "EQ", "value": args.cluster},
                {"field": "metric.consumer_group_id", "op": "EQ",
                 "value": args.consumer_group},
            ],
        },
        "granularity": args.granularity,
        "group_by": group_by,
        "intervals": [args.interval],
        "limit": args.limit,
    }


def get_credentials():
    key = os.environ.get("CONFLUENT_CLOUD_API_KEY")
    secret = os.environ.get("CONFLUENT_CLOUD_API_SECRET")
    if not key or not secret:
        sys.exit("ERROR: set CONFLUENT_CLOUD_API_KEY and "
                 "CONFLUENT_CLOUD_API_SECRET (a Cloud-scoped API key).")
    token = base64.b64encode(f"{key}:{secret}".encode()).decode()
    return token


def query_metrics(query, token):
    body = json.dumps(query).encode()
    req = urllib.request.Request(
        METRICS_URL, data=body, method="POST",
        headers={
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")
        sys.exit(f"ERROR: Metrics API returned {e.code} {e.reason}\n{detail}")
    except urllib.error.URLError as e:
        sys.exit(f"ERROR: could not reach Metrics API: {e.reason}")


def render(rows, args):
    if not rows:
        print("No data points returned for the given interval/filters.")
        return

    # Stable column ordering: dimensions first, then timestamp + value.
    dim_keys = [k for k in rows[0] if k.startswith("metric.")]
    columns = dim_keys + ["timestamp", "value"]

    if args.output == "json":
        print(json.dumps(rows, indent=2))
        return

    if args.output == "csv":
        w = csv.DictWriter(sys.stdout, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
        return

    # table
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in columns}
    line = "  ".join(c.ljust(widths[c]) for c in columns)
    print(line)
    print("  ".join("-" * widths[c] for c in columns))
    for r in sorted(rows, key=lambda x: x.get("timestamp", "")):
        print("  ".join(str(r.get(c, "")).ljust(widths[c]) for c in columns))


# --------------------------------------------------------------------------
# Dashboard (--output html)
#
# Builds two time series and a summary:
#   * avg message lag per task   = total consumer lag / # tasks
#   * production rate            = received_records / granularity (records/sec)
# and estimates the time lag:
#   time lag (s) = (lag per task) / (produced records/sec per partition)
# --------------------------------------------------------------------------

def duration_seconds(iso):
    """Convert an ISO 8601 duration like PT1M / PT5M / PT1H30M to seconds."""
    m = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m:
        sys.exit(f"ERROR: could not parse granularity '{iso}' (expected e.g. PT1M).")
    h, mn, s = (int(x) if x else 0 for x in m.groups())
    total = h * 3600 + mn * 60 + s
    if total == 0:
        sys.exit(f"ERROR: granularity '{iso}' resolves to 0 seconds.")
    return total


def humanize_seconds(secs):
    if secs is None:
        return "n/a"
    secs = float(secs)
    if secs < 60:
        return f"{secs:.1f} s"
    if secs < 3600:
        return f"{secs / 60:.1f} min"
    if secs < 86400:
        return f"{secs / 3600:.1f} h"
    return f"{secs / 86400:.1f} d"


def mean(values):
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def build_query_for(metric, args, group_by, extra_filters):
    filters = [{"field": "resource.kafka.id", "op": "EQ", "value": args.cluster}]
    filters += extra_filters
    return {
        "aggregations": [{"metric": metric, "agg": "SUM"}],
        "filter": {"op": "AND", "filters": filters},
        "granularity": args.granularity,
        "group_by": group_by,
        "intervals": [args.interval],
        "limit": args.limit,
    }


DASHBOARD_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Connector lag dashboard — {group}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }}
  h1 {{ font-size: 1.25rem; margin-bottom: .25rem; }}
  .meta {{ color: #666; font-size: .85rem; margin-bottom: 1.25rem; }}
  .cards {{ display: flex; flex-wrap: wrap; gap: 1rem; margin-bottom: 1.5rem; }}
  .card {{ border: 1px solid #e2e2e2; border-radius: 10px; padding: 1rem 1.25rem;
           min-width: 200px; background: #fafafa; }}
  .card .label {{ color: #666; font-size: .8rem; text-transform: uppercase;
                  letter-spacing: .03em; }}
  .card .value {{ font-size: 1.6rem; font-weight: 600; margin-top: .25rem; }}
  .card .sub {{ color: #888; font-size: .8rem; margin-top: .15rem; }}
  .highlight {{ background: #eef6ff; border-color: #b9d8ff; }}
  canvas {{ margin-bottom: 2rem; }}
  #wrap {{ max-width: 1000px; }}
</style>
</head>
<body>
<div id="wrap">
  <h1>Connector lag dashboard — {group}</h1>
  <div class="meta">cluster {cluster} &middot; topic {topic} &middot;
    tasks {tasks} &middot; partitions {partitions} &middot;
    interval {interval} &middot; granularity {granularity}</div>

  <div class="cards">
    <div class="card">
      <div class="label">Avg message lag / task</div>
      <div class="value">{avg_lag_per_task}</div>
      <div class="sub">total lag / {tasks} tasks</div>
    </div>
    <div class="card">
      <div class="label">Produced / sec / partition</div>
      <div class="value">{avg_prod_per_part}</div>
      <div class="sub">{avg_prod_total}/s across {partitions} partitions</div>
    </div>
    <div class="card highlight">
      <div class="label">Estimated time lag</div>
      <div class="value">{time_lag_human}</div>
      <div class="sub">{time_lag_secs} s = lag/task ÷ produced/s/partition</div>
    </div>
  </div>

  <canvas id="lagChart"></canvas>
  <canvas id="prodChart"></canvas>
</div>
<script>
const LABELS = {labels};
new Chart(document.getElementById('lagChart'), {{
  type: 'line',
  data: {{ labels: LABELS, datasets: [{{
    label: 'Avg message lag per task',
    data: {lag_per_task},
    borderColor: '#d62728', backgroundColor: '#d62728',
    tension: 0.2, spanGaps: true, pointRadius: 2 }}] }},
  options: {{ responsive: true,
    interaction: {{ mode: 'index', intersect: false }},
    scales: {{ y: {{ beginAtZero: true,
      title: {{ display: true, text: 'lag (messages / task)' }} }},
      x: {{ title: {{ display: true, text: 'time (UTC)' }},
            ticks: {{ maxRotation: 60, autoSkip: true }} }} }},
    plugins: {{ title: {{ display: true,
      text: 'Average message lag per task' }} }} }}
}});
new Chart(document.getElementById('prodChart'), {{
  type: 'line',
  data: {{ labels: LABELS, datasets: [{{
    label: 'Produced records / sec (topic)',
    data: {prod_per_sec},
    borderColor: '#1f77b4', backgroundColor: '#1f77b4',
    tension: 0.2, spanGaps: true, pointRadius: 2 }}] }},
  options: {{ responsive: true,
    interaction: {{ mode: 'index', intersect: false }},
    scales: {{ y: {{ beginAtZero: true,
      title: {{ display: true, text: 'records / sec' }} }},
      x: {{ title: {{ display: true, text: 'time (UTC)' }},
            ticks: {{ maxRotation: 60, autoSkip: true }} }} }},
    plugins: {{ title: {{ display: true,
      text: 'Messages produced per second' }} }} }}
}});
</script>
</body>
</html>
"""


def build_dashboard(args, token):
    gran_secs = duration_seconds(args.granularity)

    lag_rows = query_metrics(
        build_query_for(METRIC, args, ["metric.partition"],
                        [{"field": "metric.consumer_group_id", "op": "EQ",
                          "value": args.consumer_group}]), token).get("data", [])
    prod_rows = query_metrics(
        build_query_for(METRIC_PRODUCED, args, [],
                        [{"field": "metric.topic", "op": "EQ",
                          "value": args.topic}]), token).get("data", [])

    if not lag_rows:
        sys.exit("No consumer-lag data returned for the given interval/filters.")

    partitions = args.partitions or len({r["metric.partition"] for r in lag_rows})
    if partitions <= 0:
        sys.exit("Could not determine partition count; pass --partitions.")

    # Sum lag across partitions per timestamp; produced is one value per ts.
    lag_total = {}
    for r in lag_rows:
        lag_total[r["timestamp"]] = lag_total.get(r["timestamp"], 0) + (r.get("value") or 0)
    produced = {r["timestamp"]: (r.get("value") or 0) for r in prod_rows}

    timestamps = sorted(set(lag_total) | set(produced))
    lag_per_task, prod_per_sec, prod_per_sec_part, time_lag = [], [], [], []
    for ts in timestamps:
        lpt = lag_total.get(ts)
        lpt = lpt / args.tasks if lpt is not None else None
        pps = produced.get(ts)
        pps = pps / gran_secs if pps is not None else None
        ppsp = pps / partitions if pps not in (None, 0) else None
        lag_per_task.append(lpt)
        prod_per_sec.append(pps)
        prod_per_sec_part.append(ppsp)
        time_lag.append(lpt / ppsp if (lpt is not None and ppsp) else None)

    avg_lag_per_task = mean(lag_per_task)
    avg_prod_per_sec = mean(prod_per_sec)
    avg_prod_per_part = mean(prod_per_sec_part)
    est_time_lag = (avg_lag_per_task / avg_prod_per_part
                    if avg_lag_per_task is not None and avg_prod_per_part else None)

    html = DASHBOARD_TEMPLATE.format(
        group=args.consumer_group, cluster=args.cluster, topic=args.topic,
        tasks=args.tasks, partitions=partitions,
        interval=args.interval, granularity=args.granularity,
        avg_lag_per_task=f"{avg_lag_per_task:,.0f}" if avg_lag_per_task is not None else "n/a",
        avg_prod_per_part=f"{avg_prod_per_part:,.2f}" if avg_prod_per_part is not None else "n/a",
        avg_prod_total=f"{avg_prod_per_sec:,.2f}" if avg_prod_per_sec is not None else "n/a",
        time_lag_human=humanize_seconds(est_time_lag),
        time_lag_secs=f"{est_time_lag:,.0f}" if est_time_lag is not None else "n/a",
        labels=json.dumps(timestamps),
        lag_per_task=json.dumps([round(v, 2) if v is not None else None for v in lag_per_task]),
        prod_per_sec=json.dumps([round(v, 4) if v is not None else None for v in prod_per_sec]),
    )

    path = os.path.abspath(args.out_file)
    with open(path, "w") as f:
        f.write(html)
    print(f"Wrote dashboard to {path} ({len(timestamps)} points, "
          f"{partitions} partitions, {args.tasks} tasks).")
    print(f"  avg lag/task: {avg_lag_per_task:,.0f}" if avg_lag_per_task is not None else "  avg lag/task: n/a")
    print(f"  est. time lag: {humanize_seconds(est_time_lag)}")
    if not args.no_open:
        webbrowser.open(f"file://{path}")


def main():
    args = parse_args()
    token = get_credentials()
    if args.output == "html":
        build_dashboard(args, token)
        return
    result = query_metrics(build_query(args), token)
    render(result.get("data", []), args)


if __name__ == "__main__":
    main()
