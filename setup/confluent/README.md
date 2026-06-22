# HttpSinkV2 Connector — Confluent Cloud Setup

This is **step 2** of the demo. It creates the Confluent Cloud **HTTP Sink V2**
connector that reads the `ldasilva-users` topic and POSTs each record to the
AWS endpoint provisioned in [step 1](../aws/README.md).

```
Kafka topic (ldasilva-users, AVRO)
        │  HttpSinkV2 connector  (this step)
        ▼
API Gateway  ──►  Lambda  ──►  S3 bucket   (created in setup/aws)
```

## Prerequisites

- The AWS stack from [`setup/aws`](../aws/README.md) is deployed, and you have
  its **InvokeUrl** and **ApiKeyValue**.
- Confluent CLI v3+ installed and logged in (`confluent login`).
- Access to environment `env-kwyxd6` and cluster `lkc-jk7kdp`.

## Files

- [`httpsink-v2-connector.json`](httpsink-v2-connector.json) — connector config.
- [`openapi-httpsink.yaml`](openapi-httpsink.yaml) — optional OpenAPI spec, only
  needed if you create the connector in OpenAPI-spec mode via the UI.

## 1. Set your context

```bash
confluent login
confluent environment use env-kwyxd6
confluent kafka cluster use lkc-jk7kdp
```

## 2. Create a Kafka API key for the connector

```bash
confluent api-key create --resource lkc-jk7kdp --description "ldasilva-httpsink-v2-demo"
```

Copy the **key** and **secret** into `kafka.api.key` / `kafka.api.secret` in
[`httpsink-v2-connector.json`](httpsink-v2-connector.json).

## 3. Point the config at your endpoint

Edit [`httpsink-v2-connector.json`](httpsink-v2-connector.json) so it matches
the AWS outputs:

| Property                      | Set to                                                        |
| ----------------------------- | ------------------------------------------------------------- |
| `http.api.base.url`           | the InvokeUrl **without** the `/records` path (e.g. `…/demo`) |
| `api1.http.api.path`          | `/records`                                                    |
| `api1.http.request.headers`   | `x-api-key:<your ApiKeyValue>`                                |

## 4. Create the connector

```bash
confluent connect cluster create \
  --config-file setup/confluent/httpsink-v2-connector.json \
  --environment env-kwyxd6 \
  --cluster lkc-jk7kdp
```

Already created it and just changing config? Update it instead (get the
`lcc-…` id from `confluent connect cluster list`):

```bash
confluent connect cluster update <connector-id> \
  --config-file setup/confluent/httpsink-v2-connector.json \
  --environment env-kwyxd6 --cluster lkc-jk7kdp
```

## 5. Verify

```bash
confluent connect cluster list
confluent connect cluster describe <connector-id> --cluster lkc-jk7kdp
```

When it reaches **RUNNING** (with no errors in the logs), records should be
landing in S3:

```bash
aws s3 ls s3://<your-bucket-name>/records/ --recursive --region us-east-2
```

## HTTP Sink V2 config model (gotchas)

V2 differs from V1 — the things that trip people up:

- **Topics are declared twice.** `topics` (top-level, required) is the
  connector's subscription; `api1.topics` routes which of those topics go to
  API slot 1. In a single-API demo they're the same value.
- **Per-API keys are namespaced** with `api{n}.` — e.g. `api1.http.api.path`,
  `api1.request.method`, `api1.request.body.format`, `api1.http.request.headers`.
  The base URL (`http.api.base.url`) and `apis.num` are shared/top-level.
- **Custom headers** go in `api1.http.request.headers` as `name:value`
  (no space after the colon — a leading space becomes part of the value and
  breaks API-key matching). `api.key.location: HEADER` tells the connector to
  place the key in a header.
- **AVRO input** uses the environment's Schema Registry automatically; with
  `api1.request.body.format: json` the connector deserializes each AVRO record
  and sends it as JSON, which is what the Lambda stores.

## Tear down

```bash
confluent connect cluster delete <connector-id> --cluster lkc-jk7kdp
```

Then tear down the AWS side — see [`setup/aws`](../aws/README.md#tear-down).

---

> ⚠️ **Secrets:** [`httpsink-v2-connector.json`](httpsink-v2-connector.json)
> currently contains a live Kafka API secret and the endpoint API key. Replace
> them with placeholders (or inject via environment) before committing this to
> source control.
