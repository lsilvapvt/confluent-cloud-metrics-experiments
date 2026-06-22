# HttpSinkV2 Demo — AWS Backend

CloudFormation that stands up the AWS side of a Confluent **HttpSinkV2** demo:

```
Kafka topic (ldasilva-users)
        │  HttpSinkV2 connector
        ▼
API Gateway  ──►  Lambda  ──►  S3 bucket
   POST /records          (one object per request)
```

Every batch the connector POSTs is written to S3 as a timestamped JSON object
under `records/YYYY/MM/DD/HH/<uuid>.json`.

## Files

- [`httpsink-demo.yaml`](httpsink-demo.yaml) — the CloudFormation template.

## Parameters

| Parameter          | Default                              | Purpose                                          |
| ------------------ | ------------------------------------ | ------------------------------------------------ |
| `ResourcePrefix`   | `ldasilva-httpsink-demo`             | Prefix for all resource names + the bucket name. |
| `ApiStageName`     | `demo`                               | Stage segment in the invoke URL.                 |
| `ApiKeyValue`      | `ChangeMeHttpSinkDemoKey0123456789`  | Secret sent in the `x-api-key` header.           |
| `LambdaTimeout`    | `30`                                 | Lambda timeout (seconds).                        |
| `LambdaMemorySize` | `256`                                | Lambda memory (MB).                              |

> The **AWS region** is chosen by the `--region` flag at deploy time — no
> parameter needed.

## Deploy

```bash
aws cloudformation deploy \
  --template-file setup/aws/httpsink-demo.yaml \
  --stack-name httpsink-demo \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1 \
  --parameter-overrides \
      ResourcePrefix=ldasilva-httpsink-demo \
      ApiStageName=demo \
      ApiKeyValue=MySuperSecretDemoKey123456
```

Get the endpoint and bucket back:

```bash
aws cloudformation describe-stacks \
  --stack-name httpsink-demo \
  --region us-east-1 \
  --query "Stacks[0].Outputs" --output table
```

Quick smoke test (replace URL and key):

```bash
curl -s -X POST \
  -H "Content-Type: application/json" \
  -H "x-api-key: MySuperSecretDemoKey123456" \
  -d '{"hello":"world"}' \
  https://<api-id>.execute-api.us-east-1.amazonaws.com/demo/records
```

You should see a `200` and a new object in the bucket.

## Next step: create the connector

With the endpoint and API key working, create the Confluent Cloud HTTP Sink V2
connector that feeds it — see [`setup/confluent`](../confluent/README.md).
You'll need the **InvokeUrl** and **ApiKeyValue** from the deploy outputs above.

## Tear down

CloudFormation cannot delete a non-empty bucket, so empty it first:

```bash
aws s3 rm s3://<bucket-name> --recursive --region us-east-1

aws cloudformation delete-stack \
  --stack-name httpsink-demo \
  --region us-east-1
```

That removes the API Gateway, Lambda, IAM role and bucket — nothing is left
running after the demo.
