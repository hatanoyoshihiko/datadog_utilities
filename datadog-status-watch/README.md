# Datadog Status Watch (SAM)

Datadog のグローバル Status API を5分に1回定期チェックし、`indicator != "none"` のとき SNS に通知します。

## デプロイ

```bash
$ aws s3 mb s3://aws-sam-cli-managed-default-YOUR_AWS_ACCOUNT
$ sam build
$ sam deploy \
  --stack-name datadog-status-watch \
  --s3-bucket aws-sam-cli-managed-default-YOUR_AWS_ACCOUNT \
  --s3-prefix datadog-status-watch \
  --capabilities CAPABILITY_NAMED_IAM \
  --region ap-northeast-1 \
  --profile YOUR_AWS_PROFILE_NAME \
  --parameter-overrides \
    'ProjectName="datadog-status-watch" \
    SourceBucketName="datadog-status-watch-YOUR_AWS_ACCOUNT" \
    LambdaRoleName="datadog-status-watch-exec-role" \
    ManagedPolicyName="AWSLambdaBasicExecutionRole"
    SnsEmailSubscription=your.name@example.com'
```

## After Deploy

Confirm your E-mail Subscription.

## Resource

The following resources will be created.

- Lambda
- SNS Topic
- SNS Subscription
- Event Bridge
