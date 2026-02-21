# CricVerse360 Infrastructure

AWS CDK infrastructure for CricVerse360 backend.

## Architecture

- **VPC**: Dedicated VPC (isolated from existing AWS resources)
- **Aurora Serverless v2**: PostgreSQL, scales to 0 ACU when idle ($0 when stopped)
- **Lambda**: Node.js 20 API handler (pay per request)
- **API Gateway**: REST API with CORS, throttling
- **Cognito**: User authentication (free < 50K users)
- **S3**: Asset storage (profile pics, uploads)

## Commands

```bash
npm install          # Install dependencies
npm run synth        # Synthesize CloudFormation template
npm run deploy       # Deploy to AWS
npm run destroy      # Tear down all resources

npm run infra:start  # Resume Aurora (after stop)
npm run infra:stop   # Stop Aurora ($0 cost)
```

## Cost

| State | Monthly Cost |
|-------|-------------|
| Active (low traffic) | $0-5 |
| Shutdown (infra:stop) | ~$0 |

## Prerequisites

- AWS CLI configured with credentials
- Node.js 20+
- CDK bootstrapped: `npx cdk bootstrap`
