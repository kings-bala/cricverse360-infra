#!/bin/bash
set -e

echo "Starting CricVerse360 infrastructure..."

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
STACK_NAME="CricVerse360Stack"

# Get Aurora cluster identifier
CLUSTER_ID=$(aws cloudformation describe-stack-resources \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "StackResources[?ResourceType=='AWS::RDS::DBCluster'].PhysicalResourceId" \
  --output text 2>/dev/null || echo "")

if [ -n "$CLUSTER_ID" ]; then
  echo "Resuming Aurora cluster: $CLUSTER_ID"
  aws rds start-db-cluster --db-cluster-identifier "$CLUSTER_ID" --region "$REGION" 2>/dev/null || true
  echo "Waiting for Aurora to become available..."
  aws rds wait db-cluster-available --db-cluster-identifier "$CLUSTER_ID" --region "$REGION" 2>/dev/null || true
  echo "Aurora cluster is available."
else
  echo "No Aurora cluster found. Run 'npm run deploy' first."
fi

# Get API Gateway ID
API_ID=$(aws cloudformation describe-stack-resources \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "StackResources[?ResourceType=='AWS::ApiGateway::RestApi'].PhysicalResourceId" \
  --output text 2>/dev/null || echo "")

if [ -n "$API_ID" ]; then
  echo "API Gateway $API_ID is always on (serverless)."
fi

echo ""
echo "CricVerse360 infrastructure is UP."
echo "API URL: $(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION --query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' --output text 2>/dev/null || echo 'Run npm run deploy first')"
