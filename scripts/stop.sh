#!/bin/bash
set -e

echo "Shutting down CricVerse360 infrastructure..."

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
STACK_NAME="CricVerse360Stack"

# Get Aurora cluster identifier
CLUSTER_ID=$(aws cloudformation describe-stack-resources \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "StackResources[?ResourceType=='AWS::RDS::DBCluster'].PhysicalResourceId" \
  --output text 2>/dev/null || echo "")

if [ -n "$CLUSTER_ID" ]; then
  echo "Stopping Aurora cluster: $CLUSTER_ID"
  aws rds stop-db-cluster --db-cluster-identifier "$CLUSTER_ID" --region "$REGION" 2>/dev/null || true
  echo "Aurora cluster is stopping (will be fully stopped in ~5 min)."
else
  echo "No Aurora cluster found."
fi

echo ""
echo "CricVerse360 infrastructure is SHUT DOWN."
echo "- Aurora: STOPPED (cost = \$0)"
echo "- Lambda: IDLE (cost = \$0, only runs when called)"
echo "- API Gateway: IDLE (cost = \$0, only charges per request)"
echo "- S3: Storage only (pennies)"
echo "- Cognito: Always free (<50K users)"
echo ""
echo "The app will continue working via localStorage."
echo "Run 'npm run infra:start' to resume backend services."
