#!/usr/bin/env bash
# bootstrap-tf-state.sh — run ONCE before `terraform init`.
#
# Creates:
#   - S3 bucket for Terraform state (versioned + encrypted)
#   - DynamoDB table for state locking
#
# Usage:
#   bash scripts/bootstrap-tf-state.sh <aws-account-id> [region]
#
# Example:
#   bash scripts/bootstrap-tf-state.sh 123456789012 us-east-1

set -euo pipefail

984445750473="${1:?Usage: $0 <account-id> [region]}"
REGION="${2:-us-east-1}"
BUCKET="anomaly-detector-tf-state-${984445750473}"
DYNAMO_TABLE="anomaly-tf-locks"

echo "==> Creating S3 state bucket: ${BUCKET}"
if [[ "$REGION" == "us-east-1" ]]; then
  aws s3api create-bucket \
    --bucket "${BUCKET}" \
    --region "${REGION}"
else
  aws s3api create-bucket \
    --bucket "${BUCKET}" \
    --region "${REGION}" \
    --create-bucket-configuration LocationConstraint="${REGION}"
fi

echo "==> Enabling versioning"
aws s3api put-bucket-versioning \
  --bucket "${BUCKET}" \
  --versioning-configuration Status=Enabled

echo "==> Enabling server-side encryption"
aws s3api put-bucket-encryption \
  --bucket "${BUCKET}" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

echo "==> Blocking public access"
aws s3api put-public-access-block \
  --bucket "${BUCKET}" \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

echo "==> Creating DynamoDB lock table: ${DYNAMO_TABLE}"
aws dynamodb create-table \
  --table-name "${DYNAMO_TABLE}" \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "${REGION}" \
  2>/dev/null || echo "  (table already exists, skipping)"

echo ""
echo "==> Done. Now update infra/main.tf backend block:"
echo "    Replace 984445750473 with: ${984445750473}"
echo ""
echo "==> Then run:"
echo "    cd infra && terraform init"
echo "    # If migrating from local state: terraform init -migrate-state"
