#!/bin/bash

set -e

AWS_ACCOUNT_ID="642879525632"
REGION="ap-southeast-2"
COMPANY_PREFIX="carsales"
ENV="dev"

BUCKET_NAME="${COMPANY_PREFIX}-databricks-uc-metastore-${ENV}"
ROLE_NAME="databricks-unity-catalog-metastore-role"

echo "=========================================="
echo "Unity Catalog Setup"
echo "=========================================="
echo "AWS Account: ${AWS_ACCOUNT_ID}"
echo "Region: ${REGION}"
echo "Bucket: ${BUCKET_NAME}"
echo "=========================================="

echo "Step 1: Creating S3 bucket..."

aws s3api create-bucket \
  --bucket ${BUCKET_NAME} \
  --region ${REGION} \
  --create-bucket-configuration LocationConstraint=${REGION}

echo "✓ Bucket created: s3://${BUCKET_NAME}"

# Enable versioning
aws s3api put-bucket-versioning \
  --bucket ${BUCKET_NAME} \
  --versioning-configuration Status=Enabled

# Block public access
aws s3api put-public-access-block \
  --bucket ${BUCKET_NAME} \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

echo "✓ Bucket configured with versioning and public access blocked"

echo ""
echo "Step 2: Creating IAM role..."

cat > /tmp/metastore-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
      },
      "Action": "sts:AssumeRole",
      "Condition": {}
    }
  ]
}
EOF

ROLE_ARN=$(aws iam create-role \
  --role-name ${ROLE_NAME} \
  --assume-role-policy-document file:///tmp/metastore-trust-policy.json \
  --query 'Role.Arn' \
  --output text 2>/dev/null || aws iam get-role --role-name ${ROLE_NAME} --query 'Role.Arn' --output text)

echo "✓ IAM Role: ${ROLE_ARN}"

echo ""
echo "Step 3: Creating IAM policy..."

cat > /tmp/metastore-s3-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET_NAME}/*",
        "arn:aws:s3:::${BUCKET_NAME}"
      ],
      "Effect": "Allow"
    },
    {
      "Action": [
        "sts:AssumeRole"
      ],
      "Resource": [
        "${ROLE_ARN}"
      ],
      "Effect": "Allow"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name ${ROLE_NAME} \
  --policy-name databricks-unity-catalog-metastore-access \
  --policy-document file:///tmp/metastore-s3-policy.json

echo "✓ IAM policy attached"

echo ""
echo "=========================================="
echo "SETUP COMPLETED!"
echo "=========================================="
echo ""
echo "Metastore Configuration:"
echo "  S3 Bucket: s3://${BUCKET_NAME}"
echo "  IAM Role ARN: ${ROLE_ARN}"
echo "  Region: ${REGION}"
echo ""
echo "Next Steps:"
echo "1. Go to Databricks Account Console: https://accounts.cloud.databricks.com/"
echo "2. Click 'Catalog' > 'Create Metastore'"
echo "3. Enter:"
echo "   - Name: payment-pipeline-metastore-${ENV}"
echo "   - Region: ${REGION}"
echo "   - S3 Path: s3://${BUCKET_NAME}/metastore"
echo "   - IAM Role ARN: ${ROLE_ARN}"
echo "4. Assign your workspace to the metastore"
echo "=========================================="

# Cleanup temp files
rm -f /tmp/metastore-trust-policy.json
rm -f /tmp/metastore-s3-policy.json
