#!/usr/bin/env bash

PATH_TO_TERRAFORM_DIR=$1
TERRAFORM_STATE_S3_BUCKET_NAME=$2
TERRAFORM_STATE_S3_BUCKET_ENCRYPT=$3
TERRAFORM_STATE_S3_BUCKET_REGION=$4
TERRAFORM_STATE_LOCK_DYNAMO_DB_TABLE=$5
JOB_NAME=$6
ENV=$7

TERRAFORM_STATE_S3_KEY="${JOB_NAME}-${ENV}/terraform.tfstate"

echo  "Initializing terraform in $PATH_TO_TERRAFORM_DIR,\
 s3_bucket: $TERRAFORM_STATE_S3_BUCKET_NAME \
 encrypt: $TERRAFORM_STATE_S3_BUCKET_ENCRYPT \
 region: $TERRAFORM_STATE_S3_BUCKET_REGION \
 dynamodb_table: $TERRAFORM_STATE_LOCK_DYNAMO_DB_TABLE\
 state_s3_key: $TERRAFORM_STATE_S3_KEY"

# terraform should be initialized from "terraform dir" not from root dir
# if `terraform init` executes from root dir, .terraform dir will be created in root dir and
# it can lead to some issues if 2 or more jobs running in parallel
cd ${PATH_TO_TERRAFORM_DIR};

# TODO enable it back later
#    -backend-config="dynamodb_table=$TERRAFORM_STATE_LOCK_DYNAMO_DB_TABLE" \

terraform init \
    -backend-config="bucket=$TERRAFORM_STATE_S3_BUCKET_NAME" \
    -backend-config="region=$TERRAFORM_STATE_S3_BUCKET_REGION" \
    -backend-config="key=$TERRAFORM_STATE_S3_KEY" \
    -backend-config="encrypt=$TERRAFORM_STATE_S3_BUCKET_ENCRYPT"


echo "Terraform init finished"
