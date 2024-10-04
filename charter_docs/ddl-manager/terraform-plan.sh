#!/bin/bash
set -e

PATH_TO_TERRAFORM_DIR=$1
ENVIRONMENT=$2

cd ${PATH_TO_TERRAFORM_DIR}

terraform get --update
terraform validate

echo "TF repo is ready"

# create the actual terraform plan
terraform plan -out=plan-${ENVIRONMENT}.tf -var-file=terraform.tfvars

echo "TF plan finished"
