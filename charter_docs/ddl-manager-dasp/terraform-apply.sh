#!/bin/bash
set -e

PATH_TO_TERRAFORM_DIR=$1
ENVIRONMENT=$2

cd ${PATH_TO_TERRAFORM_DIR}

terraform apply -input=false plan-${ENVIRONMENT}.tf
