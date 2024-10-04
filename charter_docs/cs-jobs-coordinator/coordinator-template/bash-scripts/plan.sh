#!/bin/bash
set -e

PATH_TO_TERRAFORM_DIR=$1

terraform init $PATH_TO_TERRAFORM_DIR
terraform get --update $PATH_TO_TERRAFORM_DIR
#terraform force-unlock -force b462b61c-8a5e-e027-44dd-a71e1566789e $PATH_TO_TERRAFORM_DIR
terraform plan -var-file="$PATH_TO_TERRAFORM_DIR/terraform.tfvars" $PATH_TO_TERRAFORM_DIR