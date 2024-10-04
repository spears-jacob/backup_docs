provider "aws" {
  region = var.region
}
terraform {
  backend "s3" {}
}

locals {
  flow_category = "sg"
  s3_tags = {
    Mission-critical = "NULL"
    App              = "NULL"
    Tech             = "hive"
    Stack            = "hive"
  }
}

module "sg_vmx_ddl_manager" {
  source = "../../../../core/ddl-manager-template"

  business_unit = var.business_unit
  stream_category = var.stream_category
  env = var.env
  flow_category = local.flow_category

  region = var.region

  athena_output_s3_bucket_name = var.athena_output_s3_bucket_name
  db_table_name_to_config_map = module.sg_vmx_tables_configuration.db_table_name_to_config_map
}

module "sg_vmx_tables_configuration" {
  source = "../../configuration"
  env = var.env
}

module "sg_vmx_init_s3_buckets" {
  source = "../../s3-buckets-init"
  business_unit = var.business_unit
  env = var.env
  environment_class = "production"
  flow_category = local.flow_category
  kms_key_arn = var.kms_key_arn
  region = var.region
  replication_kms_key_arn = var.replication_kms_key_arn
  replication_region = var.replication_region
  stream_category = var.stream_category
  Org                     = var.Org
  Group                   = var.Group
  Team                    = var.Team
  Solution                = var.Solution
  s3_tags                 = local.s3_tags
}