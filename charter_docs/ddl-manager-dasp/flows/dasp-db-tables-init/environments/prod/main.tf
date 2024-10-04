provider "aws" {
  region = var.region
}


locals {
  flow_category = "dasp"
  s3_tags = {
    Mission-critical = "no"
    App              = "NULL"
    Tech             = "hive"
    Stack            = "hive"
  }
}

module "dasp_ddl_manager" {
  source = "git_at_company_dot_net:awspilot/ddl-manager-templatedot_git//core/ddl-manager-template"

  business_unit = var.business_unit
  stream_category = var.stream_category
  env = var.env
  flow_category = local.flow_category

  region = var.region

  athena_output_s3_bucket_name = var.athena_output_s3_bucket_name
  db_table_name_to_config_map = module.dasp_tables_configuration.db_table_name_to_config_map
}

module "dasp_tables_configuration" {
  source = "../../configuration"
  env = var.env
  flow_category = local.flow_category
}

module "dasp_init_s3_buckets" {
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