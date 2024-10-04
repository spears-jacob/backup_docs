locals {
    step_scripts = {
    "1 - adhoc_step1" = "cipher-util-${var.job_version}.hql"
  }
    job_tags = {
    Mission-critical = "no"
    Tech             = "hive"
    Stack            = "hive"
    Function         = "sg-health"
  }
}
provider "aws" {
  region = var.region
}
module "job" {
  source = "../../../../scope-job-template/terraform/environments/prod"

  allow_parallel_execution        = var.allow_parallel_execution
  artifacts_dir                   = var.artifacts_dir
  config_file_version             = var.config_file_version
  config_output_file              = "${var.config_file_directory}/transient-emr-config.json"
  default_target_capacity         = var.default_target_capacity
  enable_schedule                 = var.enable_schedule
  ENV_NAME                        = var.ENV_NAME
  flow_category                   = var.flow_category
  job_name                        = var.job_name
  job_tags                        = local.job_tags
  job_version                     = var.job_version
  launch_script_version           = var.launch_script_version
  on_demand_switch                = var.on_demand_switch
  read_db_table_names             = var.read_db_table_names
  REGION                          = var.REGION
  s3_artifacts_dir                = var.s3_artifacts_dir
  schedule_expression             = var.schedule_expression
  sns_https_notification_endpoint = var.sns_https_notification_endpoint
  step_scripts                    = local.step_scripts
  TERRAFORM_BUCKET                = var.TERRAFORM_BUCKET
  TERRAFORM_BUCKET_REGION         = var.TERRAFORM_BUCKET_REGION
  tez_memory_mb                   = var.tez_memory_mb
  write_db_table_names            = var.write_db_table_names
  emr_config_key                  = var.emr_config_key
  EMR_CONFIG_REPO_NAME            = var.EMR_CONFIG_REPO_NAME
}