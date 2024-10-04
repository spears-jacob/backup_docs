provider "aws" {
  region = var.region
}


locals {
  job_tags = {
    Mission-critical = "no"
  	Solution         = "dasp"
    Tech             = "hive"
    Stack            = "hive"
  }
}
module "job" {
  job_tags = local.job_tags
  ### Change per-job ###
  job_name = "dasp-metagg-call-enrich"
  read_db_table_names = [
    "prod_dasp.cs_calls_with_prior_visits",
    "prod_dasp.quantum_metric_agg_portals"
  ]
  write_db_table_names = [
    "prod_dasp.quantum_metric_agg_portals"
  ]
  step_scripts = {
    "1 - dasp-metagg-call-enrich" = var.job_version
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 500
  tez_memory_mb           = "8192"
  emr_version             = "emr-5.36.0"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(15 17 * * ? *)"
  enable_schedule = true

  ### Unlikely to change (per line of business) ###
  flow_category = "cs"

  ### Do not change ###
  config_output_file = "${var.config_file_directory}/transient-emr-config-${var.config_file_version}.json"
  launch_script_version = var.launch_script_version
  config_file_version = var.config_file_version
  artifacts_dir = var.artifacts_dir
  s3_artifacts_dir = var.s3_artifacts_dir
  job_version = var.job_version
  sns_https_notification_endpoint = var.sns_https_notification_endpoint #Populate from CI/CD by defining value as enviornment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo.
  glue_catalog_separator = "/" ### Parameter to access prod tables in stg
  source = "../../../../scope-job-template/terraform/environments/prod"
}
