provider "aws" {
  region = var.region
}

locals {
  job_tags = {
    Mission-critical = "no"
    App              = "NULL"
    Tech             = "hive"
    Stack            = "hive"
  }
}
module "job" {
  job_tags = local.job_tags
  ### Change per-job ###
  job_name = "onlineform-errors-dashboard"
  read_db_table_names = [
    "prod.core_quantum_events_sspp",
    "prod_dasp.cs_calls_with_prior_visits"
  ]
  write_db_table_names = [
    "prod_dasp.asp_onlineform_errors_dashboard"
  ]
  step_scripts = {
    "1 - asp_onlineform_errors_dashboard" = "1_asp_onlineform_errors_dashboard-${var.job_version}",
    "2 - tableau-refresh" = "2_tableau_refresh-${var.job_version}"
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 500
  tez_memory_mb           = "8192"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(0 10 * * ? *)"
  enable_schedule = true

  ### Unlikely to change (per line of business) ###
  flow_category = "dasp"

  ### Do not change ###
  config_output_file = "${var.config_file_directory}/transient-emr-config-${var.config_file_version}.json"
  launch_script_version = var.launch_script_version
  config_file_version = var.config_file_version
  artifacts_dir = var.artifacts_dir
  s3_artifacts_dir = var.s3_artifacts_dir
  job_version = var.job_version
  sns_https_notification_endpoint = var.sns_https_notification_endpoint #Populate from CI/CD by defining value as environment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo.
  glue_catalog_separator = "/" ### Parameter to access prod tables in stg
  source = "../../../../scope-job-template/terraform/environments/prod"
}