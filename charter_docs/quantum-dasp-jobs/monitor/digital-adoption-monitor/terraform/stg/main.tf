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
  job_name = "d-digital_adoption_monitor"
  read_db_table_names = [
    "stg.atom_accounts_snapshot",
    "stg.atom_cs_call_care_data_3",
    "stg.core_quantum_events_sspp",
    "stg_dasp.asp_digital_adoption_daily",
    "stg_dasp.asp_digital_adoption_monitor_null",
    "stg_dasp.asp_digital_adoption_monitor_cqe",
    "stg_dasp.asp_digital_adoption_monitor_da",
    "stg_dasp.asp_digital_adoption_monitor_range"
  ]
  write_db_table_names = [
    "stg_dasp.asp_digital_adoption_daily",
    "stg_dasp.asp_digital_adoption_daily_check",
    "stg_dasp.asp_digital_adoption_monitor_null",
    "stg_dasp.asp_digital_adoption_monitor_null_archive",
    "stg_dasp.asp_digital_adoption_monitor_cqe",
    "stg_dasp.asp_digital_adoption_monitor_cqe_archive",
    "stg_dasp.asp_digital_adoption_monitor_da",
    "stg_dasp.asp_digital_adoption_monitor_da_archive",
    "stg_dasp.asp_digital_adoption_monitor_range",
    "stg_dasp.asp_digital_adoption_monitor_outlier",
    "stg_dasp.asp_digital_adoption_monitor_log"
  ]
  step_scripts = {
    "1 - digital_adoption_monitor" = var.job_version
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 1500
  tez_memory_mb = "8192"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(30 14 * * ? *)"
  enable_schedule = false

  ### Unlikely to change (per line of business) ###
  flow_category = "dasp"

  ### Do not change ###
  config_output_file = "${var.config_file_directory}/transient-emr-config-${var.config_file_version}.json"
  launch_script_version = var.launch_script_version
  config_file_version = var.config_file_version
  artifacts_dir = var.artifacts_dir
  s3_artifacts_dir = var.s3_artifacts_dir
  job_version = var.job_version
  sns_https_notification_endpoint = var.sns_https_notification_endpoint #Populate from CI/CD by defining value as enviornment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo.
  glue_catalog_separator = "/"
  source = "../../../../scope-job-template/terraform/environments/stg"
}
