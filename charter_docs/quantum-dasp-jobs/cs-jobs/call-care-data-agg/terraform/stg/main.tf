provider "aws" {
  region = var.region
}


locals {
  job_tags = {
    Mission-critical = "no"
	  Solution         = "cs"
    Tech             = "hive"
    Stack            = "hive"
  }
}
module "job" {
  job_tags = local.job_tags
  ### Change per-job ###
  job_name = "call-care-data-agg"
  read_db_table_names = [
    "stg.core_quantum_events_sspp",
    "stg.atom_cs_call_care_data_3"
  ]
  write_db_table_names = [
    "stg_dasp.cs_call_care_data_agg"
  ]
  step_scripts = {
    "1 - cs-call-care-data-agg" = "1_cs_call_care_data_agg-${var.job_version}",
    "2 - tableau-refresh" = "2_tableau_refresh-${var.job_version}"
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 500
  tez_memory_mb = "8192"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(30 14 * * ? *)"
  enable_schedule = false

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
  source = "../../../../scope-job-template/terraform/environments/stg"
}
