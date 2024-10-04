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
  job_name = "pmai-feeds-export"
  read_db_table_names = [
      "stg_dasp.asp_pmai_sia_device_out"
  ]
  write_db_table_names = [
    "stg_dasp.app_figures_downloads",
    "stg_dasp.app_figures_ranks",
    "stg_dasp.app_figures_sentiment"
  ]
  step_scripts = {
    "1 - ingest-app-figures" = var.job_version
  }

  ## write keys for secure bucket
  write_kms_key = "arn:aws:kms:us-east-1:213705006773:key/41e24499-e622-4796-98ad-9ffaba7d3bef"

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 500
  tez_memory_mb           = "8192"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(30 14 * * ? *)"
  enable_schedule = false

  ### Unlikely to change (per line of business) ###
  flow_category = "dasp"

  ### Do not change ###
  config_output_file              = "${var.config_file_directory}/transient-emr-config-${var.config_file_version}.json"
  launch_script_version           = var.launch_script_version
  config_file_version             = var.config_file_version
  artifacts_dir                   = var.artifacts_dir
  s3_artifacts_dir                = var.s3_artifacts_dir
  job_version                     = var.job_version
  sns_https_notification_endpoint = var.sns_https_notification_endpoint #Populate from CI/CD by defining value as enviornment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo.
  source                          = "../../../../scope-job-template/terraform/environments/stg"
}
