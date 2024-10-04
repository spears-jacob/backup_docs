provider "aws" {
  region = var.region
}

locals {
  job_tags = {
    Mission-critical = "no"
    Solution         = "cs"
    App              = "NULL"
    Tech             = "hive"
    Stack            = "hive"
  }
}
module "job" {
  job_tags = local.job_tags
  ### Change per-job ###
  job_name = "pageview-click-aggregate"
  read_db_table_names = [
    "prod.core_quantum_events_sspp"
  ]
  write_db_table_names = [
    "prod_dasp.cs_calendar_monthly_pageview_selectaction_aggregate",
    "prod_dasp.cs_selectaction_aggregate",
    "prod_dasp.cs_dates",
    "prod_dasp.cs_daily_pageview_selectaction_aggregate",
    "prod_dasp.cs_monthly_pageview_selectaction_aggregate",
    "prod_dasp.cs_weekly_pageview_selectaction_aggregate",
    "prod_dasp.chtr_fiscal_month"
  ]
  step_scripts = {
    "1 - cs-pageview-click" = var.job_version
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 1500
  tez_memory_mb = "3584"

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
  source = "../../../../scope-job-template/terraform/environments/prod"
}
