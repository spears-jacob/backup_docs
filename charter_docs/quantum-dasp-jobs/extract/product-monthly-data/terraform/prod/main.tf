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
  job_name = "product-monthly-data"
  read_db_table_names = [
    "prod.core_quantum_events_sspp",
    "prod_dasp.quantum_metric_agg_portals",
    "prod_dasp.quantum_set_agg_portals",
    "prod_dasp.cs_prod_monthly_fiscal_month_metrics",
    "prod_dasp.asp_digital_adoption_daily"
  ]
  write_db_table_names = [
    "prod_dasp.asp_product_monthly_metrics",
    "prod_dasp.asp_product_monthly_time",
    "prod_dasp.asp_product_monthly_metrics_archive",
    "prod_dasp.asp_product_monthly_time_archive",
    "prod_dasp.asp_product_monthly_last_6months",
    "prod_dasp.asp_product_monthly_last_month",
    "prod_dasp.asp_monthly_referring_traffic",
    "prod_dasp.asp_specmobile_login",
    "prod_dasp.asp_digital_adoption_monthly",
    "prod_dasp.asp_digital_adoption_monthly_archive"
  ]
  step_scripts = {
    "1 - product_monthly_data" = var.job_version
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 2000
  tez_memory_mb = "3072"
  emr_version = "emr-5.36.0"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(30 14 * * ? *)"
  enable_schedule = false

  ### Unlikely to change (per line of business) ###
  flow_category = "dasp"


  ### Change here to update job specific extra properties ###
  extra_properties = {}

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
