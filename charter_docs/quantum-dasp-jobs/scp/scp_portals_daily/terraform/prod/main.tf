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
  job_name = "scp-portals-daily"
  read_db_table_names = [
    "prod.core_quantum_events_sspp"
  ]

  write_db_table_names = [
    "prod_dasp.asp_scp_portals_acct_agg",
    "prod_dasp.asp_scp_portals_action",
    "prod_dasp.asp_scp_portals_actions_agg",
    "prod_dasp.asp_scp_portals_billing_agg",
    "prod_dasp.asp_scp_portals_subscriber_agg"
  ]

  step_scripts = {
    "1 - scp_portals_daily_action_extract_from_cqesspp" = "1_scp_portals_daily_action_extract_from_cqesspp-${var.job_version}",
    "2 - scp_account_agg_from_action_extract"           = "2_scp_account_agg_from_action_extract-${var.job_version}",
    "3 - scp_actions_agg_30daylag_from_action_extract"  = "3_scp_actions_agg_30daylag_from_action_extract-${var.job_version}",
    "4 - scp_portals_billing_agg"                       = "4_load_scp_portals_billing_agg-${var.job_version}",
    "5 - scp_subscriber_agg"                            = "5_scp_subscriber_agg-${var.job_version}",
    "6 - tableau_refresh"                               = "6_tableau_refresh-${var.job_version}"
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 500
  tez_memory_mb = "8192"


  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(01 09 * * ? *)"
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
  glue_catalog_separator = "/" ### Parameter to access stg tables in prod
  source = "../../../../scope-job-template/terraform/environments/prod"
}
