locals {
   step_scripts = {
    "01 - scp_portals_daily_action_extract_from_cqesspp" = "01_scp_portals_daily_action_extract_from_cqesspp-${var.job_version}",
    "02 - scp_account_agg_from_action_extract"           = "02_scp_account_agg_from_action_extract-${var.job_version}",
    "03 - scp_actions_agg_30daylag_from_action_extract"  = "03_scp_actions_agg_30daylag_from_action_extract-${var.job_version}",
    "04 - scp_portals_billing_agg"                       = "04_load_scp_portals_billing_agg-${var.job_version}",
    "05 - scp_pause_schedule_action_agg"                 = "05_scp_pause_schedule_action_agg-${var.job_version}",
    "06 - scp_snapshot"                                  = "06_scp_snapshot-${var.job_version}",
    "07 - scp_login"                                     = "07_scp_login-${var.job_version}",
    "08 - scp_login_subscriber_agg"                      = "08_scp_login_subscriber_agg-${var.job_version}",
    "09 - scp_subscriber_agg"                            = "09_scp_subscriber_agg-${var.job_version}",
    "10 - scp_portals_visitation_rates"                  = "10_scp_portals_visitation_rates-${var.job_version}",
    "11 - scp_pause"                                     = "11_scp_pause-${var.job_version}",
    "12 - scp_pause_agg"                                 = "12_scp_pause_agg-${var.job_version}",
    "13 - tableau_refresh"                               = "13_tableau_refresh-${var.job_version}"
  }
  job_tags = {
    Mission-critical = "no"
    App              = "NULL"
    Tech             = "spark"
    Stack            = "spark"
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