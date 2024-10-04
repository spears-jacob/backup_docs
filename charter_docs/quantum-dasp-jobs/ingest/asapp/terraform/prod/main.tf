provider "aws" {
  region = var.region
}
terraform {
  backend "s3" {}
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
  job_name = "ingest-asapp"
  read_db_table_names = [

  ]
  write_db_table_names = [
    "prod_dasp.asp_asapp_convos_intents_ended_ingest",
    "prod_dasp.asp_asapp_convos_intents_ingest",
    "prod_dasp.asp_asapp_convos_metadata_ended_ingest",
    "prod_dasp.asp_asapp_convos_metadata_ingest",
    "prod_dasp.asp_asapp_convos_metrics_ended_ingest",
    "prod_dasp.asp_asapp_convos_metrics_ingest",
    "prod_dasp.asp_asapp_csid_containment_ingest",
    "prod_dasp.asp_asapp_customer_feedback_ingest",
    "prod_dasp.asp_asapp_customer_params_ingest",
    "prod_dasp.asp_asapp_export_row_counts_ingest",
    "prod_dasp.asp_asapp_flow_completions_ingest",
    "prod_dasp.asp_asapp_flow_detail_ingest",
    "prod_dasp.asp_asapp_intents_ingest",
    "prod_dasp.asp_asapp_issue_queues_ingest",
    "prod_dasp.asp_asapp_rep_activity_ingest",
    "prod_dasp.asp_asapp_rep_attributes_ingest",
    "prod_dasp.asp_asapp_rep_augmentation_ingest",
    "prod_dasp.asp_asapp_rep_convos_ingest",
    "prod_dasp.asp_asapp_rep_hierarchy_ingest",
    "prod_dasp.asp_asapp_rep_utilized_ingest",
    "prod_dasp.asp_asapp_reps_ingest",
    "prod_dasp.asp_asapp_transfers_ingest",
    "prod_dasp.asp_asapp_utterances_ingest"
  ]
  step_scripts = {
    "1 - ingest-asapp" = var.job_version
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 500
  tez_memory_mb = "8192"

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
