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
  job_name = "sup-search-perf"
  read_db_table_names = [
    "prod.core_quantum_events_sspp",
    "prod.atom_cs_call_care_data_3"
  ]
  write_db_table_names = [
    "prod_dasp.asp_support_quantum_events",
    "prod_dasp.asp_support_content_agg"
  ]
  step_scripts = {
    "1 - support_search" = "1_support_search-${var.job_version}",
    "2 - tableau-refresh" = "2_tableau_refresh-${var.job_version}"
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 500
  tez_memory_mb           = "8192"
  emr_version             = "emr-5.36.0"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(30 14 * * ? *)"
  enable_schedule = false

  ### Unlikely to change (per line of business) ###
  flow_category = "dasp"


  ### Change here to update job specific extra properties ###
  #extra_properties = {
  #   azkaban = {
  #      instance_name = "prod",
  #      project_name  = "asp_search_support_tab_refresh_check",
  #      flow_name     = "support_search_tab_refresh_check_end"
  #  }
  #}

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
