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
  job_name = "sup-article-call-disp"
  read_db_table_names = [
    "prod.core_quantum_events_sspp",
    "prod.atom_cs_call_care_data_3",
    "prod_dasp.cs_calls_with_prior_visits",
    "prod_dasp.cs_dispositions_issue_groups",
    "prod_dasp.cs_dispositions_cause_groups",
    "prod_dasp.cs_dispositions_resolution_groups"
  ]
  write_db_table_names = [
    "prod_dasp.asp_support_article_call_prep",
    "prod_dasp.asp_support_article_call_disposition"
  ]
  step_scripts = {
    "1 - support_article_call_disp" = "1_support_article_call_disp-${var.job_version}",
    "2 - tableau-refresh" = "2_tableau_refresh-${var.job_version}"
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 500
  tez_memory_mb = "3584"
  
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
  sns_https_notification_endpoint = var.sns_https_notification_endpoint #Populate from CI/CD by defining value as environment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo.
  glue_catalog_separator = "/" ### Parameter to access prod tables in stg
  source = "../../../../scope-job-template/terraform/environments/prod"
}
