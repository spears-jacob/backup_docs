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
  job_name = "mobile-2dot0-dashboard"
  read_db_table_names = [
    "prod_dasp.asp_m2dot0_metric_agg",
    "prod_dasp.cs_calls_with_prior_visits",
    "prod_dasp.mvno_avgperlinecount_buckets_prod",
    "prod_dasp.asp_m2dot0_visitsRanked",
    "prod.core_quantum_events_sspp"
  ]
  write_db_table_names = [
    "prod_dasp.asp_m2dot0_visitsRanked",
    "prod_dasp.asp_m2dot0_CIRagg",
    "prod_dasp.asp_m2dot0_engagedHouseholds",
    "prod_dasp.asp_m2dot0_metric_visit_lookup",
    "prod_dasp.asp_m2dot0_apiRaw",
    "prod_dasp.asp_m2dot0_apiagg",
    "prod_dasp.asp_m2dot0_apiJoin",
    "prod_dasp.asp_m2dot0_apiFinal"
  ]
  step_scripts = {
    "1 - mobile_2dot0_visitsranked" = "1_mobile_2dot0_visitsranked-${var.job_version}",
    "2 - mobile_2dot0_dashboard" = "2_mobile_2dot0_dashboard-${var.job_version}",
    "3 - 3_m2dot0_api" = "3_m2dot0_api-${var.job_version}",
    "4 - view_create" = "4_view_create-${var.job_version}",
    "5 - tableau-refresh" = "5_tableau_refresh-${var.job_version}"
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 500
  tez_memory_mb           = "8192"

  ### Only set enable_schedule = true if job is to be run independent of orchestration ###
  schedule_expression = "cron(30 18 * * ? *)"
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
