provider "aws" {
  region = var.region
}

locals {
  job_tags = {
    Mission-critical = "no"
    Tech             = "hive"
    Stack            = "hive"
    Function         = "enrichment"
  }
}
module "job" {
  job_tags = local.job_tags
  ### Change per-job ###
  job_name = "d-summary-agg"
  read_db_table_names = [
    "dev.core_quantum_events",
    "dev.atom_cs_call_care_data_3",
    "dev.atom_work_orders",
    "dev_dasp.si_summary_page_base_master",
    "dev_dasp.si_core_quantum_events"
  ]
  write_db_table_names = [
    "dev_dasp.si_summary_page_base_master",
    "dev_dasp.si_summary_agg_master",
    "dev_dasp.si_summary_call_agg_master",
    "dev_dasp.si_composite_score_kpi_dist_daily",
    "dev_dasp.si_composite_score_kpi_agg_daily",
    "dev_dasp.si_cross_platform",
    "dev_dasp.si_composite_score_kpi_agg_visit_daily",
    "dev_dasp.si_core_quantum_events"
  ]
  step_scripts = {
    "1 - si_summary/composite_score" = var.job_version
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 200
  tez_memory_mb = "8192"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(30 13 * * ? *)"
  enable_schedule = false

  ### Unlikely to change (per line of business) ###
  flow_category = "si"

  ### Do not change ###
  config_output_file = "${var.config_file_directory}/transient-emr-config-${var.config_file_version}.json"
  launch_script_version = var.launch_script_version
  config_file_version = var.config_file_version
  artifacts_dir = var.artifacts_dir
  s3_artifacts_dir = var.s3_artifacts_dir
  job_version = var.job_version
  sns_https_notification_endpoint = var.sns_https_notification_endpoint #Populate from CI/CD by defining value as enviornment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo.
  source = "../../../../scope-job-template/terraform/environments/dev"
}
