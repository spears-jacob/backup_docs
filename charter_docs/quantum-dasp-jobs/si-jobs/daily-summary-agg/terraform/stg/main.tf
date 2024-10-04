provider "aws" {
  region = var.region
}

locals {
  job_tags = {
    Team             = "self-install-reporting"
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
    "stg.core_quantum_events",
    "stg.atom_cs_call_care_data_3",
    "stg.atom_work_orders",
    "stg_dasp.si_summary_page_base_master",
    "stg_dasp.si_core_quantum_events",
    "stg.core_quantum_events_sspp"
  ]
  write_db_table_names = [
    "stg_dasp.si_summary_page_base_master",
    "stg_dasp.si_summary_agg_master",
	"stg_dasp.si_summary_call_agg_master",
	"stg_dasp.si_composite_score_kpi_dist_daily",
    "stg_dasp.si_composite_score_kpi_agg_daily",
    "stg_dasp.si_cross_platform",
    "stg_dasp.si_composite_score_kpi_agg_visit_daily",
    "stg_dasp.si_core_quantum_events",
    "stg_dasp.si_summary_tableau"
  ]
  step_scripts = {
    "1 - si_summary/composite_score" = var.job_version
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 500
  tez_memory_mb = "8192"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(30 13 * * ? *)"
  enable_schedule = false

  ### Unlikely to change (per line of business) ###
  flow_category = "si"

  ### To reprocess multiple days, set the list of number of days to substract from RUN_DATE ("3 2 1"); send "0" to avoid reprocess step ###
  default_run_option= "3 2 1"

  ### Do not change ###
  config_output_file = "${var.config_file_directory}/transient-emr-config-${var.config_file_version}.json"
  launch_script_version = var.launch_script_version
  config_file_version = var.config_file_version
  artifacts_dir = var.artifacts_dir
  s3_artifacts_dir = var.s3_artifacts_dir
  job_version = var.job_version
  sns_https_notification_endpoint = var.sns_https_notification_endpoint #Populate from CI/CD by defining value as enviornment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo.
  source = "../../../../scope-job-template/terraform/environments/stg"
}
