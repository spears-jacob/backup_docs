provider "aws" {
  region = var.region
}

locals {
  job_tags = {
    Mission-critical = "no"
    App              = "dasp-quality"
    Tech             = "hive"
    Stack            = "hive"
  }
}
module "job" {
  job_tags = local.job_tags
  ### Change per-job ###
  job_name = "dasp-quality"
  read_db_table_names = [
    "prod.core_quantum_events_sspp",
    "prod_dasp.asp_quality_kpi_core",
    "prod_dasp.asp_quality_kpi_dist",
    "prod_dasp.asp_quality_kpi_mos",
    "prod_dasp.quantum_metric_agg_portals"
  ]
  write_db_table_names = [
    "prod_dasp.asp_quality_kpi_core",
    "prod_dasp.asp_quality_kpi_dist",
    "prod_dasp.asp_quality_kpi_mos",
    "prod_dasp.asp_quality_visit_agg",
    "prod_dasp.asp_quality_bucket_distribution"
  ]
  step_scripts = {
    "1 - dasp_quality" = var.job_version
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 2000
  tez_memory_mb = "3584"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(15 12 * * ? *)"
  enable_schedule = true

  ### Unlikely to change (per line of business) ###
  flow_category = "dasp"

  ### Change here to update job specific extra properties ###
  extra_properties = {
     azkaban = {
        instance_name = "prod",
        project_name  = "asp_quality_tab_refresh_check",
        flow_name     = "asp_quality_tableau_refresh_check_end"
    }
  }


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
