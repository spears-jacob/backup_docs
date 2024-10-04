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
  job_name = "page-render-times"
  read_db_table_names = [
    "prod.core_quantum_events_sspp"
  ]
  write_db_table_names = [
    "prod_dasp.asp_page_render_time_seconds_page_views_visits",
    "prod_dasp.asp_agg_page_load_time_grouping_sets_quantum",
    "prod_dasp.asp_hourly_page_load_tenths_quantum",
    "prod_dasp.asp_top_browsers_quantum",
    "prod_dasp.asp_hourly_page_load_tenths_by_browser_quantum"
  ]
  step_scripts = {
    "1 - page_load_time_page_views_visits" = var.job_version
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 2000
  tez_memory_mb = "3584"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(30 14 * * ? *)"
  enable_schedule = false

  ### Unlikely to change (per line of business) ###
  flow_category = "dasp"

  ### Change here to update job specific extra properties ###
  extra_properties = {
     azkaban = {
        instance_name = "prod",
        project_name  = "asp_page_render_times_tab_refresh_check",
        flow_name     = "page_render_times_tableau_refresh_check_end"
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
