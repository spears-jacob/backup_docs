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
  ### determins what the job is called everywhere and must match the parent folder
  job_name = "service-appts-dashboard"
  read_db_table_names = [
    ### see examples below ###
    "prod.core_quantum_events_sspp"
  ]
  write_db_table_names = [
    ### write to ###
    "prod_dasp.asp_service_appts"
  ]
   step_scripts = {
    "1 - service-appts-dashboard" = "1_service_appts_dashboard-${var.job_version}",
    "2 - tableau-refresh" = "2_tableau_refresh-${var.job_version}"
  }


  default_target_capacity = 500
  tez_memory_mb = "3584"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  ### for more info about cron function please visit: ### https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html#CronExpressions ###
  schedule_expression = "cron(0 12 * * ? *)"
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
  sns_https_notification_endpoint = var.sns_https_notification_endpoint #Populate from CI/CD by defining value as enviornment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo.
  source = "../../../../scope-job-template/terraform/environments/prod"
}
