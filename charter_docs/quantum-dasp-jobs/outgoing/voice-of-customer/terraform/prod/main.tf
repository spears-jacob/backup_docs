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
  job_name = "voice-of-customer"
  read_db_table_names = [
    "prod.core_quantum_events_sspp"
  ]
  write_db_table_names = [
    "prod_dasp.asp_extract_voice_of_customer_troubleshooting",
    "prod_dasp.asp_extract_voice_of_customer_mobile_troubleshooting"
  ]
  step_scripts = {
    "1 - extract: voice of customer Troubleshooting"  = "1_asp_extract_voice_of_customer_troubleshooting-${var.job_version}",
    "2 - extract: voice of customer Mobile Troubleshooting"  = "2_asp_extract_voice_of_customer_mobile_troubleshooting-${var.job_version}",
    "3 - outgoing secure: voice of customer Troubleshooting"  = "3_outgoing_secure_voice_of_customer_troubleshooting-${var.job_version}",
    "4 - outgoing secure: voice of customer Mobile Troubleshooting"  = "4_outgoing_secure_voice_of_customer_mobile_troubleshooting-${var.job_version}"
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 500
  tez_memory_mb           = "8192"
 
  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(0 8 * * ? *)"
  enable_schedule = true

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
