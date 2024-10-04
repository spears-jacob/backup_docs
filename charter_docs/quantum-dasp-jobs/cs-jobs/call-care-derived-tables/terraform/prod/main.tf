provider "aws" {
  region = var.region
}


locals {
  job_tags = {
    Mission-critical = "no"
  	Solution         = "cs"
    Tech             = "scala"
    Stack            = "scala"
  }
}
module "job" {
  job_tags = local.job_tags
  ### Change per-job ###
  job_name = "call-care-derived"
  read_db_table_names = [
    "prod.atom_call_care",
    "prod.atom_cs_call_care_data_3",
    "prod_dasp.cs_resolution_lookup",
    "prod_dasp.cs_issue_cause_lookup",
    "prod.core_quantum_events_sspp"#,
#    "prod.atom_call_care_test",
#    "prod.atom_cs_call_care_data_3_test",
#    "prod_dasp.cs_resolution_lookup_test",
#    "prod_dasp.cs_issue_cause_lookup_test",
#    "prod.core_quantum_events_sspp_test"
  ]
  write_db_table_names = [
    "prod_dasp.cs_call_in_rate",
    "prod_dasp.cs_prodmonthly_call_in_rate_source",
    "prod_dasp.cs_calls_with_prior_visits"#,
#    "prod_dasp.cs_call_in_rate_test",
#    "prod_dasp.cs_calls_with_prior_visits_test"
  ]
  step_scripts = {
    "1 - call-care-derivations" = "1_call_care_derivations-${var.job_version}",
    "2 - tableau-refresh" = "2_tableau_refresh-${var.job_version}"
  }

  ### Change based on Devops recommendations/best practices ###
  default_target_capacity = 1000
  tez_memory_mb = "8192"

  ### only set the enable_schedule variable to true if job is to be run independent of orchestration ###
  schedule_expression = "cron(30 14 * * ? *)"
  enable_schedule = false

  ### Unlikely to change (per line of business) ###
  flow_category = "cs"

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
