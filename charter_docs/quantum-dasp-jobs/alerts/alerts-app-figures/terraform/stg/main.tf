locals {
  job_tags = {
    Mission-critical = "no"
    App              = "NULL"
    Tech             = "hive"
    Stack            = "hive"
  }
}

provider "aws" {
  region = var.region
}

module "alert-jobs" {
  source = "../../../../job-template/terraform/modules/alert-template/alert-job-template"

  job_name = "alert-app-figures"
  environment = "stg"
  artifacts_dir = var.artifacts_dir
  job_tags      = local.job_tags

  ### Unlikely to change (per line of business) ###
  flow_category = "dasp"

  file_masks = ["af_", "archive/<datemask>/af_"]
  s3_bucket_name = "pi-qtm-dasp-prod-aggregates-nopii"
  s3_path = "data/prod_dasp/nifi/app_figures/"

  data_description = "App Figures"
  data_usage_threshold = 100001
  use_hours = false
  start_offset = -1
  calc_size_across_all_filemasks = true

  email_from = "PI.Tableau@charter.com"
  email_to = ["elliott.easterly@charter.com"]
  email_cc = ["c-elliott.easterly@charter.com"]

  is_debugging_enabled = false
  email_debug_to  = ["elliott.easterly@charter.com"]
  email_debug_cc = ["c-elliott.easterly@charter.com"]

  retry_attempts = 8
  retry_after_sec = 5280
  query_athena = false
}
