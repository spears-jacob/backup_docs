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

  job_name = "alert-portal-events"
  environment = "prod"
  artifacts_dir = var.artifacts_dir
  job_tags      = local.job_tags

  ### Unlikely to change (per line of business) ###
  flow_category = "dasp"

  file_masks = ["partition_date_utc=<datemask>/partition_date_hour_utc=<datehourmask>"]
  table_name = "core_quantum_events_sspp"

  data_description = "Portals Events (core_quantum_events_sspp in production)"
  data_usage_threshold = 100000000
  use_hours = true
  start_offset = -1
  calc_size_across_all_filemasks = false

  email_from = "PI.Tableau@charter.com"
  email_to = ["elliott.easterly@charter.com","dl-pi-asp-reporting@charter.com"]
  email_cc = ["c-elliott.easterly@charter.com"]

  is_debugging_enabled = false
  email_debug_to  = ["elliott.easterly@charter.com"]
  email_debug_cc = ["c-elliott.easterly@charter.com"]

  retry_attempts = 8
  retry_after_sec = 5280
  query_athena = false
}
