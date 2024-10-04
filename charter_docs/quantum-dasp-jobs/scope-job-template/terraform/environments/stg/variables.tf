variable "schedule_expression" {
  default = "cron(30 12 * * ? *)"
}
variable "enable_schedule" {
  default = false
}
variable "launch_script_version" {}

variable "default_target_capacity" {
  default = 1
}

variable "allow_parallel_execution" {
  default = true
}

variable "master_instance_types" {
  type = list(string)
  default = [
      "r5.2xlarge",
      "r5b.2xlarge",
      "r5dn.2xlarge",
      "r5dn.4xlarge"
    ]
}
variable "worker_instance_types" {
  type = list(any)

  default = [
    {
      "type"  = "r5.2xlarge",
      "units" = 10
    },
    {
      "type"  = "r5b.2xlarge",
      "units" = 10
    },
#
    {
      "type"  = "r5dn.2xlarge",
      "units" = 10
    },
    {
      "type"  = "r5dn.4xlarge",
      "units" = 10
    }
  ]
}
variable "price_as_percentage" {
  default = 100
}
variable "tez_memory_mb" {
  default = "8192"
}
variable "glue_catalog_separator" {
  type = string
  default = ""
}
variable "artifacts_dir" {}
variable "s3_artifacts_dir" {}
variable "config_file_version" {}
// the keys() and values() functions are used to populate the elements of the EMR Steps
// in combination with the numeric index of each list.
// these functions return lists in lexicographic order, sorted by the key. we number
// the keys to ensure deterministic ordering
variable "step_scripts" {
  type = "map"
}

variable "job_version" {}

variable "config_output_file" {}

variable "job_name" {}
variable "flow_category" {}
variable "stream_category" {
  default = "qtm"
}
variable "business_unit" {
  default = "pi"
}

variable "artifacts_bucket" {
  # TODO value from gitlab should be used, should be deduplicated, it will not work for dev & prod deployments at the same time
  default = "pi-qtm-global-dev-artifacts"
}

variable "logs_bucket" {
  default = "qtm-temp-logs-bucket"
}

variable "read_db_table_names" {
  type = "list"
}
variable "write_db_table_names" {
  type = "list"
}

variable "sns_https_notification_endpoint" {
  type        = string
  default     = ""
  description = "Populate from CI/CD by defining value as enviornment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo."
}

variable "extra_properties" {
  type        = "map"
  default     = {}
  description = "job specific extra properties which will be used in SNS notification"
}
variable "job_tags" {
  type = map(string)
}

variable "emr_version" {
  default = "emr-6.8.0"
}

variable "core_ebs_volume_size_in_gb" {
  type = number
  default = 128
  description = "EBS volume size (GB) for core instances"
}

variable "allocation_strategy" {
  type    = string
  default = ""
}
variable "block_duration_minutes" {
  type    = number
  default = 0
}

variable "default_run_option" {
  default = 0
}

variable "write_kms_key" {
  default = "arn:aws:kms:us-east-1:213705006773:key/41e24499-e622-4796-98ad-9ffaba7d3bef"
}
