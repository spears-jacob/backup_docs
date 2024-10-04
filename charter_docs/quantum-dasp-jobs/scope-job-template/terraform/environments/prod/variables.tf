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
    "c5.2xlarge",
    "m5.2xlarge",
    "c5n.2xlarge",
  "c4.2xlarge"]
}
variable "worker_instance_types" {
  type = list(any)

  default = [
    {
      "type"  = "c5.2xlarge",
      "units" = 10
    },
    {
      "type"  = "m5.2xlarge",
      "units" = 9
    },
    {
      "type"  = "c5n.2xlarge",
      "units" = 10
    },
    {
      "type"  = "c4.2xlarge",
      "units" = 8
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

variable "s3_buckets" {
  type = "list"
  default = []
}

variable "read_db_table_names" {
  type = "list"
}
variable "write_db_table_names" {
  type = "list"
}
variable "job_name" {}
variable "flow_category" {}
variable "stream_category" {
  default = "qtm"
}
variable "business_unit" {
  default = "pi"
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
  default = "emr-6.7.0"
}

variable "core_ebs_volume_size_in_gb" {
  type = number
  default = 128
  description = "EBS volume size (GB) for core instances"
}

variable "allocation_strategy" {
  type    = string
  default = "capacity-optimized"
}
variable "block_duration_minutes" {
  type    = number
  default = 0
}
variable "write_kms_key" {
  default = "arn:aws:kms:us-east-1:387455165365:key/c226775d-0b8a-41a1-89bf-e5e7e361181f"
}
