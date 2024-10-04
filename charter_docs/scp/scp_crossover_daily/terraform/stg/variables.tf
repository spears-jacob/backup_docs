variable "region" {}
variable "launch_script_version" {}
variable "artifacts_dir" {}
variable "s3_artifacts_dir" {}
variable "config_file_directory" {}
variable "config_file_version" {}
variable "job_version" {}
variable "job_name" {}
variable "default_target_capacity" {}
variable "flow_category" {}
variable "allow_parallel_execution" {}
variable "enable_schedule" {}
variable "on_demand_switch" {}
variable "schedule_expression" {}
variable "tez_memory_mb" {}
variable "emr_config_key" { default = "" }
variable "read_db_table_names" { default = [] }
variable "write_db_table_names" { default = [] }
variable "sns_https_notification_endpoint" {
  type        = string
  default     = ""
  description = "Populate from CI/CD by defining value as enviornment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo."
}

variable "TERRAFORM_BUCKET" {
  description = "This is the CICD ENV Variable TF_VAR_TERRAFORM_BUCKET. It gets populated from the root terraform module"
}
variable "TERRAFORM_BUCKET_REGION" {
  description = "This is the CICD ENV Variable TF_VAR_TERRAFORM_BUCKET_REGION. It gets populated from the root terraform module"
}
variable "ENV_NAME" {
  description = "This is the CICD ENV Variable TF_VAR_ENV_NAME. It gets populated from the root terraform module"
}
variable "EMR_CONFIG_REPO_NAME" {
  description = "This is the CICD ENV Variable TF_VAR_EMR_CONFIG_REPO_NAME. It gets populated from the root terraform module"
}
variable "REGION" {
  description = "This is the CICD ENV Variable TF_VAR_EMR_CONFIG_REPO_NAME. It gets populated from the root terraform module"
}