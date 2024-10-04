variable "region" {}
variable "launch_script_version" {}
variable "artifacts_dir" {}
variable "s3_artifacts_dir" {}
variable "config_file_directory" {}
variable "config_file_version" {}

variable "job_version" {}
variable "sns_https_notification_endpoint" {
  type = string
  default = ""
  description = "Populate from CI/CD by defining value as environment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo."
}
