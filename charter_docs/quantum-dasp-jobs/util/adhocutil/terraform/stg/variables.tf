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
  description = "Populate from CI/CD by defining value as enviornment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo."
}

### set the variables for passing secrets between TF and Gitlab here ###
### Ensure that these variables also exist in the repo level dot_gitlab-ci.yml with TF_VAR_ prefixes ###
variable "omniture_old_for_testing_username" {}
variable "omniture_old_for_testing_password" {}
