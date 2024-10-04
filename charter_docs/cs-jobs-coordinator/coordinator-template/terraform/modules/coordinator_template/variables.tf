variable "project_tag" {}
variable "job_type" {}
variable "environment" {}
variable "region" {}
variable "aws_account_id" {}
variable "definition" {}

variable "schedule_expression" {}
variable "enable_schedule" {
  default = false
}
