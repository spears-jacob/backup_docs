variable "business_unit" {
  description = "type of business unit (e.g. pi)"
}
variable "stream_category" {
  description = "type of stream (e.g. qtm)"
}
variable "env" {
  description = "environment name (e.g. prod, dev)"
}

variable "athena_output_s3_bucket_name" {
  description = "s3 bucket name of athena execution result"
}

variable "region" {
  description = "aws region to bucket creation (e.g. us-east-1, us-west-2 etc)"
}
variable "kms_key_arn" {
  description = "arn of kms key to bucket encryption"
}

variable "replication_region" {
  description = "aws region to replication bucket creation (e.g. us-east-1, us-west-2 etc)"
}
variable "replication_kms_key_arn" {
  description = "arn of kms key to replication bucket encryption"
}
#Cost Governance tags
variable "Org" {
  description = "Org Name (ex: digital-platforms)"
}

variable "Group" {
  description = "Group Name (ex: digital-insights)"
}

variable "Team" {
  description = "Team Name(LOB) (ex: self-service-platforms-reporting)"
}

variable "Solution" {
  description = "Actual team name  (ex: Ipvideo)"
}

variable "s3_tags" {
  type = map
}