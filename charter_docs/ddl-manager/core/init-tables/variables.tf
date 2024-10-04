variable "athena_output_s3_bucket_name" {}

variable "business_unit" {
  description = "type of business unit (e.g. pi)"
}
variable "stream_category" {
  description = "type of stream (e.g. qtm)"
}
variable "flow_category" {
  description = "type of flow (e.g. ipv, dasp)"
}
variable "env" {
  description = "environment name (e.g. prod, dev)"
}

variable "region" {
  description = "aws region to bucket creation (e.g. us-east-1, us-west-2 etc)"
}

variable "db_table_name_to_config_map" {
  type = "map"
  description = "map of db_table_name to configuration objects(s3_bucket_key, ddl_path)"
}