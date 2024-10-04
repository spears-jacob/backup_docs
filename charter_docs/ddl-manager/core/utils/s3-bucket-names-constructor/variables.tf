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

variable "s3_buckets_keys" {
  type = "list"
  description = "list of s3 bucket keys should be mapped in to specific s3 bucket name"
}
// todo add descriptions
variable "all_s3_bucket_keys" {
  type = "list"
  description = "list of all s3 bucket keys"
}
