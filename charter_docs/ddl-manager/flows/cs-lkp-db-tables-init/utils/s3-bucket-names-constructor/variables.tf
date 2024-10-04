variable "business_unit" {
  description = "type of business unit (e.g. pi)"
}
variable "stream_category" {
  description = "type of stream (e.g. qtm)"
}
variable "env" {
  description = "environment name (e.g. prod, dev)"
}

variable "s3_buckets_keys" {
  type = "list"
  description = "list of s3 bucket keys should be mapped in to specific s3 bucket name"
}