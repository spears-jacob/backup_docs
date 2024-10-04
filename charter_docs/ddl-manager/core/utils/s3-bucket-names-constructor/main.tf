locals {
  bucket_name_template = "${var.business_unit}-${var.stream_category}-${var.flow_category}-${var.env}-%s"

  // some of bucket keys doesn't presented into tables_configuration, because they are not related with it
  all_s3_bucket_keys = var.all_s3_bucket_keys

  // list of bucket keys
  constructed_s3_buckets_names = formatlist(local.bucket_name_template, var.s3_buckets_keys)
  all_s3_buckets_names = formatlist(local.bucket_name_template, local.all_s3_bucket_keys)

  // map of bucket-key to s3-bucket-name
  all_s3_bucket_names_map = zipmap(local.all_s3_bucket_keys, local.all_s3_buckets_names)
}

// validates that var.s3_buckets_keys exsists in all_s3_bucket_keys
// Note: if one of the keys from ${var.s3_buckets_keys} doesn't exist in all_s3_bucket_keys terraform will fail
data "null_data_source" "validate_input_params" {
  count = length(var.s3_buckets_keys)
  inputs = {
    value = index(local.all_s3_bucket_keys, var.s3_buckets_keys[count.index])
  }
}