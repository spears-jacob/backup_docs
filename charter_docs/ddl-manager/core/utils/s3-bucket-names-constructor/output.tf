// returns list of constructed s3_bucket names
output "constructed_s3_buckets_names" {
  value = local.constructed_s3_buckets_names
}

// returns list of constructed s3_bucket names
output "all_s3_buckets_names" {
  value = local.all_s3_buckets_names
}

// returns map of bucket-key to s3-bucket-name
output "all_s3_bucket_names_map" {
  value = local.all_s3_bucket_names_map
}