locals {

  s3_location_pattern = "s3://%s/%s"

  requested_s3_locations = formatlist(local.s3_location_pattern, local.requested_s3_bucket_names, local.requested_s3_tables_paths)
  all_s3_locations = formatlist(local.s3_location_pattern, local.all_s3_bucket_names, local.all_s3_tables_paths)
}