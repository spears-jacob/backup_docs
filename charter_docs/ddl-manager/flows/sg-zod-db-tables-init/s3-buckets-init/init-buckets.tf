module "events_pii" {
  source = "git@gitlab.spectrumxg.com:awspilot/charter-telemetry-pilot-tfmodulesdot_git//datalake_bucket?ref=v2.1"

  bucket_name = lookup(local.all_s3_buckets, "events-pii")

  versioned = var.environment_class == "production" ? true : false
  lifecycled = true
  transition_days = var.environment_class == "production" ? "60" : ""
  expire_days = var.environment_class == "production" ? "395" : "90"

  region = var.region
  kms_key_arn = var.kms_key_arn

  replication = var.environment_class == "production" ? true : false
  replication_region = var.replication_region
  replication_kms_key_arn = var.replication_kms_key_arn

  tags = local.common_tags
}
