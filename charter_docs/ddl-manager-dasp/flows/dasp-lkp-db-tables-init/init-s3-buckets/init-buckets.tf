module "lkp" {
  source = "git_at_company_dot_net:awspilot/charter-telemetry-pilot-tfmodulesdot_git//datalake_bucket?ref=v4.7.1"

  bucket_name = lookup(local.all_s3_buckets, "lkp")

  versioned = var.environment_class == "production" ? true : false
  lifecycled = false
  transition_days = var.environment_class == "production" ? "730" : ""
  expire_days = var.environment_class == "production" ? "1095" : "90"

  region = var.region
  kms_key_arn = var.kms_key_arn

  replication = var.environment_class == "production" ? true : false
  replication_region = var.replication_region
  replication_kms_key_arn = var.replication_kms_key_arn

  tags = local.common_tags
}
