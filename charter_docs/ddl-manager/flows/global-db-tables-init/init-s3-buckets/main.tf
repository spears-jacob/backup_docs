locals {

  // bucket_key -> s3_bucket_name map
  all_s3_buckets = module.global_s3_buckets_names_constructor.all_s3_bucket_names_map

  env_map = {
    dev  = "development"
    stg  = "stage"
    prod = "production"

  }

  common_tags_temp = {
    Env             = local.env_map[var.env]
    Org             = var.Org
    Group           = var.Group
    Team            = var.Team
    Solution        = var.Solution
    business_unit   = var.business_unit
    stream_category = var.stream_category
    flow_category   = var.flow_category
    environment     = var.env

  }

  common_tags = merge(local.common_tags_temp, var.s3_tags)
}

module "global_s3_buckets_names_constructor" {
  source = "../utils/s3-bucket-names-constructor"

  business_unit = var.business_unit
  env = var.env
  stream_category = var.stream_category

  s3_buckets_keys = []
}

// todo global buckets commented because currently they are creating through tfstate
//module "core_events_cleansed" {
//  source = "git@gitlab.spectrumxg.com:awspilot/charter-telemetry-pilot-tfmodulesdot_git//datalake_bucket?ref=v2.0-devel"
//
//  bucket_name = lookup(local.all_s3_buckets, "core-events-cleansed")
//
//  versioned = true
//  lifecycled = true
//
//  region = var.region
//  kms_key_arn = var.kms_key_arn
//
//  replication = true
//  replication_region = var.replication_region
//  replication_kms_key_arn = var.replication_kms_key_arn
//
//  tags = local.common_tags
//}
//
//
//module "core_events_ingress" {
//  source = "git@gitlab.spectrumxg.com:awspilot/charter-telemetry-pilot-tfmodulesdot_git//datalake_bucket?ref=v2.0-devel"
//
//  bucket_name = lookup(local.all_s3_buckets, "core-events-ingress")
//
//  versioned = true
//  lifecycled = true
//
//  region = var.region
//  kms_key_arn = var.kms_key_arn
//
//  replication = true
//  replication_region = var.replication_region
//  replication_kms_key_arn = var.replication_kms_key_arn
//
//  tags = local.common_tags
//}
//

//module "logs" {
//  source = "git@gitlab.spectrumxg.com:awspilot/charter-telemetry-pilot-tfmodulesdot_git//datalake_bucket?ref=v2.0-devel"
//
//  bucket_name = lookup(local.all_s3_buckets, "logs")
//
//  versioned = true
//  lifecycled = true
//
//  region = var.region
//  kms_key_arn = var.kms_key_arn
//
//  replication = true
//  replication_region = var.replication_region
//  replication_kms_key_arn = var.replication_kms_key_arn
//
//  tags = local.common_tags
//}

//module "artifacts" {
//  source = "git@gitlab.spectrumxg.com:awspilot/charter-telemetry-pilot-tfmodulesdot_git//datalake_bucket?ref=v2.0-devel"
//
//  bucket_name = lookup(local.all_s3_buckets, "artifacts")
//
//  versioned = true
//  lifecycled = true
//
//  region = var.region
//  kms_key_arn = var.kms_key_arn
//
//  replication = true
//  replication_region = var.replication_region
//  replication_kms_key_arn = var.replication_kms_key_arn
//
//  tags = local.common_tags
//}
//
