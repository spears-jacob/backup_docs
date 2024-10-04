locals {

  // bucket_key -> s3_bucket_name map
  all_s3_buckets = module.ipv_s3_buckets_names_constructor.all_s3_bucket_names_map

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

module "ipv_s3_buckets_names_constructor" {
  source = "../utils/s3-bucket-names-constructor"

  business_unit = var.business_unit
  env = var.env
  stream_category = var.stream_category

  s3_buckets_keys = []
}

