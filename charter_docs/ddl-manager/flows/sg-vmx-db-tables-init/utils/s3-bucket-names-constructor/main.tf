module "tables_configuration" {
  source = "../../configuration"
  env = var.env
}

module "s3_bucket_names_constructor" {
  source = "../../../../core/utils/s3-bucket-names-constructor"

  all_s3_bucket_keys = module.tables_configuration.all_tables_s3_bucket_keys

  s3_buckets_keys = var.s3_buckets_keys

  env = var.env
  business_unit = var.business_unit
  flow_category = "sg"
  stream_category = var.stream_category
}