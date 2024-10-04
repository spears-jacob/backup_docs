locals {
  all_db_tables_names = keys(var.all_db_table_name_to_config_map)

  requested_s3_bucket_names = module.s3_bucket_names_constructor.constructed_s3_buckets_names
  all_s3_bucket_names = module.s3_bucket_names_constructor.all_s3_buckets_names
}

// iterates over for each element in var.db_table_names_to_construct and extract first corresponding value
// from db_table_name_to_bucket_key_map
data "null_data_source" "requested_bucket_keys" {
  count = length(var.db_table_names)
  inputs = {
    value = lookup(var.all_db_table_name_to_config_map, var.db_table_names[count.index]).s3_bucket_key
  }
}

data "null_data_source" "all_bucket_keys" {
  count = length(local.all_db_tables_names)
  inputs = {
    value = lookup(var.all_db_table_name_to_config_map, local.all_db_tables_names[count.index]).s3_bucket_key
  }
}

module "s3_bucket_names_constructor" {
  source = "../s3-bucket-names-constructor"

  business_unit = var.business_unit
  env = var.env
  flow_category = var.flow_category
  stream_category = var.stream_category

  s3_buckets_keys = data.null_data_source.requested_bucket_keys.*.outputs.value
  all_s3_bucket_keys = data.null_data_source.all_bucket_keys.*.outputs.value
}