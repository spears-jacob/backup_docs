output "db_table_name_to_config_map" {
  value = module.db_table_name_to_config_map_constructor.db_table_name_to_config_map
}

output "all_tables_s3_bucket_keys" {
  value = local.s3_bucket_keys
}
