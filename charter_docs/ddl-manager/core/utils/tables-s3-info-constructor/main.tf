locals {

  // map of db_table_name to s3_table_info(s3_bucket_name, s3_location, table_path)
  requested_db_table_name_to_s3_info_map = zipmap(var.db_table_names, data.null_data_source.requested_s3_tables_info_list.*.outputs)

  all_db_table_name_to_s3_info_map = zipmap(keys(var.all_db_table_name_to_config_map), data.null_data_source.all_s3_tables_info_list.*.outputs)
}

data "null_data_source" "requested_s3_tables_info_list" {
  count = length(var.db_table_names)
  inputs = {
    s3_bucket_name = local.requested_s3_bucket_names[count.index]
    s3_table_path = local.requested_s3_tables_paths[count.index]
    s3_location = local.requested_s3_locations[count.index]
  }
}

data "null_data_source" "all_s3_tables_info_list" {
  count = length(keys(var.all_db_table_name_to_config_map))
  inputs = {
    s3_bucket_name = local.all_s3_bucket_names[count.index]
    s3_table_path = local.all_s3_tables_paths[count.index]
    s3_location = local.all_s3_locations[count.index]
  }
}
