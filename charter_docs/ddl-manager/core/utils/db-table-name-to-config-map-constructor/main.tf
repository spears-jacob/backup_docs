# this module reverse table configuration to have convinient structure like:
# table_name to {
#   ddl_path
#   s3_bucket_key
# }
locals {

  s3_bucket_keys = keys(var.buckey_key_to_config_map)

  s3_bucket_key_ddl_path_configs = flatten([
    for s3_bucket_key in local.s3_bucket_keys:[
      for config in lookup(var.buckey_key_to_config_map, s3_bucket_key) : [
        {
          s3_bucket_key = s3_bucket_key
          ddl_path = config.ddl_path
        }
      ]
    ]
  ])

  db_table_names = flatten([
    for s3_bucket_key in local.s3_bucket_keys:[
      for config in lookup(var.buckey_key_to_config_map, s3_bucket_key) : [
        config.db_table_name
      ]
    ]
  ])

  db_table_name_to_config_map = zipmap(local.db_table_names, local.s3_bucket_key_ddl_path_configs)
}
