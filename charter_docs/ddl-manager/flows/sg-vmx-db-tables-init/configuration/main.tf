locals {
  path_to_ddl_scripts_dir = "${path.module}/../scripts"

  tables_config_all = merge(
    local.tables_config
  )

  s3_bucket_keys = keys(local.tables_config_all)
}

module "db_table_name_to_config_map_constructor" {
  source = "../../../core/utils/db-table-name-to-config-map-constructor"
  buckey_key_to_config_map = local.tables_config_all
}