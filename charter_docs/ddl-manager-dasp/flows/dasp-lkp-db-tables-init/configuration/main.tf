locals {
  db_name = "${var.env}_dasp"
  path_to_ddl_scripts_dir = "${path.module}/../scripts"

  tables_config = map(
    "lkp", [
    {
      db_table_name = "${local.db_name}.sps_error_mapping"
      ddl_path = "${local.path_to_ddl_scripts_dir}/sps_error_mapping.hql"
    }
  ]
  )

  s3_bucket_keys = keys(local.tables_config)
}

module "db_table_name_to_config_map_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-manager-templatedot_git//core/utils/db-table-name-to-config-map-constructor"
  buckey_key_to_config_map = local.tables_config
}
