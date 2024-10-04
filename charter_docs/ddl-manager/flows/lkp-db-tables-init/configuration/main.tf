locals {

  path_to_ddl_scripts_dir = "${path.module}/../scripts"

  tables_config = map(
    "lkp", [
    {
      db_table_name = "${var.env}.asset_id_family_unique"
      ddl_path = "${local.path_to_ddl_scripts_dir}/asset_id_family_unique.hql"
    },
    {
      db_table_name = "${var.env}.orig_promoted_content_tms_ids"
      ddl_path = "${local.path_to_ddl_scripts_dir}/orig_promoted_content_tms_ids.hql"
    }
  ]
  )

  s3_bucket_keys = keys(local.tables_config)
}

module "db_table_name_to_config_map_constructor" {
  source = "../../../core/utils/db-table-name-to-config-map-constructor"
  buckey_key_to_config_map = local.tables_config
}
