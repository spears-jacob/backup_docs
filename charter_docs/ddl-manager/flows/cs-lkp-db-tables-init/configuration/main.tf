locals {

  path_to_ddl_scripts_dir = "${path.module}/../scripts"

  tables_config = map(
    "lkp", [
    {
      db_table_name = "${var.env}.cs_issue_cause_lookup"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_issue_cause_lookup.hql"
    },
    {
      db_table_name = "${var.env}.cs_resolution_lookup"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_resolution_lookup.hql"
    },
    {
      db_table_name = "${var.env}.chtr_fiscal_month"
      ddl_path = "${local.path_to_ddl_scripts_dir}/chtr_fiscal_month.hql"
    },
    {
      db_table_name = "${var.env}.cs_dates"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_dates.hql"
    }
  ]
  )

  s3_bucket_keys = keys(local.tables_config)
}

module "db_table_name_to_config_map_constructor" {
  source = "../../../core/utils/db-table-name-to-config-map-constructor"
  buckey_key_to_config_map = local.tables_config
}
