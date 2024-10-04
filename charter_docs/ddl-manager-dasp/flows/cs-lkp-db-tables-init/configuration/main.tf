locals {
  db_name = "${var.env}_dasp"
  # had to hardcore flow_category to 'dasp' due cs flow tables should use dasp db 
  path_to_ddl_scripts_dir = "${path.module}/../scripts"

  tables_config = map(
    "lkp", [
    {
      db_table_name = "${local.db_name}.cs_issue_cause_lookup"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_issue_cause_lookup.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_resolution_lookup"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_resolution_lookup.hql"
    },
    {
      db_table_name = "${local.db_name}.chtr_fiscal_month"
      ddl_path = "${local.path_to_ddl_scripts_dir}/chtr_fiscal_month.hql"
    },
    {
      db_table_name = "${local.db_name}.cs_dates"
      ddl_path = "${local.path_to_ddl_scripts_dir}/cs_dates.hql"
    }
  ]
  )

  s3_bucket_keys = keys(local.tables_config)
}

module "db_table_name_to_config_map_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-manager-templatedot_git//core/utils/db-table-name-to-config-map-constructor"
  buckey_key_to_config_map = local.tables_config
}
