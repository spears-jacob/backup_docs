module "tables_s3_info_constructor" {
  source = "../../../../core/utils/tables-s3-info-constructor"

  env = var.env
  business_unit = var.business_unit
  stream_category = var.stream_category
  flow_category = "global"

  all_db_table_name_to_config_map = module.tables_configuration.db_table_name_to_config_map
  db_table_names = var.db_table_names
}

module "tables_configuration" {
  source = "../../configuration"
  env = var.env
}
