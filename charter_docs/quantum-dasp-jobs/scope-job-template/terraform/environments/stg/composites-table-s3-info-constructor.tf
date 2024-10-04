locals {
  all_db_table_name_to_s3_info_map = merge(
    module.global_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.dasp_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.cs_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.bi_red_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.cs_lkp_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.si_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.self_install_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
	module.iden_tables_s3_info_constructor.all_db_table_name_to_s3_info_map
  )

}

# global
module "global_tables_s3_info_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-managerdot_git//flows/global-db-tables-init/utils/tables-s3-info-constructor?ref=stg"

  env = local.env
  business_unit = var.business_unit
  stream_category = "qtm"

  db_table_names = []
}

# bi-red
module "bi_red_tables_s3_info_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-managerdot_git//flows/bi-red-db-tables-init/utils/tables-s3-info-constructor?ref=stg"

  env = local.env
  business_unit = var.business_unit
  stream_category = "qtm"

  db_table_names = []
}

# dasp
module "dasp_tables_s3_info_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-manager-daspdot_git//flows/dasp-db-tables-init/utils/tables-s3-info-constructor?ref=stg"

  env = local.env
  business_unit = var.business_unit
  stream_category = var.stream_category

  db_table_names = []
}

module "cs_lkp_tables_s3_info_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-manager-daspdot_git//flows/cs-lkp-db-tables-init/utils/tables-s3-info-constructor?ref=stg"

  env = local.env
  business_unit = var.business_unit
  stream_category = "qtm"

  db_table_names = []
}

module "cs_tables_s3_info_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-manager-daspdot_git//flows/cs-db-tables-init/utils/tables-s3-info-constructor?ref=stg"

  env = local.env
  business_unit = var.business_unit
  stream_category = var.stream_category

  db_table_names = []
}

# si
module "si_tables_s3_info_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-manager-daspdot_git//flows/si-db-tables-init/utils/tables-s3-info-constructor?ref=stg"

  env = local.env
  business_unit = var.business_unit
  stream_category = var.stream_category

  db_table_names = []
}

# si - external
module "self_install_tables_s3_info_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-manager-self-installdot_git//flows/si-db-tables-init/utils/tables-s3-info-constructor?ref=stg"

  env = local.env
  business_unit = var.business_unit
  stream_category = var.stream_category

  db_table_names = []
}

# iden
module "iden_tables_s3_info_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-manager-identitydot_git//flows/identity-db-tables-init/utils/tables-s3-info-constructor?ref=stg"

  env = local.env
  business_unit = var.business_unit
  stream_category = var.stream_category

  db_table_names = []
}
