locals {

  all_db_table_name_to_s3_info_map = merge(
    module.ipv_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.global_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.lkp_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.dasp_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.cs_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.bi_red_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.sg_zod_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.sg_vmx_tables_s3_info_constructor.all_db_table_name_to_s3_info_map,
    module.cs_lkp_tables_s3_info_constructor.all_db_table_name_to_s3_info_map
  )

  requested_db_table_name_to_s3_info_map = zipmap(var.db_table_names, data.null_data_source.requested_db_table_name_to_s3_info_map.*.outputs)
}

module "ipv_tables_s3_info_constructor" {
  source = "../../ipv-db-tables-init/utils/tables-s3-info-constructor"

  env = var.env
  business_unit = var.business_unit
  stream_category = var.stream_category

  db_table_names = []
}

module "dasp_tables_s3_info_constructor" {
  source = "../../dasp-db-tables-init/utils/tables-s3-info-constructor"

  env = var.env
  business_unit = var.business_unit
  stream_category = var.stream_category

  db_table_names = []
}

module "global_tables_s3_info_constructor" {
  source = "../../global-db-tables-init/utils/tables-s3-info-constructor"

  env = var.env
  business_unit = var.business_unit
  stream_category = var.stream_category

  db_table_names = []
}

module "lkp_tables_s3_info_constructor" {
  source = "../../lkp-db-tables-init/utils/tables-s3-info-constructor"

  env = var.env
  business_unit = var.business_unit
  stream_category = var.stream_category

  db_table_names = []
}

module "bi_red_tables_s3_info_constructor" {
  source = "../../bi-red-db-tables-init/utils/tables-s3-info-constructor"

  env = var.env
  business_unit = var.business_unit
  stream_category = var.stream_category

  db_table_names = []
}

module "cs_tables_s3_info_constructor" {
  source = "../../cs-db-tables-init/utils/tables-s3-info-constructor"

  env = var.env
  business_unit = var.business_unit
  stream_category = var.stream_category

  db_table_names = []
}

module "sg_vmx_tables_s3_info_constructor" {
  source = "../../sg-vmx-db-tables-init/utils/tables-s3-info-constructor"

  env = var.env
  business_unit = var.business_unit
  stream_category = "vmx"

  db_table_names = []
}

module "sg_zod_tables_s3_info_constructor" {
  source = "../../sg-zod-db-tables-init/utils/tables-s3-info-constructor"

  env = var.env
  business_unit = var.business_unit
  stream_category = "zod"

  db_table_names = []
}

module "cs_lkp_tables_s3_info_constructor" {
  source = "../../cs-lkp-db-tables-init/utils/tables-s3-info-constructor"

  env = var.env
  business_unit = var.business_unit
  stream_category = "qtm"

  db_table_names = []
}

data "null_data_source" "requested_db_table_name_to_s3_info_map" {
  count = length(var.db_table_names)
  inputs = {
    s3_bucket_name = lookup(local.all_db_table_name_to_s3_info_map, var.db_table_names[count.index]).s3_bucket_name
    s3_table_path = lookup(local.all_db_table_name_to_s3_info_map, var.db_table_names[count.index]).s3_table_path
    s3_location = lookup(local.all_db_table_name_to_s3_info_map, var.db_table_names[count.index]).s3_location
  }
}