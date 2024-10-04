terraform {
  backend "s3" {}
}

module "init_tables" {
  source = "../init-tables"

  region = var.region

  business_unit = var.business_unit
  env = var.env
  flow_category = var.flow_category
  stream_category = var.stream_category

  athena_output_s3_bucket_name = var.athena_output_s3_bucket_name
  db_table_name_to_config_map = var.db_table_name_to_config_map
}
