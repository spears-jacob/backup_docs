locals {

  tables_config = map(
    "core-events-cleansed", [
      {
        db_table_name = "${var.env}.core_quantum_events"
        ddl_path = ""
      }
    ]
  )

  s3_bucket_keys = keys(local.tables_config)

}

module "db_table_name_to_config_map_constructor" {
  source = "../../../core/utils/db-table-name-to-config-map-constructor"
  buckey_key_to_config_map = local.tables_config
}