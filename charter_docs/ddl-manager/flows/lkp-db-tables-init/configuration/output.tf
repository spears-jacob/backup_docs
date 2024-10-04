/*
  Returns map of db_table_name to its cconfiguration(s3_bucket_key, ddl_path)

  e.g.
  {
    "dev.quantum_dma_analysis" = {
      "s3_bucket_key" = "m-dma-analysis"
      "ddl_path" = "../../../dma_analysis.hql"
    }
  }
*/
output "db_table_name_to_config_map" {
  value = module.db_table_name_to_config_map_constructor.db_table_name_to_config_map
}

/*
  Returns list of all s3 bucket keys

  e.g. ["d-views", "d-acct-agg" etc.]
*/
output "all_tables_s3_bucket_keys" {
  value = local.s3_bucket_keys
}
