/*
  Returns map of db_table_name to its configuration(s3_bucket_key, ddl_path)

  e.g.
  {
    "dev.quantum_dma_analysis" = {
      "s3_bucket_key" = "m-dma-analysis"
      "ddl_path" = "../../../dma_analysis.hql"
    }
  }
*/
output "db_table_name_to_config_map" {
  value = local.db_table_name_to_config_map
}

