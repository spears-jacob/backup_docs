/*
  Returns map of db_table_name to s3_table_info(s3_bucket_name, s3_location, table_path)
  costructed for db table names from input db_table_names value

  e.g.
  {
    "dev.quantum_dma_analysis" = {
      "s3_bucket_name" = "pi-qtm-ipv-dev-m-dma-analysis"
      "s3_location" = "s3://pi-qtm-ipv-dev-m-dma-analysis/data/dev/quantum_dma_analysis/"
      "s3_table_path" = "data/dev/quantum_dma_analysis/"
    }
    "dev.quantum_monthly_deeplink_set_agg" = {
      "s3_bucket_name" = "pi-qtm-ipv-dev-m-deeplinking-set-agg"
      "s3_location" = "s3://pi-qtm-ipv-dev-m-deeplinking-set-agg/data/dev/quantum_monthly_deeplink_set_agg/"
      "s3_table_path" = "data/dev/quantum_monthly_deeplink_set_agg/"
    }
  }
*/
output "requested_db_table_name_to_s3_info_map" {
  value = local.requested_db_table_name_to_s3_info_map
}

output "all_db_table_name_to_s3_info_map" {
  value = local.all_db_table_name_to_s3_info_map
}