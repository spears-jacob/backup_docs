#tables config map in format like:
# s3_bucket_key to {
#   db_table_name
#   ddl_path
# }
variable "buckey_key_to_config_map" {
  type = "map"
}

