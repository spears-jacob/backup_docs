variable "business_unit" {
  description = "type of business unit (e.g. pi)"
}
variable "stream_category" {
  description = "type of stream (e.g. qtm)"
}
variable "flow_category" {
  description = "type of flow (e.g. ipv, dasp)"
}
variable "env" {
  description = "environment name (e.g. prod, dev)"
}

variable "db_table_names" {
  type = "list"
  description = <<EOT
    list of db table names for which need to construct s3 tables info
    (e.g. [nifi.quantum_appletv_device_sales, dev.core_quantum_events])
    Note: Each element of db_table_names_to_construct should be presented like "{db_name}.{table_name}"
  EOT
}

variable "db_table_name_to_config_map" {
  type = "map"
  description = "map of db_table_name to its configuration (ddl_path, bucket_key)"
}
