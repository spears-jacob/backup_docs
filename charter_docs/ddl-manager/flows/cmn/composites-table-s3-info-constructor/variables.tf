variable "business_unit" {
  description = "type of business unit (e.g. pi)"
}
variable "stream_category" {
  description = "type of stream (e.g. qtm)"
}
variable "env" {
  description = "environment name (e.g. prod, dev)"
}
// todo add description
variable "db_table_names" {
  type = "list"
  description = ""
}