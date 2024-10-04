locals {

  db_names_map = zipmap(local.db_table_names, data.null_data_source.db_names.*.outputs.value )
}


data "null_data_source" "db_names" {
  count = length(local.db_table_names)

  inputs = {
    value = split(".", local.db_table_names[count.index])[0]
  }
}
