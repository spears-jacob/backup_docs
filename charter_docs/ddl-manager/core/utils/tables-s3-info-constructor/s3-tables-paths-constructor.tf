locals {

  all_db_table_names = keys(var.all_db_table_name_to_config_map)

  table_path_pattern = "data/%s/%s/"

  requested_db_table_name_objects = data.null_data_source.requested_db_table_name_objects.*.outputs
  all_db_table_name_objects = data.null_data_source.all_db_table_name_objects.*.outputs

  // todo discuss location
  requested_s3_tables_paths = data.null_data_source.requested_tables_paths.*.outputs.value
  all_s3_tables_paths = data.null_data_source.all_tables_paths.*.outputs.value
}

/*
  Generates list of db_table_name objects. Each object presented like "db_name: ${value}, table_name: ${value}"
  e.g.
  [
    {
      db_name: "dev"
      table_name: "quantum_daily_feature_set_agg"
    },
    {
      db_name: "tmp_dev"
      table_name: "quantum_daily_errors_set_agg"
    }
  ]
*/
data "null_data_source" "requested_db_table_name_objects" {
  count = length(var.db_table_names)
  inputs = {
    db_name = split(".", var.db_table_names[count.index])[0],
    table_name = split(".", var.db_table_names[count.index])[1]
  }
}

data "null_data_source" "all_db_table_name_objects" {
  count = length(local.all_db_table_names)
  inputs = {
    db_name = split(".", local.all_db_table_names[count.index])[0],
    table_name = split(".", local.all_db_table_names[count.index])[1]
  }
}

/*
  Generates list of tables paths for each of db_table_name_objects
  in format like "data/{db_name}/{table_name}" (e.g. data/dev/quantum_daily_feature_set_agg)
*/
data "null_data_source" "requested_tables_paths" {
  count = length(local.requested_db_table_name_objects)
  inputs = {
    # TODO temp fix to add permissions for prod environemnt read data from core_quantum_events_app_parts folder for core_quantum_events table
    value = local.requested_db_table_name_objects[count.index].db_name == "prod" && local.requested_db_table_name_objects[count.index].table_name == "core_quantum_events" ? "data/prod/core_quantum_events_app_parts/" : format(local.table_path_pattern,local.requested_db_table_name_objects[count.index].db_name,local.requested_db_table_name_objects[count.index].table_name)
  }
}
data "null_data_source" "all_tables_paths" {
  count = length(local.all_db_table_name_objects)
  inputs = {
    # TODO temp fix to add permissions for prod environemnt read data from core_quantum_events_app_parts folder for core_quantum_events table
    value = local.all_db_table_name_objects[count.index].db_name == "prod" && local.all_db_table_name_objects[count.index].table_name == "core_quantum_events" ? "data/prod/core_quantum_events_app_parts/" : format(local.table_path_pattern,local.all_db_table_name_objects[count.index].db_name,local.all_db_table_name_objects[count.index].table_name)
  }
}
