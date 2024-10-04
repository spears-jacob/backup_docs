locals {
  db_table_names = keys(var.db_table_name_to_config_map)
  ddl_paths = data.null_data_source.ddl_paths.*.outputs.value
  db_table_name_to_ddl_path = zipmap(local.db_table_names, local.ddl_paths)
}

data "null_data_source" "ddl_paths" {
  count = length(local.db_table_names)
  inputs = {
    value = lookup(var.db_table_name_to_config_map, local.db_table_names[count.index]).ddl_path
  }
}

// iterates over each ddl and run DDL if ddl sourcecode was changed or table name was changed
resource "null_resource" "deploy_table" {
  for_each = local.db_table_name_to_ddl_path

  triggers = {
    file = each.value != "" ? sha1(file(each.value)) : null
    db_table_name = each.key
    s3_location = lookup(module.tables_s3_info_constructor.requested_db_table_name_to_s3_info_map, each.key).s3_location
  }

  provisioner "local-exec" {
    # Bootstrap script called with private_ip of each node in the clutser
    command = <<EOF

      ${local.fininsh_process_failure_bash_function}
      ${local.execute_script_in_athena_bash_function}
      ${local.get_query_exection_status_bash_function}

      if [ -z "${each.value}" ]
      then
          exit 0;
      fi

      # TODO refactor
      ddl_script=$(echo $a | sed 's/$${db_name}/${lookup(local.db_names_map, each.key )}/g ; s/$${s3_location}/${replace(lookup(module.tables_s3_info_constructor.requested_db_table_name_to_s3_info_map, each.key).s3_location,"/","\\/")}/g' ${each.value})

      execute_script_in_athena "$ddl_script"
     #  Check constructions added because of problem that s3 buckets cant be created instantly

      aws s3api put-object --bucket ${lookup(module.tables_s3_info_constructor.requested_db_table_name_to_s3_info_map, each.key).s3_bucket_name} \
                           --key ${lookup(module.tables_s3_info_constructor.requested_db_table_name_to_s3_info_map, each.key).s3_table_path};

EOF
  }
}

module "tables_s3_info_constructor" {
  source = "../utils/tables-s3-info-constructor"

  business_unit = var.business_unit
  env = var.env
  flow_category = var.flow_category
  stream_category = var.stream_category

  db_table_names = local.db_table_names

  all_db_table_name_to_config_map = var.db_table_name_to_config_map
}
