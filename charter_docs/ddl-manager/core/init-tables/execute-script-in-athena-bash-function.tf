locals {

  execute_script_in_athena_bash_function = <<EOF
      execute_script_in_athena () {

          local execution_script=$1

          local execution_id=$(aws athena start-query-execution \
                                  --result-configuration "OutputLocation=s3://${var.athena_output_s3_bucket_name}" \
                                  --query "QueryExecutionId" \
                                  --output "text" \
                                  --query-string "$execution_script" || { echo "Execute Athena script failure"; finish_process_with_failure; }
          )

          local execution_status=$(get_athena_query_execution_status $execution_id)

          if [[ "$execution_status" == "FAILED" ]];
          then
              finish_process_with_failure;
          fi
      }

  EOF
}