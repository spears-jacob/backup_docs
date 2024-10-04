locals {

  get_query_exection_status_bash_function = <<EOF
      get_athena_query_execution_status () {

          local execution_id=$1
          local status=$(aws athena get-query-execution --query-execution-id "$execution_id" \
                  --query "QueryExecution.Status.State" \
                  --output "text"
                  )
          # if status is RUNNING to wait query completion (status should be FAILED, or SUCCEEDED)
          if [[ "$status" == "RUNNING" ]];
            then
                get_athena_query_execution_status $execution_id
            else
                echo $status
          fi
      }
  EOF
}