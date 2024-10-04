#!/bin/bash
#

OUTPUT_S3_LOCATION=""
if [[ "${ENVIRONMENT}" == "dev" ]];
  then
      OUTPUT_S3_LOCATION="s3://ipv-athena-query-results-us-west-2/"
elif [[ "${ENVIRONMENT}" == "stg" ]]
  then
      OUTPUT_S3_LOCATION="s3://213705006773-us-east-1-stg-workgroup-athena-results-bucket/"
else
      OUTPUT_S3_LOCATION="s3://aws-athena-query-results-387455165365-us-east-1/"
fi

# Stop execution of execute_hql_1.sh script
finish_process_with_failure (){
    kill -s TERM ${TOP_PID};
}

# Used for execution of AWS Athena compatible scripts via aws-cli
# @param {execution_script} - AWS Athena compatible script (required)
# Note: function will stop full process with 101 exit code if smth went wrong
# (e.g. no permissions to execute query, query execution status 'FAILED')
execute_script_in_athena () {

    local execution_script=$1

    echo "# Athena script: $execution_script"
    local execution_id=$(aws athena start-query-execution \
                            --result-configuration "OutputLocation=$OUTPUT_S3_LOCATION" \
                            --query "QueryExecutionId" \
                            --output "text" \
                            --query-string "${execution_script}" || { echo "Execute Athena script failure"; finish_process_with_failure; }
    )

    echo "# Athena execution_id: $execution_id"

    local execution_status=$(get_athena_query_execution_status ${execution_id})
    echo "# Athena execution status: ${execution_status}"

    if [[ "${execution_status}" == "FAILED" ]];
    then
        finish_process_with_failure;
    fi
}

# Used for retrieving final state status (FAILED or SUCCEEDED) of AWS Athena Query via aws-cli
# @param {execution_id} - existing id of AWS Athena Query (required)
# Note: if query status is RUNNING get_athena_query_execution_status will be recursively executed
# until status be final (FAILED or SUCCEEDED)
get_athena_query_execution_status () {

    local execution_id=$1
    local status=$(aws athena get-query-execution --query-execution-id "${execution_id}" \
            --query "QueryExecution.Status.State" \
            --output "text"
            )
    # if status is RUNNING to wait query completion (status should be FAILED, or SUCCEEDED)
    if [[ "${status}" == "RUNNING" ]];
      then
          get_athena_query_execution_status ${execution_id}
      else
          echo ${status}
    fi
}

# Used for 1 drop hive view
# (todo it's temporal function, it's needed only for development, in case when someone will deploy set-agg-view job with hive views)
# @param {view_name} - name of existent view that will be dropped
# Note: function will stop full process with 101 exit code if smth went wrong
# (e.g. invalid hql script)
drop_hive_view_if_exists () {

    local view_name=$1

    echo "# Drop hive view for $view_name"

    execute_script_in_hive "drop view if exists $view_name;"
}

# Used for execution of Hive compatible scripts
# @param {hive_script} - Hive compatible script (required)
# Note: function will stop full process with 101 exit code if smth went wrong
# (e.g. invalid hql script)
execute_script_in_hive () {

    local hive_script=$1

    echo "# Executing hive script: $hive_script" >&2

    res=$(hive -e "$hive_script") || { echo "Execute Hive script failure"; finish_process_with_failure; }

    echo "$res"
}
