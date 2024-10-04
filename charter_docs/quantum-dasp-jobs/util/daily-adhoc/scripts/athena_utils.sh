#!/bin/bash

#test for modern bash version
if [ -z "${BASH_VERSINFO}" ] || [ -z "${BASH_VERSINFO[0]}" ] || [ ${BASH_VERSINFO[0]} -lt 4 ]; then echo -e "\n\n\tThis script requires Bash version >= 4 \n\n\tPlease see https://dev.to/emcain/how-to-change-bash-versions-on-mac-with-homebrew-20o3\n\n"; kill -INT $$; fi


aws sts get-caller-identity
# evaluate error code;  giving 0 on success and 255 if you have no credentials.
if [ $? -ne 0 ]; then echo -e "\n\tPlease try again with valid credentials and region"; kill -INT $$; fi

account=$(aws sts get-caller-identity | jq -r ".Account")

# checks that there is a valid region specified, default to us-east-1
reg_config=$(aws configure get region)
if [ ${#reg_config} -eq 0 ] && [ ${#AWS_DEFAULT_REGION} -eq 0 ] ; then export AWS_DEFAULT_REGION="us-east-1"; fi

# set these two ouput groups to match what is configured in the AWS Console Athena for each given workgroup
if [ $account -eq 213705006773 ]; then
  OUTPUT_S3_LOCATION="s3://213705006773-us-east-1-stg-workgroup-athena-results-bucket/pi-qtm-ssp/output/";
  echo -e "\n\n STG Impulse Dev Account and Athena configuration\n\n";
elif [ $account -eq 387455165365 ]; then
  OUTPUT_S3_LOCATION="s3://387455165365-us-east-1-prod-workgroup-athena-results-bucket/pi-qtm-ssp/output/";
  echo -e "\n\n PROD Impulse Account and Athena configuration\n\n";
else
  echo -e "\n\nUsage:\n\nsource athena_utils.sh\n\nUnknown Account and Athena configuration\n\n"; kill -INT $$;
fi

echo -e "### Evaluating credentials and output location to ensure they will work\n"
test -w .; if [ $? -ne 0 ]; then  echo -e "\n\tPlease try again with valid credentials and output location configuration.  Writing to the local file system, (`pwd`), failed."; kill -INT $$; fi
echo 'test file for writing' > test.write;
outfile="$OUTPUT_S3_LOCATION""test.write";
aws s3 cp test.write $OUTPUT_S3_LOCATION
if [ $? -ne 0 ]; then echo -e "\n\tPlease try again with valid credentials and output location configuration.  Writing to $OUTPUT_S3_LOCATION failed."; kill -INT $$; fi
aws s3 ls $outfile
if [ $? -ne 0 ]; then echo -e "\n\tPlease try again with valid credentials and output location configuration.  Listing files in $OUTPUT_S3_LOCATION failed."; kill -INT $$; fi
rm test.write
aws s3 cp $outfile .
if [ $? -ne 0 ]; then echo -e "\n\tPlease try again with valid credentials and output location configuration.  Pulling files from $OUTPUT_S3_LOCATION to the local file system failed."; kill -INT $$; fi
rm test.write

echo -e "\e[33m

### Use the following functions in the terminal to query and retrieve the results,
    one query at a time, with all databases specified, just like in the Athena console.            \e[m

To run a query from the command line:
  \e[36mexecute_script_in_athena \"<query>\"\e[m  or
  \e[36mxa \"<query>\"\e[m

To run a query from a file:
  \e[36mexecute_script_in_athena \" \$(cat <path_to_query_file>) \"\e[m  or
  \e[36mxa \" \$(cat <path_to_query_file>) \"\e[m

To retrieve the results from the most recent select query:
  \e[36mget_athena_select_query_results\e[m  or
  \e[36mqr\e[m
"

alias xa='execute_script_in_athena '
alias qr='get_athena_select_query_results '

finish_process_with_failure (){
    echo -e '\n\n\tCheck the input Athena query, region, credentials and try agin.'
    kill -INT $$;
}

# Used for execution of AWS Athena compatible scripts via aws-cli
# @param {execution_script} - AWS Athena compatible script (required)
# Note: function will stop full process with exit code if someting went wrong
# (e.g. no permissions to execute query, query execution status 'FAILED')
execute_script_in_athena () {

    local execution_script=$1

    echo "# Athena script: $execution_script"
    execution_id=$(aws athena start-query-execution \
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
          sleep 3
          get_athena_query_execution_status ${execution_id}
      else
          echo ${status}
    fi
}

get_athena_select_query_results (){
    IsResultCSVavailable=$(aws s3 ls $OUTPUT_S3_LOCATION$execution_id.csv)
    if [ ${#IsResultCSVavailable} -eq 0 ]; then echo -e "\n\tPlease try again with a SELECT query that has SUCEEDED. There is no file to read in the following location\n\t\t$OUTPUT_S3_LOCATION$execution_id.csv"; kill -INT $$; fi

    aws s3 ls $OUTPUT_S3_LOCATION$execution_id.csv;
    aws s3 cp $OUTPUT_S3_LOCATION$execution_id.csv raw_athena_query_output;
    echo -e "\n### Please see the results of the Athena Select Query below ($execution_id)\n

      To retrieve the raw CSV, run the following command.                                 \e[36m
      aws s3 cp $OUTPUT_S3_LOCATION$execution_id.csv raw_athena_query_output;

                                                                                          \e[m

    "
    cat raw_athena_query_output | perl -pe 's/((?<=,)|(?<=^)),/ ,/g;' | perl -pe 's/"//g;' |column -t -s,
    rm raw_athena_query_output;
}
