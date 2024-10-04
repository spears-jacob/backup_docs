#!/usr/bin/env bash
# Usage: au <AthenaOutputS3LocationSuffix>

#Athena Utilities
au () {

# check prerequisites and credentials found in common_utilities needs to be loaded already to use this script, preferably in ~/.bash_profile
c_preq
c_creds

if [ -z "$1" ]; then AthenaOutputS3LocationSuffix="pi-qtm-ssp/output/"; else AthenaOutputS3LocationSuffix="$1"; fi

account=$(aws sts get-caller-identity --output json | jq -r ".Account")

# set these two ouput groups to match what is configured in the AWS Console Athena for each given workgroup
if [ $account -eq 213705006773 ]; then
  OUTPUT_S3_LOCATION="s3://213705006773-us-east-1-stg-workgroup-athena-results-bucket/$AthenaOutputS3LocationSuffix";
  echo -e "\n\n STG Impulse Dev Account and Athena configuration\n\n";
elif [ $account -eq 387455165365 ]; then
  OUTPUT_S3_LOCATION="s3://387455165365-us-east-1-prod-workgroup-athena-results-bucket/$AthenaOutputS3LocationSuffix";
  echo -e "\n\n PROD Impulse Account and Athena configuration\n\n";
else
  echo -e "\n\nUsage:\n\nau\n\nUnknown Account and Athena configuration\n\n"; kill -INT $$;
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
  \e[36mget_athena_results\e[m  or
  \e[36mqr\e[m
"

alias xa='execute_script_in_athena '
alias qr='get_athena_results '

finish_process_with_failure (){
    echo -e '\n\n\tCheck the input Athena query, database references (must be complete), region, credentials and try agin.'
    kill -INT $$;
}

# Used for execution of AWS Athena compatible scripts via aws-cli
# @param {execution_script} - AWS Athena compatible script (required)
# Note: function will stop full process with exit code if someting went wrong
# (e.g. no permissions to execute query, query execution status 'FAILED')
execute_script_in_athena () {

    local execution_script=$1
    local submission_attempts=0

    echo -e "\n# Athena script: $execution_script\n"

    while [ ${submission_attempts} -lt 10 ]; do  #submit-query loop
      execution_id=$(aws athena start-query-execution \
                              --result-configuration "OutputLocation=$OUTPUT_S3_LOCATION" \
                              --query "QueryExecutionId" \
                              --output "text" \
                              --query-string "${execution_script}")
      if [ $? -ne 0 ]; then
        echo -e "\n\tWill try again in seven seconds\n\n";
        sleep 7;
        submission_attempts=$(expr $submission_attempts + 1)
      else submission_attempts=10;
      fi
    done  #submit-query loop

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
    if [[ ! "${status}" == "FAILED" ]] && [[ ! "${status}" == "SUCCEEDED" ]] ;
      then
          sleep 3
          echo -e "\n\t-> waiting for query to complete"
          get_athena_query_execution_status ${execution_id}
      else
          echo ${status}
    fi
}

get_athena_results (){
    IsResultCSVavailable=$(aws s3 ls $OUTPUT_S3_LOCATION$execution_id.csv)
    IsResultTXTavailable=$(aws s3 ls $OUTPUT_S3_LOCATION$execution_id.txt)

    if    [ ${#IsResultCSVavailable} -ne 0 ]; then
      qr_out=$OUTPUT_S3_LOCATION$execution_id.csv; isCSV=1; isTXT=0;
    elif [ ${#IsResultTXTavailable} -ne 0 ]; then
      qr_out=$OUTPUT_S3_LOCATION$execution_id.txt; isCSV=0; isTXT=1;
    else echo -e "\n\tPlease try again with a SELECT or SHOW query that has SUCEEDED. There is no file to read in the following location\n\t\t$OUTPUT_S3_LOCATION$execution_id.csv"; kill -INT $$;
    fi

    aws s3 ls $qr_out;
    aws s3 cp $qr_out raw_athena_query_output;

    echo -e "\n\n\n### Please see the results of the Athena Select Query below ($execution_id)\n

      To retrieve the raw output file, run the following command.                                 \e[36m
      aws s3 cp $qr_out raw_athena_query_output;

                                                                                          \e[m

    "
    if   [ $isCSV -eq 1 ]; then cat raw_athena_query_output | perl -pe 's/((?<=,)|(?<=^)),/ ,/g;' | perl -pe 's/"//g;' |column -t -s,
    elif [ $isTXT -eq 1 ]; then cat raw_athena_query_output
    fi
    rm raw_athena_query_output;
    echo -e "\n\n\n"

}

}