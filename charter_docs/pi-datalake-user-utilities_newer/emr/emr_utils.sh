#!/bin/bash

# Usage: source emr_utils.sh
# This script sets up the environment in an EMR

# standard EMR environment
function std_emr_env () {
  export RUN_DATE=${1}
  export ENVIRONMENT=${2}
  export GLOBAL_DB=${ENVIRONMENT}
  export DASP_db=${ENVIRONMENT}_${8}
  export TMP_db=${3}_${8}
  export ARTIFACTS_PATH=${4}
  export JOB_STEP=${5}
  if [[ "${#JOB_STEP}" -gt 9 ]]; then export JOB_STEP_NAME=${JOB_STEP:0: $(( ${#JOB_STEP} - 9 ))}; else export JOB_STEP_NAME=${JOB_STEP}; fi # deals with job steps which are actually commit hash
  export SCRIPT_VERSION=${5: -8}
  export RUN_HOUR=${6}
  export ADDITIONAL_PARAMS=${7}
  # consider adding more additional_params parsing here for common use cases, perhaps using an array
  if [[ "${ADDITIONAL_PARAMS}" != "none" ]]; then export RUN_USING=$(echo ${ADDITIONAL_PARAMS} | jq '.RUN_USING' | tr -d '"' | sed 's/null//' ); fi
  if [[ "${#RUN_USING}" -eq 0 ]]; then export RUN_USING="job_defined_engine"; fi; #ensures variable is populated
  export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`; export CLUSTER=${C_ID:2}; export pwd=`pwd`; export STEP=${pwd#*-}; export S_ID="s-${STEP}" # cluster and step do not contain j- or s- prefixes but allow for uniqueness of temp tables
  export SPARK_CONF_FILENAME=./scripts/spark-defaults.conf-${SCRIPT_VERSION}.addendums
  export LOG_CONF_FILENAME=./scripts/log4j2.properties-${SCRIPT_VERSION}.addendums
  export PROD_ACC_NO=387455165365
  export STG_ACC_NO=213705006773
  export ON_ERROR="echo 'This is the default error output, as nothing specific was passed to run_file for this step. '"
  export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys"
  export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"
  export SCALA_LIB=`ls /usr/lib/spark/jars/scala-library*`


  echo -e "\n### Execution Logging - see how execute_hql.sh and std_emr_env have been executed.
      source execute_hql-${SCRIPT_VERSION}.sh ${1} ${2} ${3} ${4} ${5} ${6} ${AP}
      std_emr_env ${1} ${2} ${3} ${4} ${5} ${6} ${AP} ${8}
  "

  echo -e "\n### Artifacts Process - Downloading ${SCRIPT_VERSION} commit-specific hql, spark, ddl, jars artifacts from S3 into hql subfolders, no clobbering (already did this with scripts)"
    #mkdir -p hql;     hdfs dfs -get ${ARTIFACTS_PATH}/hql/*${SCRIPT_VERSION}*.* ./hql
    typeset -a ARTIFACTS=("hql" "spark" "ddl" "jars") # see https://gitlab.spectrumflow.net/awspilot/job-template/-/blob/master/deploy.sh#L47-52 for definitive list of folders
  for d in "${ARTIFACTS[@]}"
  do
    arts="${ARTIFACTS_PATH}/$d/*${SCRIPT_VERSION}*.*"
    hdfs dfs -test -s ${arts} 2> /dev/null
    if [ $? -eq 0 ]; then mkdir -p $d; hdfs dfs -get ${arts} ./$d; fi
  done

  echo -e "\n### Dates process - Download shell script:  aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar . " ; aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .
  echo -e "\n### Dates process - Extract shell script and timezone lookup from jar file"; jar -xfM process_dates.jar;
  echo -e "\n### Dates process - Process dates"; source ./process_dates.sh
}

function run_file () {
  #sorts out the above array to run each step
  for si in "${!step_array[@]}" ; do if [[  "${step_array[$si]}" == "${JOB_STEP_NAME}" ]]; then fni=$((si+1));opi=$((fni+1));cti=$((opi+1));pri=$((cti+1));oei=$((pri+1));FILENAME="${step_array[$fni]}";OPERATION="${step_array[$opi]}";COMMENT="${step_array[$cti]}";POSTRUN="${step_array[$pri]}";ONERROR="${step_array[$oei]}";fi ;((si++)); done;
  if [[ "${#FILENAME}" -lt 8 ]]; then echo -e "\n Could not figure out which step to run for \${JOB_STEP_NAME}: ${JOB_STEP_NAME}"; kill -INT $$; fi

  export S_R="./scripts/spark_run-${SCRIPT_VERSION}.sh"
  if ( ([[ "${RUN_USING}" != "job_defined_engine" ]] && [[ "${RUN_USING}" == "hive" ]]) || ([[ "${RUN_USING}" == "job_defined_engine" ]] && [[ "${OPERATION}" == "hive" ]]) ); then
    echo -e "\n### Using HIVE engine....................\n${COMMENT}"; hive -S -v -f ${FILENAME} && eval ${POSTRUN} || { echo "Failure in Hive command chain with ${FILENAME}"; eval ${ONERROR}; exit 101; }
  elif ( ([[ "${RUN_USING}" != "job_defined_engine" ]] && [[ "${RUN_USING}" == "pyspark_sql" ]]) || ([[ "${RUN_USING}" == "job_defined_engine" ]] && [[ "${OPERATION}" == "pyspark_sql" ]]) ); then
    echo -e "\n### Using PySpark engine with SQL............\n${COMMENT}"; source ${S_R} && add_spark_conf ${SPARK_CONF_FILENAME} && add_log_conf ${LOG_CONF_FILENAME} && run_sql_in_pyspark ${FILENAME} && eval ${POSTRUN} || { echo "Failure in PySpark command chain with ${FILENAME}"; eval ${ONERROR}; exit 101; }
  elif [[ "${OPERATION}" == "shell" ]]; then
    echo -e "\n### Running the following in shell.......\n${FILENAME}\n${COMMENT}"; eval ${FILENAME} && eval ${POSTRUN} || { echo "Failure running the following: ${FILENAME}"; eval ${ONERROR}; exit 101; }
  else  echo -e "\n Could not figure out which \${OPERATION} to run: '${OPERATION}' -- expecting hive, spark, or shell"; exit 101;
  fi
}

# epoch time to UTC
function etu () { date --utc --date "1970-01-01 ${1} seconds"; }

# compute statistics for partition
function csfp () {
  if (! ([ $# -gt 0 ]&& [ $# -lt 4 ]) ) ; then echo -e "\n\nUsage: csfp <db.tablename> <partition_field> <partition> \n\n"; kill -INT $$; fi
  local statement="ANALYZE TABLE ${1} PARTITION(${2}='${3}') COMPUTE STATISTICS; DESCRIBE FORMATTED ${1} PARTITION(${2}='${3}');"
  echo -e "\n### Running csfp.................\n${statement}\n"
  local dep=$(hive -S -e "${statement}" 2>/dev/null);
  if [[ $? -eq 0 ]]; then
    local ttlddlt=$(echo "${dep}" | grep -E transient_lastDdlTime | perl -pe 's/\W+transient_lastDdlTime\W+(\d+)\W+/\1/g');
    if [ "${#ttlddlt}" -eq 10 ]; then nice_date=$(etu ${ttlddlt});  fi
    echo "${dep}" | grep -E 'Database|Table|numFiles|numRows|rawDataSize|totalSize|Location.+=|CreateTime|transient_lastDdlTime'  | perl -pe "s/${ttlddlt}/${nice_date}/g" ;
  fi
}


# please override this OUTPUT_S3_LOCATION as needed
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
    kill -INT $$;
}

# Used for execution of AWS Athena compatible scripts via aws-cli
# @param {execution_script} - AWS Athena compatible script (required)
# Note: function will stop full process with 101 exit code if smth went wrong
# (e.g. no permissions to execute query, query execution status 'FAILED')
execute_script_in_athena () {

    execution_script=$1

    echo "# Athena script: $execution_script"
    execution_id=$(aws athena start-query-execution \
                            --result-configuration "OutputLocation=$OUTPUT_S3_LOCATION" \
                            --query "QueryExecutionId" \
                            --output "text" \
                            --query-string "${execution_script}" || { echo "Execute Athena script failure"; finish_process_with_failure; }
    )

    echo "# Athena execution_id: $execution_id"

    execution_status=$(get_athena_query_execution_status ${execution_id})
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

    execution_id=$1
    status=$(aws athena get-query-execution --query-execution-id "${execution_id}" \
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

spark_prep () {
  # sources the spark-env
  source /usr/lib/spark/conf/spark-env.sh;

  # check to see if the prep steps have been run
  export isPrepared="/tmp/spark_run_prepation_scripts_have_already_been_executed"
  if (! [[ -f "${isPrepared}" ]] ) ; then echo -e "\n\n### This Cluster is being prepared to run Spark.\n\n";
    # Updates all packages -- this may not be necessary, but does update the environment to the latest versions available
    export PATH="/usr/local/bin:$PATH"
    sudo yum install deltarpm -q -y; sudo yum update -q -y;

    # deals with the following error: "WARN HiveConf: HiveConf of name hive.server2.thrift.url does not exist"
    sudo yum install -y -q https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm;
    sudo yum install -y -q xmlstarlet;
    sudo xmlstarlet ed -L -d  '//name[text()="hive.server2.thrift.url"]' /usr/lib/spark/conf/hive-site.xml

    # deals with the following error: "WARN CredentialsLegacyConfigLocationProvider: Found the legacy config profiles file at [/home/hadoop/.aws/config]. Please move it to the latest default location [~/.aws/credentials]."
    if (! [[ -f "${HOME}/.aws/credentials" ]] ); then cp ${HOME}/.aws/config ${HOME}/.aws/credentials; fi

    # deals with the following error: "WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME." #https://stackoverflow.com/questions/41112801/property-spark-yarn-jars-how-to-deal-with-it
    jar c0f spark-libs.jar -C $SPARK_HOME/jars/ .; hdfspath="hdfs://$(hostname).ec2.internal:8020/"; hdfs dfs -mkdir ${hdfspath}/tmp/sparks/; hdfs dfs -put spark-libs.jar ${hdfspath}/tmp/sparks/ ; echo "spark.yarn.archive  ${hdfspath}/tmp/sparks/spark-libs.jar" | sudo tee -a /usr/lib/spark/conf/spark-defaults.conf;

    # prepares a file that establishes that this set of preparatory statements has been run already (such as in a shared cluster or multiple-step job)
    touch "${isPrepared}"
  fi
}

# returns 0 if a match is true using grep along with perl regular expression syntax, anchored
function ismatch_perl_re() {
  if [[ $# -ne 2 ]]  ; then echo -e "\n\nUsage: ismatch_perl_re <input_string> <Perl-style Regex to Match> \n\n"; kill -INT $$; fi
    echo "${1}" | grep -qoP "^${2}$";
}

function truncate_tables() {
  #Deference to Aramis Martinez and Andrei Matveenko for preparing this.
  #input array with list of tables to truncate
  TRUNCATE_TABLES=($@)
  if [ -n "${TRUNCATE_TABLES}" ]
  then
      for table in "${TRUNCATE_TABLES[@]}"
      do
          echo "Checking for ${table}"
          db="`echo ${table} | cut -d '.' -f1`"
          db_table="`echo ${table} | cut -d '.' -f2`"
          partition_name="`aws glue get-table --database-name "${db}" --name "${db_table}" | jq -r '.Table.PartitionKeys' | jq -r '.[0].Name'`"
          if [ -z "${db}" ] || [ -z "${db_table}" ]
          then
              echo "Unable to parse db and table name from ${table}"
              continue
          fi
          echo "DB: ${db}"
          echo "Table: ${db_table}"
          echo "Partition: ${partition_name}"
          S3_LOCATION=`aws glue get-table --database-name "${db}" --name "${db_table}" | jq -r '.Table.StorageDescriptor.Location'`
          if [ -n "${S3_LOCATION}" ]
          then
              echo "${table} location: ${S3_LOCATION}"
              case "${S3_LOCATION}" in
                  s3://*)
                      echo "Truncating ${S3_LOCATION}"
                      echo "Removing S3 data."
                      TMP_DIR=`mktemp -d`
                      touch "${TMP_DIR}"/.truncated
                      if ! ( aws s3 rm --recursive "${S3_LOCATION}" && aws s3 cp "${TMP_DIR}"/.truncated "${S3_LOCATION}"/.truncated )
                      then
                          echo "Failed to remove S3 data."
                          continue
                      fi
                      echo "Deleting partitions from metastore."
                      if ! hive -e "ALTER TABLE \`${db}\`.\`${db_table}\` DROP IF EXISTS PARTITION (\`${partition_name}\`>='1901-01-01') PURGE;"
                      then
                          echo "Failed to delete partitions."
                          continue
                      fi
                      echo "Partitions updated."
                      ;;
                  *) echo "${table} location not an S3 URL: ${S3_LOCATION}" ;;
              esac
          else
              echo "${table} location not found."
          fi
      done
  fi
}

send_email() {
  # assumes std_env is loaded, based on code from identity team

  # parameters:
  # 1st is send to email address
  # 2nd is run date
  # 3rd is job name
  # 4th is type of E-mail: SUCCESS, FAILURE, ERROR, INFO
  # 5th is optional message

  to_email=$1
  run_date=$2
  jobname=$3
  type=$4
  msg=$5
  CLUSTER_URI="For cluster details, go to the ${ENVIRONMENT} environment: https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:${C_ID} "

  # Ensures that there is at least four inputs but not more than five inputs from the script execution
  if (! ([ $# -gt 3 ] && [ $# -lt 6 ]) ) ; then echo -e "\n\nUsage: send_email <to_email> <run_date> <job_name> <type=SUCCESS, FAILURE, ERROR, or INFO> <message - optional> \nexample: send_email \"user@charter.com\" \$RUN_DATE \"\$JOB_NAME\" SUCCESS \"Relish success ,  🦆\"\n";exit 1000101010; fi

  if   [ "$type" == "SUCCESS" ]; then
    subject="AWS $jobname Job Status: Success for $run_date"
    body="<br/><br/> Attached please find Success for job $jobname.\
              <br/><br/> Success: <br/>  "
  elif [ "$type" == "FAILURE" ]; then
    subject="AWS $jobname Job Status: Failure for $run_date"
    body="<br/><br/> Attached please find issues for job $jobname.\
              <br/><br/> Failure: <br/>  "
  elif [ "$type" == "ERROR" ]; then
    subject="AWS $jobname Job Status: Error for $run_date"
    body="<br/><br/> Attached please find issues for job $jobname.\
              <br/><br/> Error: <br/> "
  elif [ "$type" == "INFO" ]; then
    subject="AWS $jobname: Job Information for $run_date"
    body="<br/><br/> Attached please find information for job $jobname.\
              <br/><br/> Information: <br/> "
  else
    echo -e "\n\nCannot understand which type of E-mail to send ($type), \nExpecting one of SUCCESS, FAILURE, ERROR, INFO\n so exiting \n\n"; exit 100100110;
  fi

  echo "{
      \"Subject\": {
          \"Data\": \"$subject\",
          \"Charset\": \"UTF-8\"
      },
      \"Body\": {
          \"Html\": {
              \"Data\": \"$body      \
              <ul>                   \
              <li>Job Name: $jobname</li>      \
              <li>Run Date: $run_date</li>     \
              <li>$msg</li>          \
              <li>$CLUSTER_URI</li> \",
            \"Charset\": \"UTF-8\"
          }
      }
  }" > message.json


  echo "aws ses send-email --from PI.Tableau@charter.com --destination ToAddresses=$to_email --message file://message.json --region us-east-1"
  aws ses send-email --from PI.Tableau@charter.com --destination ToAddresses=$to_email --message file://message.json --region us-east-1

}
