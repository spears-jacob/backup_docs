#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export SEC_db=${ENVIRONMENT}_sec_repo_sspp
export ARTIFACTS_PATH=$4
export JOB_STEP=$5
export JOB_STEP_NAME=$(echo $JOB_STEP | cut -d"-" -f 1)
export SCRIPT_VERSION=$(echo $JOB_STEP | cut -d"-" -f 2 | cut -d"." -f 1)
export RUN_HOUR=$6
export ADDITIONAL_PARAMS=$7
export CADENCE=daily

export RUN_INIT=$(echo ${ADDITIONAL_PARAMS} | jq '.RUN_INIT' | tr -d '"' | sed "s/null//" )
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

function dtecho() {
        echo -e "\n\n`date '+%F %H:%M:%S'` $@"
}

dtecho "### Pulling down UDF cipher keys..."
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

dtecho "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts
hdfs dfs -get ${ARTIFACTS_PATH}/hql ./hql


if [ "$RUN_INIT" = "Yes" ]; then
   decho "### Creating sec tables with hive..."
   source ./scripts/create_sec_tables-${SCRIPT_VERSION}.sh
fi;

#############  Get unique IDs that will prevent tablename conflicts ############
export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
# The step id needs to be divided from the pwd directory using the below approach
# because the length of the step identifier varies.
export pwd=`pwd`
export STEP=${pwd#*-}
dtecho "
  EMR CLUSTER ID:    $CLUSTER (not including the J- prefix)
  EMR STEP ID:       $STEP (not including the S- prefix)
  JOB STEP:          ${JOB_STEP}
  JOB STEP NAME:     ${JOB_STEP_NAME}
  SCRIPT_VERSION:    ${SCRIPT_VERSION}
  RUN_HOUR:          ${RUN_HOUR}
  ADDITIONAL_PARAMS: ${ADDITIONAL_PARAMS}
  "
dtecho -e "\n\n### Execution is occurring the following directory:  $pwd\n\n"

#echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

dtecho "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

dtecho "### Dates process - Process dates"
source process_dates.sh

export ROLLING_03_START_DATE=$(date -d "$START_DATE - 3 day" +%F)
export ROLLING_09_START_DATE=$(date -d "$START_DATE - 9 day" +%F)
export ROLLING_62_START_DATE=$(date -d "$START_DATE - 62 day" +%F)

dtecho "### Loading Athena utils "
source scripts/util-${SCRIPT_VERSION}.sh || { echo "shell scripting failure"; exit 101; }

if [ ${ENVIRONMENT} != 'prod' ] ; then  echo -e "\n\tThis script ran successfully in stg.  It only performs file processing in production;"; exit 0; fi

if   [ "${JOB_STEP_NAME}" == "1_sent_messages_process_incoming_files" ]; then
  dtecho "### Running Sent-Messages initial processing step that separates two different json schemas into different text json files  ..................."
  source scripts/separate_files_into_like_jsons-${SCRIPT_VERSION}.sh || { echo "shell scripting failure"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "2_ingest_skycreek_epid" ]; then
  dtecho "### Running Sent-Messages Skycreek EPID ingest  ..................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/2_ingest_skycreek_epid-${SCRIPT_VERSION}.hql || { echo "HQL Failure ${JOB_STEP_NAME}"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "3_ingest_ucchub_hp" ]; then
  dtecho "### Running Sent-Messages UCC-HUB HP ingest sparky ..................."
  source /usr/lib/spark/conf/spark-env.sh; sudo python3 -m pip -q -q -q install pyspark
  function spark_run {
    # replaces ALL ${env:var} hive-style environment variable references with the value of the variable in the shell with the same name
    readarray -t env_array < <( compgen -v ); for envar in "${env_array[@]}"; do sed -i "s/\\\${env:$envar}/${!envar}/g" "$1" 2>/dev/null; done
    dtecho "Spark file to be executed: $1"; cat $1;
    spark-submit ./scripts/spark_sql3-${SCRIPT_VERSION}.py --sql $1 || { echo "PySpark SQL Failure ${JOB_STEP_NAME}"; exit 101; }
    if [[ $? -ne 0 ]] ; then dtecho "spark job failed" ; exit 101 ; else dtecho "Pyspark SQL ran successfully" ; fi
  return
  }
  spark_run ./hql/3_ingest_ucc_hub_hp-${SCRIPT_VERSION}.spark || { echo "Spark SQL Failure ${JOB_STEP_NAME}"; exit 101; }
  dtecho "### Completed 63-day processing of Sent-Messages UCC-HUB HP ..................."
elif [ "${JOB_STEP_NAME}" == "4_ingest_skycreek_epid" ]; then
  dtecho "### Running Skycreek PM Notifications epid ..................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/4_skycreek_pm_notifications-${SCRIPT_VERSION}.hql || { echo "HQL Failure ${JOB_STEP_NAME}"; exit 101; }
fi
