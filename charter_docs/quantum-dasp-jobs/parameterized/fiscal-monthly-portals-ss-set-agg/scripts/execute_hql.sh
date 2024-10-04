#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export JOB_STEP=$5
export JOB_STEP_NAME=$(echo $5 | cut -d"-" -f 1)
export SCRIPT_VERSION=$(echo $5 | cut -d"-" -f 2 | cut -d"." -f 1)
export CADENCE=fiscal_monthly
export ADDITIONAL_PARAMS=$7
#echo "Additional params: $ADDITIONAL_PARAMS"

############  Set table names #################################################
export  OUTPUT_SETAGG_TABLE=${DASP_db}.quantum_set_agg_portals
export  INPUT_METRICAGG_TABLE=${DASP_db}.quantum_metric_agg_portals


#############  Get unique IDs that will prevent tablename conflicts ############
export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
 EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"
export exec_id=$CLUSTER

export pwd=`pwd`
echo -e "\n\n### Execution is occurring the following directory:  $pwd\n\n"

# The step needs to be divided from the pwd directory using the below approach
# because the length of the step identifier varies.
export STEP=${pwd#*-}
echo "
  EMR STEP ID: $STEP (not including the S- prefix)
"

############ Do different things depending on which step we're looking at ###########
if [ "${JOB_STEP_NAME}" == "2_portals_ss_set_agg_call_enrich" ]; then
  check_date=$(date -d"$RUN_DATE - 9 days" +%d)
  echo "The last date modified was the $check_date day of the month"

  if [ "$check_date" = "28" ]; then
    export RUN_DATE=$(date -d"$RUN_DATE - 9 days" +%Y-%m-%d)
    echo "New month finalized: running for fiscal $(date -d"$RUN_DATE" +%B)"
  else
    echo "No new fiscal month has finalized; no need to re-run call enrichment"
    exit 0;
  fi
else
  echo "Running for $RUN_DATE"
fi

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
 hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts
echo "### Dates process - Download shell script"
 aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .
echo "### Dates process - Extract shell script and timezone lookup from jar file"
 jar -xfM process_dates.jar
echo "### Dates process - Process dates"
 source process_dates.sh
echo "Start of steps running..."
echo TIME_ZONE: $TZ
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;
echo label_date_denver: $LABEL_DATE_DENVER;
echo grain: $GRAIN;
echo ProcessTimestamp: $processing_started_date_time_denver
echo ProcessUser: $processing_started_by_user
echo exec_id: $exec_id
echo step_id: $STEP
echo reading from: $INPUT_METRICAGG_TABLE
echo writing to: $OUTPUT_SETAGG_TABLE

echo "### Running Portals Set Agg tableset...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_selfservice_set_agg_tableset-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
 -hiveconf ProcessUser="${processing_started_by_user}" \
 -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
 -hiveconf inputmetricaggtable="${INPUT_METRICAGG_TABLE}" -hiveconf outputsetaggtable="${OUTPUT_SETAGG_TABLE}" \
  || { echo "HQL Failure"; exit 101; }

echo "### Running Portals Set Agg accounts...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_selfservice_set_agg_accounts-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
 -hiveconf ProcessUser="${processing_started_by_user}" \
 -hiveconf execid="${exec_id}"  -hiveconf stepid="${STEP}" \
 -hiveconf inputmetricaggtable="${INPUT_METRICAGG_TABLE}" -hiveconf outputsetaggtable="${OUTPUT_SETAGG_TABLE}" \
  || { echo "HQL Failure"; exit 101; }

echo "### Running Portals Set Agg devices...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_selfservice_set_agg_devices-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
 -hiveconf ProcessUser="${processing_started_by_user}" \
 -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}"  \
 -hiveconf inputmetricaggtable="${INPUT_METRICAGG_TABLE}" -hiveconf outputsetaggtable="${OUTPUT_SETAGG_TABLE}" \
  || { echo "HQL Failure"; exit 101; }

echo "### Running Portals Set Agg instances...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_selfservice_set_agg_instances-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
 -hiveconf ProcessUser="${processing_started_by_user}" \
 -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}"  \
 -hiveconf inputmetricaggtable="${INPUT_METRICAGG_TABLE}" -hiveconf outputsetaggtable="${OUTPUT_SETAGG_TABLE}" \
  || { echo "HQL Failure"; exit 101; }

echo "### Running Portals Set Agg visits...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_selfservice_set_agg_visits-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
 -hiveconf ProcessUser="${processing_started_by_user}" \
 -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
 -hiveconf inputmetricaggtable="${INPUT_METRICAGG_TABLE}" -hiveconf outputsetaggtable="${OUTPUT_SETAGG_TABLE}" \
  || { echo "HQL Failure"; exit 101; }

echo "### Running Portals Set Agg final...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_selfservice_set_agg_final-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
 -hiveconf ProcessUser="${processing_started_by_user}" \
 -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}"  \
 -hiveconf inputmetricaggtable="${INPUT_METRICAGG_TABLE}" -hiveconf outputsetaggtable="${OUTPUT_SETAGG_TABLE}" \
  || { echo "HQL Failure"; exit 101; }
