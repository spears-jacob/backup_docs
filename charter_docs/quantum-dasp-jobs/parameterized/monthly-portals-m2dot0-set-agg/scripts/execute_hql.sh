#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export CADENCE="monthly"
export JOB_STEP_NAME=$(echo $5 | cut -d"-" -f 1)
export SCRIPT_VERSION=$(echo $5 | cut -d"-" -f 2 | cut -d"." -f 1)
export RUN_HOUR=$6
export ADDITIONAL_PARAMS=$7
export isReprocessing=0


hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

if [ "${JOB_STEP_NAME}" == "2_tableau_refresh" ]; then
#	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
  echo "Skip tableau refresh"
else
  echo "### Dates process - Download shell script"
  aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

  echo "### Dates process - Extract shell script and timezone lookup from jar file"
  jar -xfM process_dates.jar

  echo "### Dates process - Process dates"
  source process_dates.sh


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


  echo START_DATE: $START_DATE
  echo END_DATE: $END_DATE
  echo ProcessTimestamp: $processing_started_date_time_denver
  echo ProcessUser: $processing_started_by_user
  echo GRAIN: $GRAIN
  echo EXEC_ID: ${exec_id}
  echo STEP_ID: $STEP


  echo "### Running Portals m2dot0 Set Agg tableset...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_m2dot0_set_agg_tableset-${SCRIPT_VERSION}.hql \
  -hiveconf START_DATE="${START_DATE}" \
  -hiveconf END_DATE="${END_DATE}" \
  -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
  -hiveconf ProcessUser="${processing_started_by_user}" \
  -hiveconf grain="${GRAIN}" \
  -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
  || { echo "HQL Failure"; exit 101; }

  echo "### Running Portals m2dot0 Set Agg accounts...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_m2dot0_set_agg_accounts-${SCRIPT_VERSION}.hql \
  -hiveconf START_DATE="${START_DATE}" \
  -hiveconf END_DATE="${END_DATE}" \
  -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
  -hiveconf ProcessUser="${processing_started_by_user}" \
  -hiveconf grain="${GRAIN}" \
  -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
  || { echo "HQL Failure"; exit 101; }

  echo "### Running Portals m2dot0 Set Agg devices...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_m2dot0_set_agg_devices-${SCRIPT_VERSION}.hql \
  -hiveconf START_DATE="${START_DATE}" \
  -hiveconf END_DATE="${END_DATE}" \
  -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
  -hiveconf ProcessUser="${processing_started_by_user}" \
  -hiveconf grain="${GRAIN}" \
  -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
  || { echo "HQL Failure"; exit 101; }

  echo "### Running Portals m2dot0 Set Agg instances...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_m2dot0_set_agg_instances-${SCRIPT_VERSION}.hql \
  -hiveconf START_DATE="${START_DATE}" \
  -hiveconf END_DATE="${END_DATE}" \
  -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
  -hiveconf ProcessUser="${processing_started_by_user}" \
  -hiveconf grain="${GRAIN}" \
  -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
  || { echo "HQL Failure"; exit 101; }

  echo "### Running Portals m2dot0 Set Agg visits...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_m2dot0_set_agg_visits-${SCRIPT_VERSION}.hql \
  -hiveconf START_DATE="${START_DATE}" \
  -hiveconf END_DATE="${END_DATE}" \
  -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
  -hiveconf ProcessUser="${processing_started_by_user}" \
  -hiveconf grain="${GRAIN}" \
  -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
  || { echo "HQL Failure"; exit 101; }

  echo "### Running Portals m2dot0 Set Agg final...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_m2dot0_set_agg_final-${SCRIPT_VERSION}.hql \
  -hiveconf START_DATE="${START_DATE}" \
  -hiveconf END_DATE="${END_DATE}" \
  -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
  -hiveconf ProcessUser="${processing_started_by_user}" \
  -hiveconf grain="${GRAIN}" \
  -hiveconf execid="${exec_id}" -hiveconf stepid="${STEP}" \
  || { echo "HQL Failure"; exit 101; }
fi
