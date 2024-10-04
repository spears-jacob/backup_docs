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
export CADENCE=daily
export PROD_ACC_NO=387455165365

if [ "$ENVIRONMENT" == "stg" ]; then
    export CQE_SSPP=${PROD_ACC_NO}/prod.core_quantum_events_sspp
elif [ "$ENVIRONMENT" == "prod" ]; then
    export CQE_SSPP=prod.core_quantum_events_sspp
fi

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar


if   [ "${JOB_STEP_NAME}" == "1_cid_cmp_extract" ]; then
  export CADENCE=daily;  source process_dates.sh
  hive -S -v -f ${ARTIFACTS_PATH}/hql/cid_cmp_extract-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "2_cid_cmp_agg_daily" ]; then
  export CADENCE=daily;  source process_dates.sh
  hive -S -v -f ${ARTIFACTS_PATH}/hql/cid_cmp_aggregate-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "3_cid_cmp_agg_weekly" ]; then
  export CADENCE=weekly;  source process_dates.sh
  hive -S -v -f ${ARTIFACTS_PATH}/hql/cid_cmp_aggregate-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "4_cid_cmp_agg_monthly" ]; then
  export CADENCE=monthly;  source process_dates.sh
  hive -S -v -f ${ARTIFACTS_PATH}/hql/cid_cmp_aggregate-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "5_cid_cmp_agg_fiscal_monthly" ]; then
  export CADENCE=fiscal_monthly;  source process_dates.sh
  hive -S -v -f ${ARTIFACTS_PATH}/hql/cid_cmp_aggregate-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
elif [ "${JOB_STEP_NAME}" == "6_tableau_refresh" ]; then
  echo "############ Refreshing View and Tableau"
  bash ./scripts/create_views-${SCRIPT_VERSION}.sh || { echo "HQL Failure"; exit 101;}
  source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
fi