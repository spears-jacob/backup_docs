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
export PROD_ACC_NO=387455165365
export CADENCE=daily # (daily, weekly, monthly, fiscal_monthly)


if [ "$ENVIRONMENT" == "stg" ]; then
    export CQE_SSPP=${PROD_ACC_NO}/prod.core_quantum_events_sspp
elif [ "$ENVIRONMENT" == "prod" ]; then
    export CQE_SSPP=prod.core_quantum_events_sspp
fi

export pwd=`pwd`
echo -e "\n\n### Execution is occurring the following directory:  $pwd\n\n"

export STEP=${pwd#*-}
echo "
  EMR STEP ID: $STEP (not including the S- prefix)
"

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

echo TIME_ZONE: $TZ
echo START_DATE_TZ: $START_DATE_TZ;
echo END_DATE_TZ: $END_DATE_TZ;
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

echo "Running for "$RUN_DATE
echo "Which means loading data from "$LOAD_DATE



if [ "${JOB_STEP_NAME}" == "2_tableau_refresh" ]; then
	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
else
  hive -S -v -f ${ARTIFACTS_PATH}/hql/service_appts_dashboard-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
fi