#!/bin/bash
export TZ=America/Denver
export CADENCE=daily
export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export IsReprocess=0
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

#echo "### Create all views with hive..."
#bash ./scripts/create_hive_views-${SCRIPT_VERSION}.sh

source ./scripts/process_dates-${SCRIPT_VERSION}.sh $IsReprocess $TZ
echo "### Running IDM Paths ...................."
echo START_DATE_TZ: $START_DATE_TZ
echo END_DATE_TZ: $END_DATE_TZ

echo "### Running Report flow ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/report_flow-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_TZ=${START_DATE_TZ} \
 -hiveconf END_DATE_TZ=${END_DATE_TZ} || { echo "HQL Failure"; exit 101; }

echo "### Running Report time ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/report_time-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_TZ=${START_DATE_TZ} \
 -hiveconf END_DATE_TZ=${END_DATE_TZ} || { echo "HQL Failure"; exit 101; }

echo "### Running Report metrics ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/report_metrics-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_TZ=${START_DATE_TZ} \
 -hiveconf END_DATE_TZ=${END_DATE_TZ} || { echo "HQL Failure"; exit 101; }

