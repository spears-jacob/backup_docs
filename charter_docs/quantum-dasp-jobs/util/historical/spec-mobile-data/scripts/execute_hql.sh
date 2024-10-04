#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"
export TIME_ZONE=America/New_York
export CADENCE=daily
export LAG_DAYS=2

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

echo START_DATE_TZ: $START_DATE_TZ
echo END_DATE_TZ: $END_DATE_TZ

echo "### Running Spec Mobile Data ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/spec_mobile_data-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_TZ=${START_DATE_TZ} \
 -hiveconf END_DATE_TZ=${END_DATE_TZ} || { echo "HQL Failure"; exit 101; }
