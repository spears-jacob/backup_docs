#!/bin/bash
export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"
export CADENCE=daily

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

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

echo START_DATE_TZ: $START_DATE_TZ;
echo END_DATE_TZ: $END_DATE_TZ;
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;

echo "### create all views with hive..."
bash ./scripts/create_hive_views-${SCRIPT_VERSION}.sh

echo "### Running Dasp quality mos...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/dasp_quality_mos-${SCRIPT_VERSION}.hql \
-hiveconf START_DATE_TZ=$START_DATE_TZ \
-hiveconf END_DATE_TZ=$END_DATE_TZ \
-hiveconf START_DATE=$START_DATE \
-hiveconf END_DATE=$END_DATE || { echo "HQL Failure"; exit 101; }

echo "### Running Dasp quality core...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/dasp_quality_core-${SCRIPT_VERSION}.hql \
-hiveconf START_DATE_TZ=$START_DATE_TZ \
-hiveconf END_DATE_TZ=$END_DATE_TZ \
-hiveconf START_DATE=$START_DATE \
-hiveconf END_DATE=$END_DATE || { echo "HQL Failure"; exit 101; }
