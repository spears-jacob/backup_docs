#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export RUN_HOUR=$6
export ADDITIONAL_PARAMS=$7

export CADENCE=daily
export IsProcessed=0
export TZ=America/Denver

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

echo "### Running Portals Privacysite Metric Agg ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_m2dot0_metric_agg-${SCRIPT_VERSION}.hql \
-hiveconf START_DATE="${START_DATE}" \
-hiveconf END_DATE="${END_DATE}" \
|| { echo "HQL Failure"; exit 101; }
