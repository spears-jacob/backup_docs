#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

export IsProcessed=0
export TZ=America/Denver
export CADENCE=daily
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

#echo "### Creating views ...................."
#. ./scripts/create_hive_views-${SCRIPT_VERSION}.sh

echo "Process dates..."
source ./scripts/process_dates-${SCRIPT_VERSION}.sh $IsProcessed $TZ


echo "### Running Portals API Agg ...................."
echo START_DATE_TZ: $START_DATE_TZ;
echo END_DATE_TZ: $END_DATE_TZ;

hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_api_agg-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_TZ=${START_DATE_TZ} \
 -hiveconf END_DATE_TZ=${END_DATE_TZ} || { echo "HQL Failure"; exit 101; }