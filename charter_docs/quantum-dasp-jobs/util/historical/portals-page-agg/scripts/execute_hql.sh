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

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Creating Tableau views with AWS Athena..."
bash ./scripts/create_athena_views-${SCRIPT_VERSION}.sh
echo "### Running Date process..."
source ./scripts/process_dates-${SCRIPT_VERSION}.sh $IsProcessed $TZ

echo "### Running Portals Page Agg......"
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;
echo START_DATE_TZ: $START_DATE_TZ;
echo END_DATE_TZ: $END_DATE_TZ;

echo "### Running Page Agg ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_page_agg-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_TZ=${START_DATE_TZ} \
 -hiveconf END_DATE_TZ=${END_DATE_TZ} || { echo "HQL Failure"; exit 101; }

echo "### Running Page Set Counts Agg ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_page_set_counts_agg-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE=${START_DATE} \
 -hiveconf END_DATE=${END_DATE} || { echo "HQL Failure"; exit 101; }

echo "### Running Page Set Pathing Agg ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_page_set_pathing_agg-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE=${START_DATE} \
 -hiveconf END_DATE=${END_DATE} || { echo "HQL Failure"; exit 101; }
