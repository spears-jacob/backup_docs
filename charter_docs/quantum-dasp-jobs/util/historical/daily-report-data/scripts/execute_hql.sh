#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

export TZ=America/Denver

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Creating all views with hive..."
bash ./scripts/create_hive_views-${SCRIPT_VERSION}.sh
#echo "### Date check and wait ..."
#bash ./scripts/data_check_and_wait-${SCRIPT_VERSION}.sh

echo "### Running Daily Report Data ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/daily_report_data-${SCRIPT_VERSION}.hql -hiveconf RUN_DATE=${RUN_DATE} || { echo "HQL Failure"; exit 101; }
echo "### Running Daily Data Summary...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/daily_data_summary-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
