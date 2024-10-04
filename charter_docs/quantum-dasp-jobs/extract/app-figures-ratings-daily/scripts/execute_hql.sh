#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Drop and Creating all views with hive..."
bash ./scripts/create_hive_views-${SCRIPT_VERSION}.sh

echo $RUN_DATE

echo "### Running App Figures Ratings Daily ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/app_figures_daily_rating-${SCRIPT_VERSION}.hql -hiveconf RUN_DATE="${RUN_DATE}" || { echo "HQL Failure"; exit 101; }

echo "### Running App Figures Daily Aggregation ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/app_figures_daily_aggregation-${SCRIPT_VERSION}.hql -hiveconf RUN_DATE="${RUN_DATE}" || { echo "HQL Failure"; exit 101; }

