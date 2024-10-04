#!/bin/bash
export TZ=America/Denver

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export historical_load=0

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

source ./scripts/process_dates-${SCRIPT_VERSION}.sh

echo "### Running Portals Pageview click aggregate ...................."
echo START_DATE: $START_DATE
echo END_DATE: $END_DATE

echo "Starting extract_quantum_event_data"
hive -S -v -f ${ARTIFACTS_PATH}/hql/extract_quantum_event_data-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE=${START_DATE} \
 -hiveconf END_DATE=${END_DATE}  || { echo "HQL Failure"; exit 101; }

echo "Starting calendar_monthly_pageview_click_aggregate"
hive -S -v -f ${ARTIFACTS_PATH}/hql/calendar_monthly_pageview_click_aggregate-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }

echo "Starting daily_pageview_click_aggregate"
hive -S -v -f ${ARTIFACTS_PATH}/hql/daily_pageview_click_aggregate-${SCRIPT_VERSION}.hql  || { echo "HQL Failure"; exit 101; }

echo "Starting weekly_pageview_click_aggregate"
hive -S -v -f ${ARTIFACTS_PATH}/hql/weekly_pageview_click_aggregate-${SCRIPT_VERSION}.hql  || { echo "HQL Failure"; exit 101; }

echo "Starting monthly_pageview_click_aggregate"
hive -S -v -f ${ARTIFACTS_PATH}/hql/monthly_pageview_click_aggregate-${SCRIPT_VERSION}.hql  || { echo "HQL Failure"; exit 101; }

echo "Starting selectaction_aggregate_view"
bash ./scripts/create_views-${SCRIPT_VERSION}.sh || { echo "HQL Failure"; exit 101; }