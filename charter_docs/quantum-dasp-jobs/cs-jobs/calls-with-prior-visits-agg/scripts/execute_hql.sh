#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Creating views..."
bash ./scripts/create_views-${SCRIPT_VERSION}.sh

LOAD_DATE=`date -d "$RUN_DATE -9 day" +%Y-%m-%d` #since source data may change as far as 7 days back, this should update until then

echo "Running for "$RUN_DATE
echo "Which means loading data since "$LOAD_DATE

hive -S -v -f ${ARTIFACTS_PATH}/hql/cwpv_agg-${SCRIPT_VERSION}.hql -hiveconf load_date=$LOAD_DATE || { echo "HQL Failure"; exit 101; }
