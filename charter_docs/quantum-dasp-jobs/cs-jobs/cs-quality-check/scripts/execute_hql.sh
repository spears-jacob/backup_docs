#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export TMP_db=$3
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Creating all views with hive..."
bash ./scripts/create_views-${SCRIPT_VERSION}.sh

TWO_DAYS=`date -d "$RUN_DATE -2 day" +%Y-%m-%d`
THIRTYONE_DAYS=`date -d "$RUN_DATE -31 day" +%Y-%m-%d`

echo $RUN_DATE
echo $TWO_DAYS
echo $THIRTYONE_DAYS

hive -v -f ${ARTIFACTS_PATH}/hql/daily_cs_qc-${SCRIPT_VERSION}.hql -hiveconf two_days=$TWO_DAYS -hiveconf thirtyone_days=$THIRTYONE_DAYS || { echo "HQL Failure"; exit 101; }
#hive -v -f ${ARTIFACTS_PATH}/hql/cir_record_count-${SCRIPT_VERSION}.hql -hiveconf two_days=$TWO_DAYS -hiveconf thirtyone_days=$THIRTYONE_DAYS || { echo "HQL Failure"; exit 101; }
#hive -v -f ${ARTIFACTS_PATH}/hql/call_care_long_segments-${SCRIPT_VERSION}.hql -hiveconf two_days=$TWO_DAYS -hiveconf thirtyone_days=$THIRTYONE_DAYS || { echo "HQL Failure"; exit 101; }
