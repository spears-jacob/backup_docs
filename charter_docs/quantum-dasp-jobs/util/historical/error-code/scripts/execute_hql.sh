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

export RUN_OPTION=$(echo ${ADDITIONAL_PARAMS} | jq '.RUN_OPTION' | tr -d '"' | sed "s/null//" )

export TZ=America/Denver
export CADENCE=daily
export IsReprocess=0

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

source ./scripts/process_dates-${SCRIPT_VERSION}.sh $IsReprocess $TZ

echo "### Running Daily Error Code ...................."
echo START_DATE_TZ: $START_DATE_TZ
echo END_DATE_TZ: $END_DATE_TZ
echo DASP_db: $DASP_db
echo GLOBAL_DB: $GLOBAL_DB
echo RUN_OPTION: $RUN_OPTION

echo "### Running daily_error_code ...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/daily_error_code-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_TZ=${START_DATE_TZ} \
 -hiveconf END_DATE_TZ=${END_DATE_TZ} || { echo "HQL Failure"; exit 101; }
