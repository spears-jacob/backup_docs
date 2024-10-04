#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_db=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export IDEN_db=${ENVIRONMENT}_iden

export TZ=America/Denver
export CADENCE="monthly"
export IsReprocess=0
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Date process..."
source ./scripts/process_dates-${SCRIPT_VERSION}.sh $IsReprocess $TZ

echo START_DATE: $START_DATE
echo END_DATE: $END_DATE
echo START_DATE_TZ: $START_DATE_TZ
echo END_DATE_TZ: $END_DATE_TZ
echo PRIOR_8DAYS_START_DATE: $PRIOR_8DAYS_START_DATE
echo PRIOR_8DAYS_END_DATE: $PRIOR_8DAYS_END_DATE
echo PRIOR_60DAYS_START_DATE: $PRIOR_60DAYS_START_DATE
echo LABEL_DATE_DENVER: $LABEL_DATE_DENVER
echo GRAIN: $GRAIN
echo LAST_MONTH_START_DATE: $LAST_MONTH_START_DATE
echo LAST_MONTH_END_DATE: $LAST_MONTH_END_DATE

echo "### Running ${CADENCE} Self-Install activation channel metrics (agg table)...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/1_si_activation_channel_monthly-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
 -hiveconf START_DATE_TZ="${START_DATE_TZ}" -hiveconf END_DATE_TZ="${END_DATE_TZ}" \
 -hiveconf PRIOR_8DAYS_START_DATE="${PRIOR_8DAYS_START_DATE}" -hiveconf PRIOR_8DAYS_END_DATE="${PRIOR_8DAYS_END_DATE}" \
 -hiveconf LAST_MONTH_START_DATE="${LAST_MONTH_START_DATE}" -hiveconf LAST_MONTH_END_DATE="${LAST_MONTH_END_DATE}" \
 -hiveconf PRIOR_60DAYS_START_DATE="${PRIOR_60DAYS_START_DATE}" \
 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }

 