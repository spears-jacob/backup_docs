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

#echo "### Drop all billing views..."
#bash ./scripts/drop_all_views-${SCRIPT_VERSION}.sh
#echo "### Creating all views with hive..."
#bash ./scripts/create_hive_views-${SCRIPT_VERSION}.sh

echo "### Process dates..."
source ./scripts/process_dates-${SCRIPT_VERSION}.sh $IsProcessed $TZ

echo "### Running billing agg..."
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;
echo label_date_denver: $LABEL_DATE_DENVER;
echo grain: $GRAIN;
echo ProcessTimestamp: $processing_started_date_time_denver
echo ProcessUser: $processing_started_by_user

hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_billing_agg-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }

