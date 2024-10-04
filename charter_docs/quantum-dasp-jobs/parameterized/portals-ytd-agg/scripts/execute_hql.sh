#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export JOB_STEP=$5
export JOB_STEP_NAME=$(echo $5 | cut -d"-" -f 1)
export SCRIPT_VERSION=$(echo $5 | cut -d"-" -f 2 | cut -d"." -f 1)

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

if [ "${JOB_STEP_NAME}" == "2_tableau_refresh" ]; then
	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
else
	export START_DATE=$(date -d "$RUN_DATE -7 day" +%Y-01-01)          #beginning of year
	export END_DATE=$(date -d "$RUN_DATE" +%Y-%m-01)                   #first day of run month
	export LABEL_DATE_DENVER=$(date -d "$END_DATE -1 day" +%Y-%m-%d)   #last day of previous month

	echo "### Running portals ytd agg"
	echo RUN_DATE: $RUN_DATE;
	echo START_DATE: $START_DATE;
	echo END_DATE: $END_DATE;
	echo LABEL_DATE_DENVER: $LABEL_DATE_DENVER;

	echo "### Creating tableau views with athena..."
	bash ./scripts/create_athena_views-${SCRIPT_VERSION}.sh

  hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_ytd_agg-${SCRIPT_VERSION}.hql \
   -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
   -hiveconf LABEL_DATE_DENVER="${LABEL_DATE_DENVER}" || { echo "HQL Failure"; exit 101; }
fi
