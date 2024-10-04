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
	echo "### Creating all views with hive..."
	bash ./scripts/create_views-${SCRIPT_VERSION}.sh

	LOAD_DATE=`date -d "$RUN_DATE -7 day" +%Y-%m-%d` #since source data may change as far as 7 days back, this should update until then

	echo "Running for "$RUN_DATE
	echo "Which means loading data since "$LOAD_DATE

  hive -S -v -f ${ARTIFACTS_PATH}/hql/call_care_data_agg-${SCRIPT_VERSION}.hql -hiveconf load_date=$LOAD_DATE || { echo "HQL Failure"; exit 101; }
fi
