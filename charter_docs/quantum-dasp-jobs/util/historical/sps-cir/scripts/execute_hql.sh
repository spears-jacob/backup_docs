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

export IsProcessed=0
export TZ=America/Denver
export CADENCE=daily

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

if [ "${JOB_STEP_NAME}" == "2_tableau_refresh" ]; then
	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
else
	# run the spark job based on the parameters
	spark-shell --num-executors=64 -i ./scripts/pageview_call_in_rate_daily-${SCRIPT_VERSION}.scala
	spark-shell --num-executors=64 -i ./scripts/cs_call_care_derivations-${SCRIPT_VERSION}.scala

	LOAD_DATE=`date -d "$RUN_DATE -9 day" +%Y-%m-%d` #since source data may change as far as 7 days back, this should update until then

	echo "Running for "$RUN_DATE
	echo "Which means loading data since "$LOAD_DATE

  hive -S -v -f ${ARTIFACTS_PATH}/hql/cwpv_agg-${SCRIPT_VERSION}.hql -hiveconf load_date=$LOAD_DATE || { echo "HQL Failure"; exit 101; }
fi
