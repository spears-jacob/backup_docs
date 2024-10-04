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
#Joey's not sure if this line is essential or not. It's pulling every single file, which seems excessive.  But we do need it to be access the one file it needs

if [ "${JOB_STEP_NAME}" == "2_tableau_refresh" ]; then
	source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT
else
  spark-shell --driver-memory 10G  --executor-memory 10G --executor-cores 1 --num-executors=64 -i ./scripts/cs_call_care_derivations-${SCRIPT_VERSION}.scala
fi
