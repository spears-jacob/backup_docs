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

export INPUT_VALUE=$(echo ${ADDITIONAL_PARAMS} | jq '.INPUT_VALUE' | tr -d '"' | sed "s/null//" )

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER_ID=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER_ID (not including the J- prefix)
"

echo INPUT_VALUE: $INPUT_VALUE

if [[ -z "$INPUT_VALUE" ]]; then
   exit 1
fi

IFS='|' read -r TABLE_NAME FILTER_DATE_COLUMN PARTITION_COLUMNS START_DATE END_DATE<<<"$INPUT_VALUE"
export ORIG_TABLE_NAME="$DASP_db.$TABLE_NAME"

echo ORIG_TABLE_NAME: $ORIG_TABLE_NAME
echo FILTER_DATE_COLUMN: $FILTER_DATE_COLUMN
echo PARTITION_COLUMNS: $PARTITION_COLUMNS
echo START_DATE: $START_DATE
echo END_DATE: $END_DATE
echo CLUSTER_ID: $CLUSTER_ID

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Running FileIntegrator ...................."

spark-submit --class com.spectrum.dasp.FileIntegrator s3://pi-global-stg-udf-jars/fileintegrator_2.12-0.1.jar $ORIG_TABLE_NAME $FILTER_DATE_COLUMN "$PARTITION_COLUMNS" $START_DATE $END_DATE $CLUSTER_ID
