#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
# run_sentiment is '0'... that step is deprecated but now uses hardcoded value
export run_sentiment=0
# Change DaysToLag here to debug.
export DaysToLag=0

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

# specify without "/" at the end of a path
export s3_bucket=s3://pi-qtm-dasp-prod-aggregates-nopii
export incoming_path=$s3_bucket/data/prod_dasp/nifi/app_figures
# todo: remove hardcoded "pi-qtm-dasp-${ENVIRONMENT}-aggregates-nopii" bucket from the list
echo $incoming_path

export warehouse_path=/apps/hive/warehouse
echo $warehouse_path

export data_sources_path=$(pwd)
echo $data_sources_path

echo ".............Creating local temporary tables............"
hive -S -v -f ${ARTIFACTS_PATH}/hql/app_figures_init-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }

echo ".............Running Parse stage............."
bash ./scripts/appfigures_parse-${SCRIPT_VERSION}.sh $data_sources_path $warehouse_path $TMP_db $incoming_path $SCRIPT_VERSION $DaysToLag $s3_bucket

echo ".............Running Appfigures Insert............."
bash ./scripts/appfigures_insert-${SCRIPT_VERSION}.sh $run_sentiment $data_sources_path $warehouse_path $TMP_db $incoming_path $SCRIPT_VERSION

echo "Running Appfigures Cleanup"
bash ./scripts/appfigures_cleanup-${SCRIPT_VERSION}.sh $warehouse_path $ENVIRONMENT
