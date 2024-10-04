#!/bin/bash

run_sentiment=$1
data_sources_path=$2
warehouse_path=$3
TMP_db=$4
incoming_path=$5
SCRIPT_VERSION=$6

# run_sentiment is '0'... that step is deprecated
if [ $1 == 1 ];  then hdfs dfs -cp ${incoming_path}/app_figures_sentiment/* $warehouse_path/$TMP_db.db/app_figures_sentiment ; fi

echo "Load appfigures sentiment data to tmp table data"
hive -v -f ${ARTIFACTS_PATH}/hql/app_figure_repair_sentiment-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }

echo "Load appfigures data"
hive -v -f ${ARTIFACTS_PATH}/hql/app_figures-${SCRIPT_VERSION}.hql  || { echo "HQL Failure"; exit 101; }

echo "Load appfigures reviews/sentiment data"
# run_sentiment is '0'... that iteration is deprecated
if [ $1 == 1 ];  then hive -f ${ARTIFACTS_PATH}/hql/app_figures_sentiment-${SCRIPT_VERSION}.hql ;
else hive -f ${ARTIFACTS_PATH}/hql/app_figures_reviews_to_sentiment-${SCRIPT_VERSION}.hql  || { echo "HQL Failure"; exit 101; }
fi

# run_sentiment is '0'... that iteration is deprecated
# echo "Clean up data source directory"
# rm -rf  $data_sources_path/appfigures/sentiment
