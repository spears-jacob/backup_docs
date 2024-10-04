#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

export LAG_DAYS=2
export CADENCE=fiscal_monthly

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

echo "### Creating all views with hive..."
bash ./scripts/create_hive_views-${SCRIPT_VERSION}.sh

echo TIME_ZONE: $TZ
echo LABEL_DATE_DENVER=$LABEL_DATE_DENVER
echo START_DATE=$START_DATE
echo END_DATE=$END_DATE
export label_date_denver=$LABEL_DATE_DENVER
echo $label_date_denver

DAY=$(date -d "$RUN_DATE" '+%d')
#if [ "$DAY" != "09" -a "$DAY" != "10" -a "$DAY" != "11" ]; then
if [ "$DAY" = "29" -o "$DAY" = "30" -o "$DAY" = "31" -o "$DAY" = "01" -o "$DAY" = "02" ]; then
  echo ".................... Running Referring Traffic ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/referring_traffic-${SCRIPT_VERSION}.hql \
   -hiveconf START_DATE=${START_DATE} \
   -hiveconf END_DATE=${END_DATE} \
   -hiveconf label_date_denver=${label_date_denver} || { echo "HQL Failure"; exit 101; }

  echo ".................... Running Page Load Time ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/page_load_time-${SCRIPT_VERSION}.hql \
   -hiveconf START_DATE=${START_DATE} \
   -hiveconf END_DATE=${END_DATE} \
   -hiveconf label_date_denver=${label_date_denver} || { echo "HQL Failure"; exit 101; }

  echo ".................... Running Error Message ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/error_message-${SCRIPT_VERSION}.hql \
   -hiveconf START_DATE=${START_DATE} \
   -hiveconf END_DATE=${END_DATE} \
   -hiveconf label_date_denver=${label_date_denver} || { echo "HQL Failure"; exit 101; }

  echo ".................... Running Unique Housholds ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/unique_households-${SCRIPT_VERSION}.hql \
   -hiveconf START_DATE=${START_DATE} \
   -hiveconf END_DATE=${END_DATE} \
   -hiveconf label_date_denver=${label_date_denver} || { echo "HQL Failure"; exit 101; }

  echo ".................... Running Other Metrics ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/other_metrics-${SCRIPT_VERSION}.hql -hiveconf label_date_denver=${label_date_denver} || { echo "HQL Failure"; exit 101; }

  echo ".................... Running Callin Rate ...................."
  hive -S -v -f ${ARTIFACTS_PATH}/hql/callin_rate-${SCRIPT_VERSION}.hql -hiveconf label_date_denver=${label_date_denver} || { echo "HQL Failure"; exit 101; }
fi

echo "### Execute Calendar Monthly Job"
bash ./scripts/execute_cm-${SCRIPT_VERSION}.sh $1 $2 $3 $4 $5
