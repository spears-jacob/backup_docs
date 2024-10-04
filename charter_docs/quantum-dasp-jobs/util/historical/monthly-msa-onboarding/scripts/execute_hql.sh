#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

export LAG_DAYS=2
export CADENCE=monthly

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

echo "### create all views with hive..."
bash ./scripts/create_hive_views-${SCRIPT_VERSION}.sh

echo START_DATE: $START_DATE
echo END_DATE: $END_DATE
echo START_DATE_TZ: $START_DATE_TZ
echo END_DATE_TZ: $END_DATE_TZ
echo LABEL_DATE_DENVER: $LABEL_DATE_DENVER
echo GRAIN: $GRAIN

echo "### Running ${CADENCE} Msa onboarding metrics...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/msa_onboarding_metrics-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE="${START_DATE}" -hiveconf END_DATE="${END_DATE}" \
 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }

echo "### Running ${CADENCE} Msa onboarding time...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/msa_onboarding_time-${SCRIPT_VERSION}.hql \
 -hiveconf START_DATE_TZ="${START_DATE_TZ}" -hiveconf END_DATE_TZ="${END_DATE_TZ}" \
 -hiveconf label_date_denver="${LABEL_DATE_DENVER}" -hiveconf grain="${GRAIN}" \
 -hiveconf ProcessTimestamp="${processing_started_date_time_denver}" \
 -hiveconf ProcessUser="${processing_started_by_user}" || { echo "HQL Failure"; exit 101; }
