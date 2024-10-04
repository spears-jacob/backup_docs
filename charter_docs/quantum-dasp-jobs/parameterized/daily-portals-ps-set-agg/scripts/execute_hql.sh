#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

export TIME_ZONE=UTC
export CADENCE=daily

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

export partition_date_utc=$START_DATE

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

echo TIME_ZONE: $TZ
echo START_DATE: $START_DATE
echo END_DATE: $END_DATE
echo partition_date_utc: $partition_date_utc
echo GRAIN: $GRAIN
echo CLUSTER: $CLUSTER

echo "### Running Portals Privacysite Set Agg tableset...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_privacysite_set_agg_tableset-${SCRIPT_VERSION}.hql -hiveconf START_DATE="${START_DATE}" \
-hiveconf END_DATE="${END_DATE}" \
-hiveconf partition_date_utc="${partition_date_utc}" \
-hiveconf grain="${GRAIN}" \
-hiveconf CLUSTER="${CLUSTER}" || { echo "HQL Failure"; exit 101; }

#echo "### Running Portals Privacysite Set Agg accounts...................."
#hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_privacysite_set_agg_accounts-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }

echo "### Running Portals Privacysite Set Agg devices...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_privacysite_set_agg_devices-${SCRIPT_VERSION}.hql -hiveconf START_DATE="${START_DATE}" \
-hiveconf END_DATE="${END_DATE}" \
-hiveconf partition_date_utc="${partition_date_utc}" \
-hiveconf grain="${GRAIN}" \
-hiveconf CLUSTER="${CLUSTER}" || { echo "HQL Failure"; exit 101; }

echo "### Running Portals Privacysite Set Agg instances...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_privacysite_set_agg_instances-${SCRIPT_VERSION}.hql -hiveconf START_DATE="${START_DATE}" \
-hiveconf END_DATE="${END_DATE}" \
-hiveconf partition_date_utc="${partition_date_utc}" \
-hiveconf grain="${GRAIN}" \
-hiveconf CLUSTER="${CLUSTER}" || { echo "HQL Failure"; exit 101; }

echo "### Running Portals Privacysite Set Agg visits...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_privacysite_set_agg_visits-${SCRIPT_VERSION}.hql -hiveconf START_DATE="${START_DATE}" \
-hiveconf END_DATE="${END_DATE}" \
-hiveconf partition_date_utc="${partition_date_utc}" \
-hiveconf grain="${GRAIN}" \
-hiveconf CLUSTER="${CLUSTER}" || { echo "HQL Failure"; exit 101; }

echo "### Running Portals Privacysite Set Agg final...................."
hive -S -v -f ${ARTIFACTS_PATH}/hql/portals_privacysite_set_agg_final-${SCRIPT_VERSION}.hql -hiveconf START_DATE="${START_DATE}" \
-hiveconf END_DATE="${END_DATE}" \
-hiveconf partition_date_utc="${partition_date_utc}" \
-hiveconf grain="${GRAIN}" \
-hiveconf CLUSTER="${CLUSTER}" || { echo "HQL Failure"; exit 101; }
