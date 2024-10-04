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
export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export LAG_DAYS=5
export LEAD_DAYS=4
export CADENCE=daily
export PROD_ACC_NO=387455165365

if [ "$ENVIRONMENT" == "stg" ]; then
    export CQES=${PROD_ACC_NO}/prod.core_quantum_events_sspp
    export CWPV=${PROD_ACC_NO}/prod_dasp.cs_calls_with_prior_visits
    export CCCD=${PROD_ACC_NO}/prod.atom_cs_call_care_data_3

elif [ "$ENVIRONMENT" == "prod" ]; then
    export CQES=prod.core_quantum_events_sspp
    export CWPV=prod_dasp.cs_calls_with_prior_visits
    export CCCD=prod.atom_cs_call_care_data_3
fi


echo "### Pulling down UDF cipher keys..."
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

export ENDDATE=`date --date="$RUN_DATE + 1 day" +%Y-%m-%d`

echo TIME_ZONE: $TZ
echo START_DATE_TZ: $START_DATE_TZ;
echo END_DATE_TZ: $END_DATE_TZ;
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;
echo ENDDATE: $ENDDATE;

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

echo "### Running Support Article Call Disposition ...................."
hive -v -f ${ARTIFACTS_PATH}/hql/support_article_call_disp-${SCRIPT_VERSION}.hql \
-hiveconf START_DATE="${START_DATE}" || { echo "HQL Failure"; exit 101; }
