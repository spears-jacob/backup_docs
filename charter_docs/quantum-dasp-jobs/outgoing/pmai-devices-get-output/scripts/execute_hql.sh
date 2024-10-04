#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export SEC_db=${ENVIRONMENT}_sec_repo_sspp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export RUN_HOUR=$6
export ADDITIONAL_PARAMS=$7

export START_DATE_MANUAL=$(echo ${ADDITIONAL_PARAMS} | jq '.START_DATE_MANUAL' | tr -d '"' | sed "s/null//" )

export LAG_DAYS=2
export CADENCE=weekly

echo "START_DATE_MANUAL: $START_DATE_MANUAL"

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

if [[ ! -z ${START_DATE_MANUAL} ]]; then
  START_DATE=${START_DATE_MANUAL}
fi

echo TIME_ZONE: $TZ
echo RUN_DATE: $RUN_DATE
echo START_DATE: $START_DATE
echo END_DATE : $END_DATE
echo START_DATE_TZ: $START_DATE_TZ
echo END_DATE_TZ : $END_DATE_TZ

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

echo "### List PAMI File of Secured Bucket ...................."
echo "aws s3 ls s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/asp_pmai_sia_account_in/"
aws s3 ls s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/asp_pmai_sia_account_in/

export INPUT_FILE_NAME=`aws s3 ls s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/asp_pmai_sia_account_in/ | grep gz | awk -F ' ' '{print $4}'`
echo "INPUT_FILE_NAME: $INPUT_FILE_NAME"
INPUT_DATE_STRING=`echo $INPUT_FILE_NAME | cut -d'_' -f4 | cut -d'.' -f1 | head -c 8`
INPUT_DATE_YEAR=`echo $INPUT_DATE_STRING | cut -c1-4`
INPUT_DATE_MONTH=`echo $INPUT_DATE_STRING | cut -c5-6`
INPUT_DATE_DAY=`echo $INPUT_DATE_STRING | cut -c7-8`
export INPUT_FILE_DATE="${INPUT_DATE_YEAR}-${INPUT_DATE_MONTH}-${INPUT_DATE_DAY}"
echo "INPUT_FILE_DATE: $INPUT_FILE_DATE"

export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

echo "### Copy Key File ...................."
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

echo "### Running PMAI data ...................."

hive -S -v -f ${ARTIFACTS_PATH}/hql/asp_extract_pmai_data_daily-${SCRIPT_VERSION}.hql \
-hiveconf START_DATE_TZ=$START_DATE_TZ \
-hiveconf END_DATE_TZ=$END_DATE_TZ \
-hiveconf RUN_DATE=$RUN_DATE \
-hiveconf START_DATE=$START_DATE \
-hiveconf END_DATE=$END_DATE || { echo "HQL Failure"; exit 101; }

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "ls ./scripts"
ls ./scripts/

echo "### Running Quality Check ...................."
source ./scripts/quality_check-${SCRIPT_VERSION}.sh $ENVIRONMENT $DASP_db $RUN_DATE
