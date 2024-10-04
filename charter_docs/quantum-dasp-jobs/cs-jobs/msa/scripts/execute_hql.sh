#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5

export CADENCE=daily

echo "### Artifacts Process - Downloading artifacts from S3 into scripts folder"
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo "### Dates process - Download shell script"
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/process_dates.jar .

echo "### Dates process - Extract shell script and timezone lookup from jar file"
jar -xfM process_dates.jar

echo "### Dates process - Process dates"
source process_dates.sh

export END_DATE_LESS_THREE=`date --date="$END_DATE -3 day"  +%Y-%m-%d`
export END_DATE_LESS_SEVEN=`date --date="$END_DATE -7 day"  +%Y-%m-%d`
export END_DATE_LESS_FIFTEEN=`date --date="$END_DATE -15 day" +%Y-%m-%d`
export END_DATE_LESS_EIGHTEEN=`date --date="$END_DATE -18 day" +%Y-%m-%d`

echo TIME_ZONE: $TZ
echo START_DATE_TZ: $START_DATE_TZ;
echo END_DATE_TZ: $END_DATE_TZ;
echo START_DATE: $START_DATE;
echo END_DATE: $END_DATE;
echo END_DATE_LESS_THREE: $END_DATE_LESS_THREE;
echo END_DATE_LESS_SEVEN: $END_DATE_LESS_SEVEN;
echo END_DATE_LESS_FIFTEEN: $END_DATE_LESS_FIFTEEN;
echo END_DATE_LESS_EIGHTEEN: $END_DATE_LESS_EIGHTEEN;

export C_ID=`jq -r ".jobFlowId" /mnt/var/lib/info/job-flow.json`
export CLUSTER=${C_ID:2}
echo "
  EMR CLUSTER ID: $CLUSTER (not including the J- prefix)
"

export KEYS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-keys/"
export JARS_S3_LOCATION="s3://pi-global-${ENVIRONMENT}-udf-jars"

#hive -e "DROP FUNCTION IF EXISTS $ENVIRONMENT.aes_encrypt256; CREATE FUNCTION $ENVIRONMENT.aes_encrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesEncrypt256' USING JAR 's3://pi-global-$ENVIRONMENT-udf-jars/hadoop-libs-hive-1.0.10.jar'"
#hive -e "DROP FUNCTION IF EXISTS $ENVIRONMENT.aes_decrypt256; CREATE FUNCTION $ENVIRONMENT.aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256' USING JAR 's3://pi-global-$ENVIRONMENT-udf-jars/hadoop-libs-hive-1.0.10.jar'"

s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

echo "### Running SSP MSA Parameterized ...................."
hive -v -f ${ARTIFACTS_PATH}/hql/parameterized_msa-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }
