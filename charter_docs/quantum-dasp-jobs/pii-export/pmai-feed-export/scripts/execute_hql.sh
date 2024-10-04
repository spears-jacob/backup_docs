#!/bin/bash

export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
export HQL_FILE="pmai_devices_decrypt_export"

echo 'RUN_DATE is ' $RUN_DATE
echo 'ENVIRONMENT is ' $ENVIRONMENT
echo 'TMP_db is ' $TMP_db
echo 'ARTIFACTS_PATH is ' $ARTIFACTS_PATH
echo 'SCRIPT_VERSION ' $SCRIPT_VERSION
echo 'RUN_HOUR is ' $6

extract_date_string=`date --date="$RUN_DATE" +%Y-%m-%d`
echo RUN_DATE: $extract_date_string

export EXPORT_FILE="pmai_devices_export_${extract_date_string}.txt"

extract_run_date=`date --date="$extract_date_string -1 day" +%Y-%m-%d`

echo 'ARTIFACTS_PATH ' ${ARTIFACTS_PATH}
echo 'HQL_SCRIPT ' ${HQL_FILE}
echo 'ls results '
hadoop fs -ls ${ARTIFACTS_PATH}/hql/
echo "end results"

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts

echo ".............Running hql file ............."
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone
echo "running hql file"
hive -S -f ${ARTIFACTS_PATH}/hql/${HQL_FILE}-${SCRIPT_VERSION}.hql -hiveconf RUN_DATE=$extract_date_string > ${EXPORT_FILE} || { echo "HQL Failure"; exit 101; }
gzip ${EXPORT_FILE}

echo ".............Copy export file to secured bucket ............."
aws s3 cp ${EXPORT_FILE}.gz s3://pi-global-sec-repo-${ENVIRONMENT}-feeds-pii/data/${ENVIRONMENT}_sec_repo_sspp/pmai_extracts/pmai_devices_exports/

echo 'SCRIPT_VERSION for export-send-devices' $SCRIPT_VERSION
echo ".............Running send tdcs file script ............."
hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts
ls ./scripts
. ./scripts/export-send-devices-${SCRIPT_VERSION}.sh

rm ${EXPORT_FILE}.gz

echo 'SCRIPT_VERSION is ' ${5}
echo 'ENVIRONMENT is ' $ENVIRONMENT
echo 'EXPORT_FILE is ' $EXPORT_FILE
echo ".............Running Quality Check Script ............."
. ./scripts/quality_check-${5}.sh $ENVIRONMENT $EXPORT_FILE
