#!/bin/bash

export DASP_db=$1
export START_DATE=$2
export END_DATE=$3

export EXPORT_FILE="specmobile_login_export_${START_DATE}_${END_DATE}.txt"

echo ".............Running specmobile_login_decrypt_export.hql ............."
s3-dist-cp --src=s3://pi-global-$ENVIRONMENT-udf-keys/ --dest=hdfs:///enc_zone

hive -S -f ${ARTIFACTS_PATH}/hql/specmobile_login_decrypt_export-${SCRIPT_VERSION}.hql \
-hiveconf START_DATE=$START_DATE \
-hiveconf END_DATE=$END_DATE > ${EXPORT_FILE} || { echo "HQL Failure"; exit 101; }

pwd
ls -l
head ${EXPORT_FILE}

gzip ${EXPORT_FILE}
