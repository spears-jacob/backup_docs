#!/bin/bash

warehouse_path=$1
echo "Clean up $warehouse_path HDFS directory"

hdfs dfs -rm -R $warehouse_path/$TMP_db.db/app_figures_parsed/*
hdfs dfs -rm -R $warehouse_path/$TMP_db.db/app_figures_sentiment

echo "Clean up old temporary tables and glue"
hive -v -f ${ARTIFACTS_PATH}/hql/app_figures_cleanup_temp_tables-${SCRIPT_VERSION}.hql || { echo "HQL Failure"; exit 101; }

ENVIRONMENT=$2
s3_buk="s3://pi-qtm-dasp-${ENVIRONMENT}-aggregates-nopii"
s3_loc="${s3_buk}/data/${ENVIRONMENT}_dasp/nifi/app_figures/"
aws s3 ls --summarize --human-readable --recursive \
${s3_loc} | grep -v archive | grep nifi | awk -F ' ' '{print $3, $5}' > rmlist

zero=0
while IFS=' ' read size line; do
    if [ ${size} -eq ${zero} ]; then
    s3_loc="${s3_buk}/${line}"
    echo "aws s3 rm ${s3_loc}"

    aws s3 rm ${s3_loc}
    fi
done < rmlist

rm rmlist
