#!/bin/bash
# This script is to make a tsv file and send it to a ftp folders

# 0. - Check for the existence of a RUN_DATE or bail out.
if [ "$RUN_DATE" != "" ]; then
  extract_run_date=`date --date="$RUN_DATE -1 day" +%Y-%m-%d`
else
  echo "
    ### ERROR: Run date ($RUN_DATE) value not available
  " && exit 1
fi

LOCAL_DIR=login_data_feed_msa
TABLE_NAME=asp_extract_login_data_daily_pvt
HDFS_DIR=/archive/$TABLE_NAME

login_data_feed=$(hive -e "set hive.cli.print.header=false; USE $ENVIRONMENT; SELECT COUNT(date_denver) FROM $TABLE_NAME;" || echo "something failed when querying $TABLE_NAME"; exit 10001110101 ;)

echo "

    $login_data_feed records in $ENVIRONMENT.$TABLE_NAME

"

if [ "$login_data_feed" -lt 1 ]; then
  echo "Records not found in $ENVIRONMENT.$TABLE_NAME"
  exit 1
else
    echo $login_data_feed "Records found in $ENVIRONMENT.$TABLE_NAME"

    FILE_NAME="$TABLE_NAME"_${extract_run_date}_spa
    echo "Writing to $FILE_NAME"

    mkdir -p ${LOCAL_DIR}
    hive -f ../src/create_file.hql --hiveconf table_name=$TABLE_NAME --hiveconf extract_start_date=${extract_run_date} --hiveconf extract_end_date=${extract_run_date} > ${LOCAL_DIR}/$FILE_NAME.tsv
fi

if [ $? -eq 0 ]; then

    ldr=$(pwd)/${LOCAL_DIR}

    echo "report generated at $ldr"
    if [ "$ENVIRONMENT" != "prod" ]; then
        echo "Not prod; no file transfer attempted"
      else
        cd $ldr
        tar --force-local --warning=no-file-changed -czvf $FILE_NAME.tgz  $FILE_NAME.tsv --remove-files|| [[ $? -eq 1 ]]
        echo "
             moving file to hdfs after clearing
        "
        hdfs dfs -mkdir -p ${HDFS_DIR}

        hdfs dfs -rm -skipTrash -r -f ${HDFS_DIR}/*.tgz

        hdfs dfs -moveFromLocal ${FILE_NAME}.tgz ${HDFS_DIR}
        hdfs dfs -ls ${HDFS_DIR}/${FILE_NAME}.tgz
    fi

else
    echo "job ended unsuccessfully -- Please re-run"
    exit 1
fi
