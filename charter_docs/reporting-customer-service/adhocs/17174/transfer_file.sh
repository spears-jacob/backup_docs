#!/bin/bash
# This script is to make a csv file and send it to a ftp folders

TABLE_NAME=cs_dps_update_summary
  #tablename in create_historical_file.hql is not dynamic
  #you'll have to change it there too if you want a different system
FILE_NAME="dps_update_summary.csv"
HDFS_DIR=/archive/$TABLE_NAME

hive -f create_historical_file.hql | sed 's/[\t]/,/g' >> $FILE_NAME || exit 1

    tar --force-local --warning=no-file-changed -czvf $FILE_NAME.tgz  $FILE_NAME --remove-files|| [[ $? -eq 1 ]]
#    echo "
#         moving file to hdfs after clearing
#    "
#		hdfs dfs -mkdir -p ${HDFS_DIR}
#
#    hdfs dfs -rm -skipTrash -r -f ${HDFS_DIR}/*.tgz
#
#		hdfs dfs -moveFromLocal ${FILE_NAME}.tgz ${HDFS_DIR}
#    hdfs dfs -ls ${HDFS_DIR}/${FILE_NAME}.tgz


