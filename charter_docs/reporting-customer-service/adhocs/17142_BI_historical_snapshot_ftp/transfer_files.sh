#!/bin/bash
# This script is to make a csv file and send it to a ftp folders

END_DATE=$(date -d "yesterday" +%Y-%m-%d)
FILE_NAME="dps_update_summary.csv"


rm $FILE_NAME

hive -f create_historical_file.hql --hiveconf end_date=$END_DATE | sed 's/[\t]/,/g' >> $FILE_NAME || exit 1
tar --force-local --warning=no-file-changed -czvf $FILE_NAME.tgz  $FILE_NAME --remove-files|| [[ $? -eq 1 ]]


echo "moving file to ftp"
lftp sftp://DLProductdata:JBdE5dhU@files.chartercom.com -e "cd login_data_feed_msa_extract; put -c $FILE_NAME.tgz; bye"
lftp sftp://DLProductdata:JBdE5dhU@files.chartercom.com -e "ls login_data_feed_msa_extract; bye"

