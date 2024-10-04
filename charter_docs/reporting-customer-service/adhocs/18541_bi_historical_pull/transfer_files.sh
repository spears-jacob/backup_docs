#!/bin/bash
# This script is to make a csv file and send it to a ftp folders

FILE_NAME="dps_update_summary_2019-10-14.csv"

rm $FILE_NAME

hive -f create_file.hql | sed 's/[\t]/,/g' >> $FILE_NAME || exit 1
tar --force-local --warning=no-file-changed -czvf $FILE_NAME.tgz $FILE_NAME || [[ $? -eq 1 ]]


echo "moving file to ftp"
lftp sftp://DLProductdata:JBdE5dhU@files.chartercom.com -e "cd login_data_feed_msa_extract; put -c $FILE_NAME.tgz; bye"
lftp sftp://DLProductdata:JBdE5dhU@files.chartercom.com -e "ls login_data_feed_msa_extract; bye"

