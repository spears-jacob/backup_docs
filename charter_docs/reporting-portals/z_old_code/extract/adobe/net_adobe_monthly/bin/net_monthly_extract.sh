#!/bin/bash
source ~/.bashrc
hive -e "SELECT * FROM $TMP_db.net_extract_monthly_run_date;" > src/net_extract_monthly_run_date.txt
extract_run_date=`cat src/net_extract_monthly_run_date.txt`

echo "### Initiating Process to update the $TMP_db.net_extract_monthly_run_date table for next run....."
extract_run_date=`date --date="$extract_run_date 1 month" +%Y-%m-%d`
hive -e "INSERT OVERWRITE TABLE $TMP_db.net_extract_monthly_run_date VALUES('$extract_run_date');"
echo "### Completed Process to update the $TMP_db.net_extract_monthly_run_date table for next run....."

rm src/net_extract_monthly_run_date.txt
