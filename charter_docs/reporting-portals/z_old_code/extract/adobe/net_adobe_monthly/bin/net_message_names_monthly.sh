#!/bin/bash
source ~/.bashrc
hive -e "SELECT * FROM $TMP_db.net_extract_monthly_run_date;" > src/net_extract_monthly_run_date.txt
extract_run_date=`cat src/net_extract_monthly_run_date.txt`
start_date=`date --date="$extract_run_date -1 month" +%Y-%m-%d`
end_date=`date --date="$extract_run_date -1 day" +%Y-%m-%d`

echo "### Running net_message_names_monthly job for run date "$extract_run_date", start date "$start_date", end date "$end_date
hive -f src/net_message_names_monthly.hql -hiveconf TMP=$TMP_db -hiveconf LKP=$LKP_db  -hiveconf DB=$1 -hiveconf RUN_DATE=$extract_run_date -hiveconf START_DATE=$start_date -hiveconf END_DATE=$end_date
echo "### Completed the net_message_names_monthly job...."

rm src/net_extract_monthly_run_date.txt
