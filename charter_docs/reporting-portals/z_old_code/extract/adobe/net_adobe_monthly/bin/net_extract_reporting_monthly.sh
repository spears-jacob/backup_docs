#!/bin/bash
export TMP_DATE=`date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 1 day" +%F`

#--Month Begin Date for previous month (based on rundate)
export MONTH_START_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days + 1 day" +%F`

#--Month End Date for previous month (based on rundate)
export MONTH_END_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days +1 month" +%F`

export YEAR_MONTH=`date --date="$MONTH_START_DATE" +%Y-%m`

#hive -e "SELECT * FROM $TMP_db.net_extract_monthly_run_date;" > src/net_extract_monthly_run_date.txt
#extract_run_date=`cat src/net_extract_monthly_run_date.txt`
#month_begin_date=`date --date="$extract_run_date -1 month" +%Y-%m-%d`
#month_end_date=`date --date="$extract_run_date -1 day" +%Y-%m-%d`



echo "### Running net_extract_reporting_monthly queries for month begin date $MONTH_START_DATE month end date $MONTH_END_DATE Run Date $TMP_DATE"
hive -f src/net_extract_reporting_monthly.hql  || exit 1 #-hiveconf TMP=$TMP_db -hiveconf LKP=$LKP_db -hiveconf DB=$1 -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date -hiveconf YEAR_MONTH=$year_month
echo "### Completed the net_extract_reporting_monthly job...."

#rm src/net_extract_monthly_run_date.txt
