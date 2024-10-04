#!/bin/bash
export TMP_DATE=`date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 1 day" +%F`

#--Month Begin Date for previous month (based on rundate)
export MONTH_START_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days + 1 day" +%F`

#--Month End Date for previous month (based on rundate)
export MONTH_END_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days +1 month" +%F`

#--Year-Month for previous month (based on MONTH_START_DATE)
export year_month=`date --date="$MONTH_START_DATE" +%Y-%m`

# hive -e "SELECT * FROM $TMP_db.net_extract_monthly_run_date" > src/net_extract_monthly_run_date.txt
# extract_run_date=`cat src/net_extract_monthly_run_date.txt`

if [ $TMP_DATE != "" ]; then
##	month_begin_date=`date --date="$extract_run_date -1 month" +%Y-%m-%d`
##	month_end_date=`date --date="$extract_run_date -1 day" +%Y-%m-%d`
#	year_month=`date --date="$month_begin_date" +%Y-%m`

echo "### Running .NET Auth Flow Page Visits daily Monthly queries for year-month "$year_month", month begin date $MONTH_START_DATE month end date $MONTH_START_DATE "Run Date" $TMP_DATE"
hive -f src/net_auth_flow_page_visits.hql || exit 1 
echo "### Completed the .NET Auth Flow Page Visits daily Monthly job...."

else 

echo "### ERROR: .NET Auth Flow Page Visits daily Monthly queries ended unsuccessfully -- Please re-run" && exit 1

fi
