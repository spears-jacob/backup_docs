#!/bin/bash
export TMP_DATE=`date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 1 day" +%F`

#--Month Begin Date for previous month (based on rundate)
export MONTH_START_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days + 1 day" +%F`

#--Month End Date for previous month (based on rundate)
export MONTH_END_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days +1 month" +%F`


#extract_run_date=`cat src/net_extract_monthly_run_date.txt`
#month_begin_date=`date --date="$extract_run_date -1 month" +%Y-%m-%d`
#month_end_date=`date --date="$extract_run_date -1 day" +%Y-%m-%d`
year_month=`date --date="$month_begin_date" +%Y-%m`

echo "### Running .NET Create Username Monthly queries for year-month $MONTH_START_DATE, month begin date $MONTH_START_DATE month end date $MONTH_END_DATE Run Date $TMP_DATE"
hive -f src/net_create_username_monthly.hql || exit 1 
echo "### Completed the .NET Create Username Monthly job...."