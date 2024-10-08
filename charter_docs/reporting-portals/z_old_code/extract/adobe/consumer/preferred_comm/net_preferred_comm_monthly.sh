#!/bin/bash

#--Run Date
export TMP_DATE=`date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 1 day" +%F`

echo $TMP_DATE

#--Month Begin Date for previous month (based on rundate)
export MONTH_START_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days + 1 day" +%F`

echo $MONTH_START_DATE

#--Month End Date for previous month (based on rundate)
export MONTH_END_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days +1 month" +%F`

echo $MONTH_END_DATE

#--Year and Month of current reporting period (based on MONTH_START_DATE)
export YEAR_MONTH=`date --date="$MONTH_START_DATE" +%Y-%m`

echo $YEAR_MONTH

#--Year and Month with a wildcard for day based on rundate
export YEAR_MONTH_WC="$YEAR_MONTH"%

echo $YEAR_MONTH_WC

echo "### Initiating net_preferred_comm_monthly query for year-month" $MONTH_START_DATE
hive -f net_preferred_comm_monthly.hql || exit 1 
echo "### Completed the net_preferred_comm_monthly job...."
