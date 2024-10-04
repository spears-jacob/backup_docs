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

echo "### Running percent_account_creation_success_on_chtr_network job for  start date $MONTH_START_DATE, end date $MONTH_END_DATE"
hive -f src/src_percent_calcs/percent_account_creation_success_on_chtr_network.hql || exit 1 
echo "### Completed the percent_account_creation_success_on_chtr_network job...."
