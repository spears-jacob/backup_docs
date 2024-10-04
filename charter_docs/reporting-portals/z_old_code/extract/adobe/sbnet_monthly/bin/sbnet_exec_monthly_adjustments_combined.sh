#!/bin/bash
export TMP_DATE=`date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 1 day" +%F`

#--Month Begin Date for previous month (based on rundate)
export MONTH_START_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days + 1 day" +%F`

#--Month End Date for previous month (based on rundate)
export MONTH_END_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days +1 month" +%F`

#--Year and Month of current reporting period (based on MONTH_START_DATE)
export YEAR_MONTH=`date --date="$MONTH_START_DATE" +%Y-%m`

#source ~/.bashrc

echo "### Running sbnet_exec_monthly_adjustments_combined job for  start date $MONTH_START_DATE, end date $MONTH_END_DATE"
hive -f src/sbnet_exec_monthly_adjustments_combined.hql || exit 1 
echo "### Completed the sbnet_exec_monthly_adjustments_combined job...."
