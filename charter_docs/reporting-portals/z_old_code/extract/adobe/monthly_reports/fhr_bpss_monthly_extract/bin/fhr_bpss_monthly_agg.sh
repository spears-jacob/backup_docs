#!/bin/bash
export TMP_DATE=`date -d "$RUN_DATE -$(date -d $RUN_DATE +%d) days - 1 day" +%F`

#--Month Begin Date for previous month (based on rundate)
export MONTH_START_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days + 1 day" +%F`

#--Month End Date for previous month (based on rundate)
export MONTH_END_DATE=`date -d "$TMP_DATE -$(date -d $TMP_DATE +%d) days +1 month" +%F`

#--Year and Month of current reporting period (based on MONTH_START_DATE)
export YEAR_MONTH=`date --date="$MONTH_START_DATE" +%Y-%m`

#source ~/.bashrc

echo "### Dropping Partition for year_month $YEAR_MONTH"
hive -f src/fhr_bpss_drop_partition.hql -hiveconf TABLE_NAME=fhr_monthly_bpss_metrics_agg
echo "### Completed Dropping Partition for year_month $YEAR_MONTH...."

echo "### Running fhr_bpss_metrics_agg script for start date $MONTH_START_DATE, end date $MONTH_END_DATE"
hive -f src/fhr_bpss_metrics_agg.hql 
echo "### Completed the fhr_bpss_metrics_agg script...."
