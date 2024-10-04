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

echo "### Running SB.NET Executive Monthly Pre-Process job for run_date of "$TODAY_DEN

current_month_begin_date=`date --date="$TODAY_DEN -$(($(date -d $TODAY_DEN +%d)-1)) days" +%Y-%m-%d`
echo "### First day of current month: "$current_month_begin_date

if [ "$current_month_begin_date" != "" ]; then
  month_begin_date=`date --date="$current_month_begin_date -1 month" +%Y-%m-%d`
  month_end_date=`date --date="$current_month_begin_date" +%Y-%m-%d`
  year_month=`date --date="$month_begin_date" +%Y-%m`
  
  # LOAD AGGREGATED FED ID DATA
  echo "### Loading Fed ID AUTH Data for year_month = "$year_month ", month begin date "$month_begin_date ", month end date "$month_end_date " ###"
  hive -f src/sbnet_exec_monthly_fed_id.hql -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date -hiveconf YEAR_MONTH=$year_month 
  echo "##### Completed loading SB.NET Executive Monthly src table (VISITS) ........."

  # AGGREGATE DATA IN SRC TABLES
  echo "### Loading SB.NET Executive Monthly src table (INSTANCES) for year_month = "$year_month ", month begin date "$month_begin_date ", month end date "$month_end_date " ###"
  hive -f src/sbnet_exec_monthly_agg_src_instances.hql -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date -hiveconf YEAR_MONTH=$year_month 
  echo "##### Completed loading SB.NET Executive Monthly src table (INSTANCES) ........."

  echo "### Loading SB.NET Executive Monthly src table (VISITS) for year_month = "$year_month ", month begin date "$month_begin_date ", month end date "$month_end_date " ###"
  hive -f src/sbnet_exec_monthly_agg_src_visits.hql -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date -hiveconf YEAR_MONTH=$year_month 
  echo "##### Completed loading SB.NET Executive Monthly src table (VISITS) ........."

  ## PULL DATA THAT IS MANUALLY LOADED INTO SRC TABLES ##
  # SUB COUNTS
  echo "### Loading SMB Sub Counts into SB.NET Executive Monthly src table for year_month = "$year_month ", month begin date "$month_begin_date ", month end date "$month_end_date " ###"
  hive -f src/sbnet_exec_monthly_agg_src_sub_counts.hql -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date -hiveconf YEAR_MONTH=$year_month 
  echo "##### Completed loading SMB Sub Counts into SB.NET Executive Monthly src table ........."

  # BHN SSO METRICS
  echo "### Loading BHN SSO Metrics into SB.NET Executive Monthly src table for year_month = "$year_month ", month begin date "$month_begin_date ", month end date "$month_end_date " ###"
  hive -f src/sbnet_exec_monthly_agg_src_bhn_sso_metrics.hql -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date -hiveconf YEAR_MONTH=$year_month 
  echo "##### Completed loading BHN SSO Metrics into SB.NET Executive Monthly src table ........."

  # BHN ACCOUNTS DATA
  echo "### Loading BHN Accounts Data into SB.NET Executive Monthly src table for year_month = "$year_month ", month begin date "$month_begin_date ", month end date "$month_end_date " ###"
  hive -f src/sbnet_exec_monthly_agg_src_bhn_accounts.hql -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date -hiveconf YEAR_MONTH=$year_month 
  echo "##### Completed loading BHN Accounts Data into SB.NET Executive Monthly src table ........."

  if [ $? -eq 0 ]; then
    echo "### SUCCESS: SB.NET Executive Monthly Pre-Process job finished"
  else
    echo "### ERROR: SB.NET Executive Monthly Pre-Process ended unsuccessfully -- Please re-run" && exit 1
  fi
else
  echo "### ERROR: SB.NET Executive Monthly Pre-Process job aborted due to a null run date" && exit 1
fi
