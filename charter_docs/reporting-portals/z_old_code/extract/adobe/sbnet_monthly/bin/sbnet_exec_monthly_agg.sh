#!/bin/bash

echo "### Running SB.NET Executive Monthly Agg job for run_date of "$TODAY_DEN

current_month_begin_date=`date --date="$TODAY_DEN -$(($(date -d $TODAY_DEN +%d)-1)) days" +%Y-%m-%d`
echo "### First day of current month: "$current_month_begin_date

if [ "$current_month_begin_date" != "" ]; then
  month_begin_date=`date --date="$current_month_begin_date -1 month" +%Y-%m-%d`
  month_end_date=`date --date="$current_month_begin_date" +%Y-%m-%d`
  year_month=`date --date="$month_begin_date" +%Y-%m`

  # DROP PARTITION FOR CURRENT MONTH IF EXISTS
  echo "##### Dropping current month partitions if they exist (INSTANCES) for year_month = "$year_month
  hive -f src/sbnet_monthly_extracts_drop_partition.hql -hiveconf TABLE_NAME=sbnet_exec_monthly_agg_instances -hiveconf YEAR_MONTH=$year_month
  echo "##### Completed Dropping current month partitions (INSTANCES) ........."

  echo "##### Dropping current month partitions if they exist (VISITS) for year_month = "$year_month
  hive -f src/sbnet_monthly_extracts_drop_partition.hql -hiveconf TABLE_NAME=sbnet_exec_monthly_agg_visits -hiveconf YEAR_MONTH=$year_month
  echo "##### Completed Dropping current month partitions (VISITS) ........."

  # ADD MANUAL subscribers -- ADDED 2017-12-29
  echo "### Running SB.NET Executive Monthly MANUAL SUBSCRIBERS JOB for year_month = "$year_month " ###"
  hive -f src/sbnet_exec_monthly_agg_src_sub_counts.hql -hiveconf YEAR_MONTH=$year_month
  echo "##### Completed SB.NET Executive Monthly Agg job ........."

  # AGGREGATE METRICS BY LEGACY COMPANY INTO AGG TABLE
  echo "### Running SB.NET Executive Monthly Agg job for year_month = "$year_month ", month begin date "$month_begin_date ", month end date "$month_end_date " ###"
  hive -f src/sbnet_exec_monthly_agg.hql -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date -hiveconf YEAR_MONTH=$year_month
  echo "##### Completed SB.NET Executive Monthly Agg job ........."

  # AGGREGATE "TOTAL COMBINED" METRICS ACROSS LEGACY COMPANIES
  echo "### Totaling SB.net Executive Monthly metrics for all Legacy Companies for year_month = "$year_month ", month begin date "$month_begin_date ", month end date "$month_end_date " ###"
  hive -f src/sbnet_exec_monthly_totals.hql -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date -hiveconf YEAR_MONTH=$year_month
  echo "##### Aggregated Totals for Executive Monthly metrics for all Legacy Companies ........."

  # INSERT "TOTAL COMBINED" METRICS INTO AGG TABLE
  echo "### Running Final SB.NET Executive Monthly insertion for year_month = "$year_month ", month begin date "$month_begin_date ", month end date "$month_end_date " ###"
  hive -f src/sbnet_exec_monthly_agg_final.hql -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date -hiveconf YEAR_MONTH=$year_month
  echo "##### Completed Final SB.NET Executive Monthly insertion ........."

  if [ $? -eq 0 ]; then
    echo "### SUCCESS: SB.NET Executive Monthly Agg job finished"
  else
    echo "### ERROR: SB.NET Executive Monthly Agg ended unsuccessfully -- Please re-run" && exit 1
  fi
else
  echo "### ERROR: SB.NET Executive Monthly Agg job aborted due to a null run date" && exit 1
fi
