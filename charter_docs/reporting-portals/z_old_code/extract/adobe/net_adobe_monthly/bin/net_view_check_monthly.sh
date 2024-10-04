#!/bin/bash
source ~/.bashrc
hive -e "SELECT * FROM $TMP_db.net_extract_monthly_run_date;" > src/net_view_run_date.txt
net_view_last_run_date=`cat src/net_view_run_date.txt`
if [ "$net_view_last_run_date" != "" ]; then
	month_begin_date=`date --date="$net_view_last_run_date -1 month" +%Y-%m-%d`
	month_end_date=`date --date="$net_view_last_run_date -1 day" +%Y-%m-%d`
	year_month=`date --date="$month_begin_date" +%Y-%m`

	echo -e "\n\n    --- Running net_views_agg_monthly queries for year-month "$year_month", month begin date "$month_begin_date ", month end date " $month_end_date " ----  \n\n"
	hive -f src/net_views_agg_monthly.hql -hiveconf TMP=$TMP_db -hiveconf LKP=$LKP_db -hiveconf DB=$1 -hiveconf LAST_MONTH_START_DATE=$month_begin_date -hiveconf LAST_MONTH_END_DATE=$month_end_date -hiveconf LAST_YEAR_MONTH=$year_month 
	echo -e "\n\n    --- Completed the net_views_agg_monthly job....\n\n"

fi
