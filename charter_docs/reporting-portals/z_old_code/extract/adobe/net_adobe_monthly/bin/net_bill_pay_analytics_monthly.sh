 #!/bin/bash
source ~/.bashrc
hive -e "SELECT * FROM $TMP_db.net_extract_monthly_run_date" > src/net_extract_monthly_run_date.txt
extract_run_date=`cat src/net_extract_monthly_run_date.txt`

if [ "$extract_run_date" != "" ]; then
	month_begin_date=`date --date="$extract_run_date -1 month" +%Y-%m-%d`
	month_end_date=`date --date="$extract_run_date -1 day" +%Y-%m-%d`
	year_month=`date --date="$month_begin_date" +%Y-%m`

	echo "### Running Bill Pay Analytics - NET monthly queries for year-month "$year_month", month begin date "$month_begin_date "month end date" $month_end_date "Run Date" $extract_run_date
	hive -f src/net_bill_pay_analytics_monthly.hql -hiveconf TMP=$TMP_db -hiveconf LKP=$LKP_db -hiveconf DB=$1 -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date
	echo "End of Bill Pay Analytics - NET monthly job"

	echo "### Running Bill Pay Analytics - STVA monthly queries for year-month "$year_month", month begin date "$month_begin_date "month end date" $month_end_date "Run Date" $extract_run_date
	hive -f src/net_bill_pay_analytics_STVA_monthly.hql -hiveconf TMP=$TMP_db -hiveconf LKP=$LKP_db -hiveconf DB=$1 -hiveconf MONTH_START_DATE=$month_begin_date -hiveconf MONTH_END_DATE=$month_end_date
	echo "End of Bill Pay Analytics - STVA monthly job"

	echo "### Completed the .NET Bill Pay Analytics monthly job...."
else
	echo "### .NET Bill Pay Analytics monthly job aborted due to a null run date"
fi
rm src/net_extract_monthly_run_date.txt
