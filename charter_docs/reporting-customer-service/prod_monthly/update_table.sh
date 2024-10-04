#!/bin/bash
# This script is to run the one_month_metrics.hql query for the desired
# date range, which is the current fiscal month (in most cases) or the most
# recent fiscal month (if it's less than 3 days after the end of the fiscal
# months)




#################   Find start date and fiscal month for the desired date range
######################## using the lookup table so that if the fiscal month changes, it will still use the correct dates

RUNDATE=$(date -d "$RUN_DATE 3 days ago" +%Y-%m-%d) #we want to keep updating a fiscal momth's data until 3 days after that month ends
echo $RUNDATE

FISCALMONTH=$(hive -e "SELECT fiscal_month FROM prod_lkp.chtr_fiscal_month WHERE partition_date='$RUNDATE';")
echo "Running for "$FISCALMONTH

STARTDATE=$(date -d "$RUN_DATE 40 days ago" +%Y-%m-%d) #Need to be sure our subquery includes all days of the fiscal month. 40 days ensures more than enough

hive -f  one_month_metrics.hql --hiveconf FISCAL_MONTH=$FISCALMONTH --hiveconf subquery_start=$STARTDATE

hive -e "SELECT * FROM $ENVIRONMENT.cs_prod_monthly_fiscal_month_metrics WHERE fiscal_month='$FISCALMONTH';"
