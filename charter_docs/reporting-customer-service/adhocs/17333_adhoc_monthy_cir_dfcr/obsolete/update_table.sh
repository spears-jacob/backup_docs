#!/bin/bash
# This script is to run the one_month_metrics.hql query for the desired 
# date range, which is the current fiscal month (in most cases) or the most 
# recent fiscal month (if it's less than 3 days after the end of the fiscal 
# months)



#################   Find start and end dates of the desired date range
######################## using the lookup table so that if the fiscal month changes, it will still use the correct dates

RUNDATE=$(date -d "5 days ago" +%Y-%m-%d) #we want to keep updating a fiscal momth's data until 2 days after that month ends
echo $RUNDATE

FISCALMONTH=$(hive -e "SELECT fiscal_month FROM prod_lkp.chtr_fiscal_month WHERE partition_date='$RUNDATE';")
#echo $FISCALMONTH

#Pull the start date of the relevant fiscal month
FISCALMONTHSTART=$(hive -e "SELECT min(partition_date) FROM prod_lkp.chtr_fiscal_month WHERE fiscal_month='$FISCALMONTH';")

FISCALMONTHEND=$(hive -e "SELECT max(partition_date) FROM prod_lkp.chtr_fiscal_month WHERE fiscal_month='$FISCALMONTH';")
ENDDATE=$(date -d "$FISCALMONTHEND +1 day" +%Y-%m-%d)

echo "Starting data at $FISCALMONTHSTART"
echo "Pulling data up to but not including" $ENDDATE


hive -f  one_month_metrics.hql --hiveconf start_date=$FISCALMONTHSTART --hiveconf end_date=$ENDDATE --hiveconf FISCAL_MONTH=$FISCALMONTH
