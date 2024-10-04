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

echo "### Initiating net_extract_reporting_monthly preprocess queries for $extract_year_month"


#------------------------------net_hhs_pvs_links_monthly
echo "########################### Initiating the preprocess to check and load data into net_hhs_pvs_links_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_hhs_pvs_links_monthly WHERE year_month = $extract_year_month" > src/net_hhs_pvs_links_monthly_cnt.txt
net_hhs_pvs_links_monthly_cnt=` cat src/net_hhs_pvs_links_monthly_cnt.txt`

if [ "$net_hhs_pvs_links_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_hhs_pvs_links_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=year_month -hiveconf COLUMN_DATE2=year_month -hiveconf RUN_YEAR=$extract_year_month -hiveconf RUN_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_hhs_pvs_links_monthly 
	echo "##### Completed net_hhs_pvs_links_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_hhs_pvs_links_monthly for $TMP_DATE"
fi
rm src/net_hhs_pvs_links_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_hhs_pvs_links_monthly"


#------------------------------net_hhs_sections_monthly
echo "########################### Initiating the preprocess to check and load data into net_hhs_sections_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_hhs_sections_monthly WHERE year_month = $extract_year_month" > src/net_hhs_sections_monthly_cnt.txt
net_hhs_sections_monthly_cnt=` cat src/net_hhs_sections_monthly_cnt.txt`

if [ "$net_hhs_sections_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_hhs_sections_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=year_month -hiveconf COLUMN_DATE2=year_month -hiveconf RUN_YEAR=$extract_year_month -hiveconf RUN_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_hhs_sections_monthly
	echo "##### Completed net_hhs_sections_monthly preprocess job successfully....."
else
     echo "##### No pre-existing data in table net_hhs_sections_monthly for $TMP_DATE"
 fi
 rm src/net_hhs_sections_monthly_cnt.txt
 echo "########################### Finished the preprocess to check and load data into net_hhs_sections_monthly"


#------------------------------net_return_frequency_monthly
echo "########################### Initiating the preprocess to check and load data into net_return_frequency_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_return_frequency_monthly WHERE year_month = $extract_year_month" > src/net_return_frequency_monthly_cnt.txt
net_return_frequency_monthly_cnt=` cat src/net_return_frequency_monthly_cnt.txt`

if [ "$net_return_frequency_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_return_frequency_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=year_month -hiveconf COLUMN_DATE2=year_month -hiveconf RUN_YEAR=$extract_year_month -hiveconf RUN_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_return_frequency_monthly
	echo "##### Completed net_return_frequency_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_return_frequency_monthly for $TMP_DATE"
fi
rm src/net_return_frequency_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_return_frequency_monthly"


#------------------------------net_visit_metrics_monthly
echo "########################### Initiating the preprocess to check and load data into net_visit_metrics_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_visit_metrics_monthly WHERE year_month = $extract_year_month" > src/net_visit_metrics_monthly_cnt.txt
net_visit_metrics_monthly_cnt=` cat src/net_visit_metrics_monthly_cnt.txt`

if [ "$net_visit_metrics_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_visit_metrics_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=year_month -hiveconf COLUMN_DATE2=year_month -hiveconf RUN_YEAR=$extract_year_month -hiveconf RUN_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_visit_metrics_monthly
	echo "##### Completed net_visit_metrics_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_visit_metrics_monthly for $TMP_DATE"
fi
rm src/net_visit_metrics_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_return_frequency_monthly"


#------------------------------net_hh_total_monthly
echo "########################### Initiating the preprocess to check and load data into net_hh_total_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_hh_total_monthly WHERE year_month = $extract_year_month" > src/net_hh_total_monthly_cnt.txt
net_hh_total_monthly_cnt=` cat src/net_hh_total_monthly_cnt.txt`

if [ "$net_hh_total_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_hh_total_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql  -hiveconf COLUMN_DATE1=year_month -hiveconf COLUMN_DATE2=year_month -hiveconf RUN_YEAR=$extract_year_month -hiveconf RUN_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_hh_total_monthly
	echo "##### Completed net_hh_total_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_hh_total_monthly for $TMP_DATE"
fi
rm src/net_hh_total_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_hh_total_monthly"


#------------------------------net_hh_account_monthly
echo "########################### Initiating the preprocess to check and load data into net_hh_account_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_hh_account_monthly WHERE year_month = $extract_year_month" > src/net_hh_account_monthly_cnt.txt
net_hh_account_monthly_cnt=` cat src/net_hh_account_monthly_cnt.txt`

if [ "$net_hh_account_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_hh_account_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql  -hiveconf COLUMN_DATE1=year_month -hiveconf COLUMN_DATE2=year_month -hiveconf RUN_YEAR=$extract_year_month -hiveconf RUN_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_hh_account_monthly
	echo "##### Completed net_hh_account_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_hh_account_monthly for $TMP_DATE"
fi
rm src/net_hh_account_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_hh_account_monthly"


#------------------------------net_message_names_monthly
echo "########################### Initiating the preprocess to check and load data into net_message_names_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_message_names_monthly WHERE year_month = $extract_year_month" > src/net_message_names_monthly_cnt.txt
net_message_names_monthly_cnt=` cat src/net_message_names_monthly_cnt.txt`

if [ "$net_message_names_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_message_names_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=year_month -hiveconf COLUMN_DATE2=year_month -hiveconf RUN_YEAR=$extract_year_month -hiveconf RUN_MONTH=$extract_year_month  -hiveconf TABLE_NAME=net_message_names_monthly
	echo "##### Completed net_message_names_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_message_names_monthly for $TMP_DATE"
fi
rm src/net_message_names_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_message_names_monthly"


#------------------------------net_reset_password_monthly
echo "########################### Initiating the preprocess to check and load data into net_reset_password_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_reset_password_monthly WHERE RUN_YEAR=$extract_year AND RUN_MONTH=$extract_month" > src/net_reset_password_monthly_cnt.txt
net_reset_password_monthly_cnt=` cat src/net_reset_password_monthly_cnt.txt`

if [ "$net_reset_password_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_reset_password_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=run_year -hiveconf COLUMN_DATE2=run_month -hiveconf RUN_YEAR=$extract_year -hiveconf RUN_MONTH=$extract_month -hiveconf TABLE_NAME=net_reset_password_monthly
	echo "##### Completed net_reset_password_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_reset_password_monthly for $TMP_DATE"
fi
rm src/net_reset_password_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_reset_password_monthly"


#------------------------------net_reset_user_monthly
echo "########################### Initiating the preprocess to check and load data into net_reset_user_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_reset_user_monthly WHERE RUN_YEAR=$extract_year AND RUN_MONTH=$extract_month" > src/net_reset_user_monthly_cnt.txt
net_reset_user_monthly_cnt=` cat src/net_reset_user_monthly_cnt.txt`

if [ "$net_reset_user_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_reset_user_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=run_year -hiveconf COLUMN_DATE2=run_month -hiveconf RUN_YEAR=$extract_year -hiveconf RUN_MONTH=$extract_month -hiveconf TABLE_NAME=net_reset_user_monthly
	echo "##### Completed net_reset_user_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_reset_user_monthly for $TMP_DATE"
fi
rm src/net_reset_user_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_reset_user_monthly"


#------------------------------net_zip_metrics_monthly
echo "########################### Initiating the preprocess to check and load data into net_zip_metrics_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_zip_metrics_monthly WHERE year_month = $extract_year_month" > src/net_zip_metrics_monthly_cnt.txt
net_zip_metrics_monthly_cnt=` cat src/net_zip_metrics_monthly_cnt.txt`

if [ "$net_zip_metrics_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_zip_metrics_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=year_month -hiveconf COLUMN_DATE2=year_month -hiveconf RUN_YEAR=$extract_year_month -hiveconf RUN_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_zip_metrics_monthly
	echo "##### Completed net_zip_metrics_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_zip_metrics_monthly for $TMP_DATE"
fi
rm src/net_zip_metrics_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_zip_metrics_monthly"

#------------------------------net_auth_flow_page_visits_monthly
echo "########################### Initiating the preprocess to check and load data into net_auth_flow_page_visits_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_auth_flow_page_visits_monthly WHERE year_month = $extract_year_month" > src/net_auth_flow_page_visits_monthly_cnt.txt
net_auth_flow_page_visits_monthly_cnt=` cat src/net_auth_flow_page_visits_monthly_cnt.txt`

if [ "$net_auth_flow_page_visits_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_auth_flow_page_visits_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_drop_partition.hql -hiveconf YEAR_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_auth_flow_page_visits_monthly
	echo "##### Completed net_auth_flow_page_visits_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_auth_flow_page_visits_monthly for $TMP_DATE"
fi
rm src/net_auth_flow_page_visits_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_auth_flow_page_visits_monthly"

#------------------------------net_network_status_monthly
echo "########################### Initiating the preprocess to check and load data into net_network_status_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_network_status_monthly WHERE year_month = $extract_year_month" > src/net_network_status_monthly_cnt.txt
net_network_status_monthly_cnt=` cat src/net_network_status_monthly_cnt.txt`

if [ "$net_network_status_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_network_status_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=year_month -hiveconf COLUMN_DATE2=year_month -hiveconf RUN_YEAR=$extract_year_month -hiveconf RUN_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_network_status_monthly
	echo "##### Completed net_network_status_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_network_status_monthly for $TMP_DATE"
fi
rm src/net_network_status_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_network_status_monthly"

##------------------------------net_visits_agg_monthly
#echo "########################### Initiating the preprocess to check and load data into net_visits_agg_monthly"
#hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_visits_agg_monthly WHERE year_month = $extract_year_month" > src/net_visits_agg_monthly_cnt.txt
#net_visits_agg_monthly_cnt=` cat src/net_visits_agg_monthly_cnt.txt`
#
#if [ "$net_visits_agg_monthly_cnt" -gt 0 ]; then
#	echo "##### Initiating net_visits_agg_monthly preprocess job ........."
#	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=year_month -hiveconf COLUMN_DATE2=year_month -hiveconf RUN_YEAR=$extract_year_month -hiveconf RUN_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_visits_agg_monthly
#	echo "##### Completed net_visits_agg_monthly preprocess job successfully....."
#else
#    echo "##### No pre-existing data in table net_visits_agg_monthly for $TMP_DATE"
#fi
#rm src/net_visits_agg_monthly_cnt.txt
#echo "########################### Finished the preprocess to check and load data into net_visits_agg_monthly"
#
##------------------------------net_views_agg_monthly
#echo "########################### Initiating the preprocess to check and load data into net_views_agg_monthly"
#hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_views_agg_monthly WHERE year_month = $extract_year_month" > src/net_views_agg_monthly_cnt.txt
#net_views_agg_monthly_cnt=` cat src/net_views_agg_monthly_cnt.txt`
#
#if [ "$net_views_agg_monthly_cnt" -gt 0 ]; then
#	echo "##### Initiating net_views_agg_monthly preprocess job ........."
#	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=year_month -hiveconf COLUMN_DATE2=year_month -hiveconf RUN_YEAR=$extract_year_month -hiveconf RUN_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_views_agg_monthly
#	echo "##### Completed net_views_agg_monthly preprocess job successfully....."
#else
#    echo "##### No pre-existing data in table net_views_agg_monthly for $TMP_DATE"
#fi
#rm src/net_views_agg_monthly_cnt.txt
#echo "########################### Finished the preprocess to check and load data into net_zip_metrics_monthly"

#------------------------------net_products_agg_monthly
echo "########################### Initiating the preprocess to check and load data into net_products_agg_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_products_agg_monthly WHERE year_month = $extract_year_month" > src/net_products_agg_monthly_cnt.txt
net_products_agg_monthly_cnt=` cat src/net_products_agg_monthly_cnt.txt`

if [ "$net_products_agg_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_products_agg_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_drop_partition.hql #-hiveconf TMP=$TMP_db -hiveconf LKP=$LKP_db -hiveconf DB=$ENVIRONMENT -hiveconf YEAR_MONTH==$extract_year_month -hiveconf TABLE_NAME=net_products_agg_monthly
	echo "##### Completed net_products_agg_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_products_agg_monthly for $TMP_DATE"
fi
rm src/net_products_agg_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_products_agg_monthly"


#------------------------------net_create_username_monthly
echo "########################### Initiating the preprocess to check and load data into net_create_username_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_create_username_monthly WHERE RUN_YEAR=$extract_year AND RUN_MONTH=$extract_month" > src/net_create_username_monthly_cnt.txt
net_create_username_monthly_cnt=` cat src/net_create_username_monthly_cnt.txt`

if [ "$net_create_username_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_create_username_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_preprocess.hql -hiveconf COLUMN_DATE1=run_year -hiveconf COLUMN_DATE2=run_month -hiveconf RUN_YEAR=$extract_year -hiveconf RUN_MONTH=$extract_month -hiveconf TABLE_NAME=net_create_username_monthly
	echo "##### Completed net_create_username_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_create_username_monthly for $TMP_DATE"
fi
rm src/net_create_username_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_create_username_monthly"

#------------------------------net_bill_pay_analytics_monthly
echo "########################### Initiating the preprocess to check and load data into net_bill_pay_analytics_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_bill_pay_analytics_monthly WHERE year_month = $extract_year_month" > src/net_bill_pay_analytics_monthly_cnt.txt
net_bill_pay_analytics_monthly_cnt=` cat src/net_bill_pay_analytics_monthly_cnt.txt`

if [ "$net_bill_pay_analytics_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_bill_pay_analytics_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_drop_partition.hql -hiveconf YEAR_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_bill_pay_analytics_monthly
	echo "##### Completed net_bill_pay_analytics_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_bill_pay_analytics_monthly for $TMP_DATE"
fi
rm src/net_bill_pay_analytics_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_bill_pay_analytics_monthly"

#------------------------------net_bill_pay_analytics_STVA_monthly
echo "########################### Initiating the preprocess to check and load data into net_bill_pay_analytics_STVA_monthly"
hive -e "SELECT COUNT(*) FROM $ENVIRONMENT.net_bill_pay_analytics_STVA_monthly WHERE year_month = $extract_year_month" > src/net_bill_pay_analytics_STVA_monthly_cnt.txt
net_bill_pay_analytics_STVA_monthly_cnt=` cat src/net_bill_pay_analytics_STVA_monthly_cnt.txt`

if [ "$net_bill_pay_analytics_STVA_monthly_cnt" -gt 0 ]; then
	echo "##### Initiating net_bill_pay_analytics_STVA_monthly preprocess job ........."
	hive -f src/net_extract_reporting_monthly_drop_partition.hql -hiveconf YEAR_MONTH=$extract_year_month -hiveconf TABLE_NAME=net_bill_pay_analytics_STVA_monthly
	echo "##### Completed net_bill_pay_analytics_STVA_monthly preprocess job successfully....."
else
    echo "##### No pre-existing data in table net_bill_pay_analytics_STVA_monthly for $TMP_DATE"
fi
rm src/net_bill_pay_analytics_STVA_monthly_cnt.txt
echo "########################### Finished the preprocess to check and load data into net_bill_pay_analytics_STVA_monthly"

echo "########################### Finished all tables preprocessing###########"
