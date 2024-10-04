#!/bin/bash
export RUN_DATE=$1
export ENVIRONMENT=$2
export GLOBAL_DB=${ENVIRONMENT}
export DASP_db=${ENVIRONMENT}_dasp
export TMP_db=${3}_dasp
export ARTIFACTS_PATH=$4
export SCRIPT_VERSION=$5
# export ENVIRONMENT=stg
#TODO: swich these back to variables

hdfs dfs -get ${ARTIFACTS_PATH}/scripts ./scripts
hdfs dfs -get ${ARTIFACTS_PATH}/hql ./hql
############## Set Variables  #################################################
tablename=cs_call_care_data
oldtablesuffix="3"

oldtablename=

lastfortnightdate=`date -d "$RUN_DATE -14 day" +%Y-%m-%d`

lastweekdate=`date -d "$RUN_DATE -7 day" +%Y-%m-%d`
#lastweektablename=${ENVIRONMENT}_dasp.${tablename}_"$(echo "$formattedlastweek" | sed 's/-//g')"

# yesterdaydate=`date -d "$RUN_DATE -1 day" +%Y-%m-%d`
yesterdayday=`date -d "$RUN_DATE -1 day" +%a`
yesterdaytablename="${ENVIRONMENT}_dasp.${tablename}_${yesterdayday}"

# todaydate=`date -d "$RUN_DATE" +%Y-%m-%d`
todayday=`date -d "$RUN_DATE" +%a`
todaytablename=${ENVIRONMENT}_dasp.${tablename}_${todayday}

##### Copy today's table so we'll have it around for comparison tomorrow ######
echo "Copying to "$todaytablename
#TODO: this should be truncate and insert, instead of just insert
copyquery="INSERT OVERWRITE TABLE $todaytablename partition (call_end_date_utc, call_end_date_east) SELECT * FROM $ENVIRONMENT.atom_cs_call_care_data_3 WHERE call_end_date_utc>='$lastfortnightdate'; MSCK REPAIR TABLE $todaytablename;"
echo "$copyquery"
 hive -e "$copyquery"

NEWCALLTABLE=$ENVIRONMENT.atom_${tablename}_${oldtablesuffix}
OLDCALLTABLE=$yesterdaytablename
TESTDATE=`date -d "$RUN_DATE -8 day" +%Y-%m-%d`

echo "comparing
$OLDCALLTABLE
to
$NEWCALLTABLE
for $TESTDATE"

#Later TODO: Add these back in after we've got the code running on our tables
#?? Need to copy yesterday's tabels of these also?
# OLDVISITSTABLE=stg_dasp.cs_calls_with_prior_visits_prod_copy
 NEWVISITSTABLE=${DASP_db}.cs_calls_with_prior_visits
#
# OLDCIRTABLE=stg_dasp.cs_call_in_rate
 NEWCIRTABLE=${DASP_db}.cs_call_in_rate

echo "***"
echo "1_as_ns_check_prior_visit_segment_counts"
hive -f ./hql/1_as_ns_check_prior_visit_segment_counts-${SCRIPT_VERSION}.hql --hiveconf new_visits_table=$NEWVISITSTABLE --hiveconf new_call_table=$NEWCALLTABLE --hiveconf test_date=$TESTDATE
echo "***"

echo "***"
echo "2_as_sdd_dispo_data_discrepancies"
hive -f ./hql/2_as_sdd_dispo_data_discrepancies-${SCRIPT_VERSION}.hql --hiveconf new_call_table=$NEWCALLTABLE --hiveconf old_call_table=$OLDCALLTABLE --hiveconf test_date=$TESTDATE
echo "***"

echo "***"
echo "1_cir_cd_check"
hive -f ./hql/1_cir_cd_check-${SCRIPT_VERSION}.hql --hiveconf new_visits_table=$NEWVISITSTABLE --hiveconf new_cir_table=$NEWCIRTABLE --hiveconf new_call_table=$NEWCALLTABLE --hiveconf start_date=$lastfortnightdate --hiveconf end_date=$lastweekdate --hiveconf daspdb=$DASP_db
echo "***"
#
# echo "***"
# echo "2_cir_od_check"
# hive -f ./hql/2_cir_od_check-${SCRIPT_VERSION}.hql --hiveconf old_cir_table=$OLDCIRTABLE --hiveconf new_cir_table=$NEWCIRTABLE --hiveconf test_date=$TESTDATE
# echo "***"

echo "***"
echo "2_mc_check_mso_counts"
echo "2019-11-22: We do expect this to change but proportions should be close"
hive -f ./hql/2_mc_check_mso_counts-${SCRIPT_VERSION}.hql --hiveconf test_date=$TESTDATE --hiveconf old_call_table=$OLDCALLTABLE --hiveconf new_call_table=$NEWCALLTABLE
echo "***"

echo "***"
echo "2_sd_vol_call_volume_check.hql"
hive -f ./hql/2_sd_vol_call_volume_check-${SCRIPT_VERSION}.hql --hiveconf test_date=$TESTDATE --hiveconf old_call_table=$OLDCALLTABLE --hiveconf new_call_table=$NEWCALLTABLE
echo "***"

echo "***"
echo "1_sd_ak_check_account_key_by_segment_id"
hive -f ./hql/1_sd_ak_check_account_key_by_segment_id-${SCRIPT_VERSION}.hql --hiveconf new_call_table=$NEWCALLTABLE --hiveconf test_date=$lastfortnightdate
echo "***"

echo "***"
echo "1_sd_ct_check"
hive -f ./hql/1_sd_ct_check-${SCRIPT_VERSION}.hql --hiveconf new_call_table=$NEWCALLTABLE --hiveconf test_date=$lastfortnightdate
echo "***"

echo "***"
echo "3_sd_ts_average_endtime_discrepancy.hql"
hive -f ./hql/3_sd_ts_average_endtime_discrepancy-${SCRIPT_VERSION}.hql --hiveconf old_call_table=$OLDCALLTABLE --hiveconf new_call_table=$NEWCALLTABLE --hiveconf start_date=$lastfortnightdate --hiveconf end_date=$lastweekdate
echo "***"

#TODO: make it possible to pass this in instead of hardcoding it in the script field_array=("call_inbound_key" "call_id" "call_start_date_utc" "call_start_time_utc" "call_end_time_utc" "call_start_datetime_utc" "call_end_datetime_utc" "call_start_timestamp_utc" "call_end_timestamp_utc" "segment_id" "segment_number" "segment_status_disposition" "segment_start_time_utc" "segment_end_time_utc" "segment_start_datetime_utc" "segment_end_datetime_utc" "segment_start_timestamp_utc" "segment_end_timestamp_utc" "segment_duration_seconds" "segment_duration_minutes" "segment_handled_flag" "customer_call_count_indicator" "call_handled_flag" "call_owner" "product" "account_number" "customer_account_number" "customer_type" "customer_subtype" "truck_roll_flag" "notes_txt" "resolution_description" "cause_description" "issue_description" "company_code" "service_call_tracker_id" "created_on" "created_by" "phone_number_from_tracker" "call_type" "split_sum_desc" "location_name" "care_center_management_name" "agent_job_role_name" "agent_effective_hire_date" "agent_mso" "eduid" "last_handled_segment_flag" "record_update_timestamp" "source" "enhanced_account_number" "previous_call_time_utc" "call_end_date_utc")
bash ./scripts/2_sd_fd_check_field_discrepancies-${SCRIPT_VERSION}.sh $OLDCALLTABLE $NEWCALLTABLE $TESTDATE
