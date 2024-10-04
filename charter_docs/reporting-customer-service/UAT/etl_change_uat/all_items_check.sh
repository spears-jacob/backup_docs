#!/bin/bash

#set these variables to whatever tables you're comparing
OLDCALLTABLE=prod.cs_call_care_data
NEWCALLTABLE=test.cs_call_care_data_1075_2_amo
OLDVISITSTABLE=prod.cs_calls_with_prior_visits
NEWVISITSTABLE=test.cs_calls_with_prior_visits_2_1075_amo
OLDCIRTABLE=prod.cs_call_in_rate
NEWCIRTABLE=test.cs_call_in_rate_1075_2_amo
#todo create variables that query test table for date range and can be subbed into
## mc_check_mso_counts

rm all_items_check_results.txt

echo "as_ns_check_prior_visit_segment_counts" >> all_items_check_results.txt
hive -f as_ns_check_prior_visit_segment_counts.hql --hiveconf new_visits_table=$NEWVISITSTABLE --hiveconf new_call_table=$NEWCALLTABLE >> all_items_check_results.txt
echo "\n***\n" >> all_items_check_results.txt

echo "as_sdd_dispo_data_discrepancies" >> all_items_check_results.txt
hive -f as_sdd_dispo_data_discrepancies.hql --hiveconf new_call_table=$NEWCALLTABLE --hiveconf old_call_table=$OLDCALLTABLE >> all_items_check_results.txt
echo "\n***\n" >> all_items_check_results.txt

echo "cir_cd_check" >> all_items_check_results.txt
hive -f cir_cd_check.hql --hiveconf new_visits_table=$NEWVISITSTABLE --hiveconf new_cir_table=$NEWCIRTABLE --hiveconf new_call_table=$NEWCALLTABLE >> all_items_check_results.txt
echo "\n***\n" >> all_items_check_results.txt

echo "cir_od_check" >> all_items_check_results.txt
hive -f cir_od_check.hql --hiveconf old_cir_table=$OLDCIRTABLE --hiveconf new_cir_table=$NEWCIRTABLE >> all_items_check_results.txt
echo "\n***\n" >> all_items_check_results.txt

echo "mc_check_mso_counts" >> all_items_check_results.txt
hive -f mc_check_mso_counts.hql --hiveconf old_table=$OLDCALLTABLE --hiveconf new_table=$NEWCALLTABLE >> all_items_check_results.txt
echo "\n***\n" >> all_items_check_results.txt

echo "sd_ak_check_account_key_by_segment_id" >> all_items_check_results.txt
hive -f sd_ak_check_account_key_by_segment_id.hql --hiveconf new_call_table=$NEWCALLTABLE >> all_items_check_results.txt
echo "\n***\n" >> all_items_check_results.txt

#we agreed to move this to a different ticket
echo "sd_ct_check" >> all_items_check_results.txt
hive -f sd_ct_check.hql --hiveconf new_call_table=$NEWCALLTABLE >> all_items_check_results.txt
echo "\n***\n" >> all_items_check_results.txt

echo "sd_et_no_negative_durations_check" >> all_items_check_results.txt
hive -f sd_et_no_negative_durations_check.hql --hiveconf new_call_table=$NEWCALLTABLE >> all_items_check_results.txt
echo "\n***\n" >> all_items_check_results.txt

#we agreed to move this to a different ticket
#hive -e "DESCRIBE test.cs_calls_with_prior_visits_amo_july;" | grep visit_id
#echo "There should be something after the OK ^^.  If not, fails on vi"

##TODO: make it possible to pass this in instead of hardcoding it in the script field_array=("call_inbound_key" "call_id" "call_start_date_utc" "call_start_time_utc" "call_end_time_utc" "call_start_datetime_utc" "call_end_datetime_utc" "call_start_timestamp_utc" "call_end_timestamp_utc" "segment_id" "segment_number" "segment_status_disposition" "segment_start_time_utc" "segment_end_time_utc" "segment_start_datetime_utc" "segment_end_datetime_utc" "segment_start_timestamp_utc" "segment_end_timestamp_utc" "segment_duration_seconds" "segment_duration_minutes" "segment_handled_flag" "customer_call_count_indicator" "call_handled_flag" "call_owner" "product" "account_number" "customer_account_number" "customer_type" "customer_subtype" "truck_roll_flag" "notes_txt" "resolution_description" "cause_description" "issue_description" "company_code" "service_call_tracker_id" "created_on" "created_by" "phone_number_from_tracker" "call_type" "split_sum_desc" "location_name" "care_center_management_name" "agent_job_role_name" "agent_effective_hire_date" "agent_mso" "eduid" "last_handled_segment_flag" "record_update_timestamp" "source" "enhanced_account_number" "previous_call_time_utc" "call_end_date_utc")
bash sd_fd_check_field_discrepancies.sh $OLDCALLTABLE $NEWCALLTABLE
echo "When this finishes there will be a .txt file comparing the two call tables"
