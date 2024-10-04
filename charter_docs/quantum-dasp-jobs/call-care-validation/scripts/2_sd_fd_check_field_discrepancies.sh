#!/bin/bash

##Assumption: for any given segment, each field for that segment should have
### the same data in the old call_care_data table and the new one.

## Same Data (sd)
###### Field Discrepancies (sd_fd)

##This generates a file with the names of the two tables you've comparing,
### and lists each field in call_care_data
### for each field, it lists how many rows have different data in that field
## It may be useful to compare prod data to itself, to find what baseline of
### errors we're currently accepting / what is caused by duplicates.

##TODO: make this insert into a table instead of a text file, so that it can
## use FROM ... INSERT INTO syntax

if [ $# -ne 3 ]; then
    echo $0: Usage: sd_check_field_discrepancies.sh oldtable newtable date
    exit 1
fi

declare -a field_array
#this array will have to be changed for the fields you want to check
#full array
field_array=("call_inbound_key" "call_id" "call_start_date_utc" "call_start_time_utc" "call_end_time_utc" "call_start_datetime_utc" "call_end_datetime_utc" "call_start_timestamp_utc" "call_end_timestamp_utc" "segment_id" "segment_number" "segment_status_disposition" "segment_start_time_utc" "segment_end_time_utc" "segment_start_datetime_utc" "segment_end_datetime_utc" "segment_start_timestamp_utc" "segment_end_timestamp_utc" "segment_duration_seconds" "segment_duration_minutes" "segment_handled_flag" "customer_call_count_indicator" "call_handled_flag" "call_owner" "product" "account_number" "customer_account_number" "customer_type" "customer_subtype" "truck_roll_flag" "notes_txt" "resolution_description" "cause_description" "issue_description" "company_code" "service_call_tracker_id" "created_on" "created_by" "phone_number_from_tracker" "call_type" "split_sum_desc" "location_name" "care_center_management_name" "agent_job_role_name" "agent_effective_hire_date" "agent_mso" "eduid" "last_handled_segment_flag" "record_update_timestamp" "source" "enhanced_account_number" "previous_call_time_utc" "call_end_date_utc")

#set these variables to whatever tables you're comparing
OLDTABLE=$1
NEWTABLE=$2
TESTDATE=$3
JOINFIELD=segment_id
OUTPUTFILENAME=${OLDTABLE:10}_vs_${NEWTABLE:10}.txt

echo Old Table: $OLDTABLE
echo New Table: $NEWTABLE
echo Date: $TESTDATE
echo Join Field: $JOINFIELD



for i in `echo ${field_array[@]}`
do
  hive -f ./hql/2_call_care_data_check_script-${SCRIPT_VERSION}.hql --hiveconf old_table=$OLDTABLE --hiveconf new_table=$NEWTABLE --hiveconf field_name=$i --hiveconf test_date=$TESTDATE

done
