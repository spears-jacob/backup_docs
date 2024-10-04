-- first add in necessary columns to cs_call_data, cs_call_data_spark_etl, cs_call_data_preload
ALTER TABLE dev.cs_call_data ADD COLUMNS
(source STRING COMMENT 'Indicates the data source, whether extract or atom.',
enhanced_account_number BOOLEAN COMMENT 'Indicates whether the account number has been determined through enhancement.' );
ALTER TABLE dev.cs_call_data_spark_etl ADD COLUMNS
(source STRING COMMENT 'Indicates the data source, whether extract or atom.',
enhanced_account_number BOOLEAN COMMENT 'Indicates whether the account number has been determined through enhancement.' );
ALTER TABLE dev.cs_call_data_preload ADD COLUMNS
(source STRING COMMENT 'Indicates the data source, whether extract or atom.',
enhanced_account_number BOOLEAN COMMENT 'Indicates whether the account number has been determined through enhancement.' );

-- then create cs_call_data_extract just like cs_call_data
DROP TABLE IF EXISTS dev.cs_call_data_extract;
CREATE TABLE dev.cs_call_data_extract LIKE dev.cs_call_data;

-- copy the data from cs_call_data into cs_call_data_extract, adding in 'extract' as the source and false for enhancecd_account_number
INSERT INTO dev.cs_call_data_extract PARTITION (call_end_date_utc)
SELECT call_inbound_key
,call_id
,call_start_date_utc
,call_start_time_utc
,call_end_time_utc
,call_start_datetime_utc
,call_end_datetime_utc
,call_start_timestamp_utc
,call_end_timestamp_utc
,previous_call_time_utc
,segment_id
,segment_number
,segment_status_disposition
,segment_start_time_utc
,segment_end_time_utc
,segment_start_datetime_utc
,segment_end_datetime_utc
,segment_start_timestamp_utc
,segment_end_timestamp_utc
,segment_duration_seconds
,segment_duration_minutes
,segment_handled_flag
,customer_call_count_indicator
,call_handled_flag
,call_owner
,product
,account_number
,customer_account_number
,customer_type
,customer_subtype
,truck_roll_flag
,notes_txt
,resolution_description
,cause_description
,issue_description
,company_code
,service_call_tracker_id
,created_on
,created_by
,phone_number_from_tracker
,call_type
,split_sum_desc
,location_name
,care_center_management_name
,agent_job_role_name
,agent_effective_hire_date
,agent_mso
,eduid
,last_handled_segment_flag
,record_update_timestamp
,'extract' as source
,0 AS enhanced_account_number
,call_end_date_utc
 FROM dev.cs_call_data;

-- copy data with source and enhanced_account_number values back into call_end_date_utc
INSERT OVERWRITE TABLE dev.cs_call_data PARTITION(call_end_date_utc)
SELECT *
FROM dev.cs_call_data_extract;

INSERT INTO TABLE dev.cs_call_data PARTITION(call_end_date_utc)
SELECT call_inbound_key
,call_id
,call_start_date_utc
,call_start_time_utc
,call_end_time_utc
,call_start_datetime_utc
,call_end_datetime_utc
,call_start_timestamp_utc
,call_end_timestamp_utc
,previous_call_time_utc
,segment_id
,segment_number
,segment_status_disposition
,segment_start_time_utc
,segment_end_time_utc
,segment_start_datetime_utc
,segment_end_datetime_utc
,segment_start_timestamp_utc
,segment_end_timestamp_utc
,segment_duration_seconds
,segment_duration_minutes
,segment_handled_flag
,customer_call_count_indicator
,call_handled_flag
,call_owner
,product
,account_number
,customer_account_number
,customer_type
,customer_subtype
,truck_roll_flag
,notes_txt
,resolution_description
,cause_description
,issue_description
,company_code
,service_call_tracker_id
,created_on
,created_by
,phone_number_from_tracker
,call_type
,split_sum_desc
,location_name
,care_center_management_name
,agent_job_role_name
,agent_effective_hire_date
,agent_mso
,eduid
,last_handled_segment_flag
,record_update_timestamp
,'legacy' as source
,0 AS enhanced_account_number
,call_end_date_utc
FROM cs_call_data_2017;
