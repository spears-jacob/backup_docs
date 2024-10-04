

use ${env:TMP_db};


--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_call_data_preload;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_call_data_preload
(
  --Call details
  call_inbound_key bigint COMMENT 'Unique identifier to each call a customer has placed'
  ,call_id bigint COMMENT 'Unique identifier to each call a customer has placed'
  ,call_start_date_utc string COMMENT 'The start date of the call'
  ,call_start_time_utc string COMMENT 'The call start time HH:MM:SS stored in UTC'
  ,call_end_time_utc string COMMENT 'The call end time HH:MM:SS stored in UTC'
  ,call_start_datetime_utc string COMMENT 'The start date time of the call stored in UTC'
  ,call_end_datetime_utc string COMMENT 'The end date time of the call stored in UTC'
  ,call_start_timestamp_utc bigint COMMENT 'The start timestamp of the call stored in UTC'
  ,call_end_timestamp_utc bigint COMMENT 'The end timestamp of the call stored in UTC'
  ,previous_call_time_utc bigint COMMENT 'The previous call time by account_number stored in UTC'

  --Segment details
  ,segment_id string COMMENT 'The unique identifier of each segment'
  ,segment_number string COMMENT 'The segment number of the call'
  ,segment_status_disposition string COMMENT 'Used in logic for segment_handled_flag'
  ,segment_start_time_utc string COMMENT 'The segment start time HH:MM:SS stored in UTC'
  ,segment_end_time_utc string COMMENT 'The segment end time HH:MM:SS stored in UTC'
  ,segment_start_datetime_utc string COMMENT 'The start date time of the call segment stored in UTC'
  ,segment_end_datetime_utc string COMMENT 'The end date time of the call segment stored in UTC'
  ,segment_start_timestamp_utc bigint COMMENT 'The start timestamp of the call segment stored in UTC'
  ,segment_end_timestamp_utc bigint COMMENT 'The end timestamp of the call segment stored in UTC'
  ,segment_duration_seconds decimal(12,4) COMMENT 'The segment duration stored in seconds'
  ,segment_duration_minutes decimal(12,4) COMMENT 'The segment duration stored in minutes'
  ,segment_handled_flag boolean COMMENT 'A flag set based on customer_call_count_indicator, call_handled_flag, call_owner, and segment_status_disposition. This is equivalent to BI handled segments in Microstrategy'
  ,customer_call_count_indicator string COMMENT 'Used in logic for segment_handled_flag'
  ,call_handled_flag string COMMENT 'Used in logic for segment_handled_flag'
  ,call_owner string COMMENT 'The organization responsible for handling the call. Used in logic for segment_handled_flag'

  --Call/Segment attributes
  ,product string COMMENT 'The customers IVR determined reason for calling'
  ,account_number string COMMENT 'aes256 encrypted version of account'
  ,customer_account_number string COMMENT 'aes256 encrypted version of the customer account number'
  ,customer_type string COMMENT 'The customer type at the time of the call'
  ,customer_subtype string COMMENT 'The customer sub type at the time of the call'
  ,truck_roll_flag boolean COMMENT '1/true= Truck Roll Initiated 0/false= No Truck Intiated'
  ,notes_txt string COMMENT 'Notes entered by the Customer Care Representative'
  ,resolution_description string COMMENT 'Description of resolution provided by customer service mapped to categories by unknown code'
  ,cause_description string COMMENT 'Description of cause provided by customer service mapped to categories by unknown code'
  ,issue_description string COMMENT 'Description of issue provided by customer service mapped to categories by unknown code'
  ,company_code string COMMENT 'The company code associated with the call'
  ,service_call_tracker_id string
  ,created_on string COMMENT 'The created timestamp of the record provided by the BI team'
  ,created_by string COMMENT 'The employee identifier who handled the call'
  ,phone_number_from_tracker string COMMENT 'The phone number passed to the call tracker'
  ,call_type string COMMENT 'The call subject'
  ,split_sum_desc string COMMENT 'A more specific reason for calling within a particular product group'
  ,location_name string COMMENT 'The call center where the call was sent'
  ,care_center_management_name string COMMENT 'Department of the answering agent'
  ,agent_job_role_name string COMMENT 'The job role of the agent who handled the call'
  ,agent_effective_hire_date string COMMENT 'The hire date of the agent who handled the call'
  ,agent_mso string COMMENT 'The MSO of the agent who handled the call'
  ,eduid string COMMENT 'Used to research calls with CII (Customer Insights and Intelligence)'
  ,last_handled_segment_flag string COMMENT 'Last handled segment across unique call (call_inbound_key)'
  ,record_update_timestamp string COMMENT 'The timestamp of the record provided by the BI team'
  ,source string COMMENT 'Indicates the data source, whether extract or atom.'
  ,enhanced_account_number BOOLEAN COMMENT 'Indicates whether the account number has been determined through enhancement.'
)
COMMENT 'This table contains data about customer call care. The data is stored at the segment level and calls can have multiple segments.'
PARTITIONED BY
(
call_end_date_utc string COMMENT 'The date the call took place'
)
;


--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_call_data;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_call_data
(
  --Call details
  call_inbound_key bigint COMMENT 'Unique identifier to each call a customer has placed'
  ,call_id bigint COMMENT 'Unique identifier to each call a customer has placed'
  ,call_start_date_utc string COMMENT 'The start date of the call'
  ,call_start_time_utc string COMMENT 'The call start time HH:MM:SS stored in UTC'
  ,call_end_time_utc string COMMENT 'The call end time HH:MM:SS stored in UTC'
  ,call_start_datetime_utc string COMMENT 'The start date time of the call stored in UTC'
  ,call_end_datetime_utc string COMMENT 'The end date time of the call stored in UTC'
  ,call_start_timestamp_utc bigint COMMENT 'The start timestamp of the call stored in UTC'
  ,call_end_timestamp_utc bigint COMMENT 'The end timestamp of the call stored in UTC'
  ,previous_call_time_utc bigint COMMENT 'The previous call time by account_number stored in UTC'

  --Segment details
  ,segment_id string COMMENT 'The unique identifier of each segment'
  ,segment_number string COMMENT 'The segment number of the call'
  ,segment_status_disposition string COMMENT 'Used in logic for segment_handled_flag'
  ,segment_start_time_utc string COMMENT 'The segment start time HH:MM:SS stored in UTC'
  ,segment_end_time_utc string COMMENT 'The segment end time HH:MM:SS stored in UTC'
  ,segment_start_datetime_utc string COMMENT 'The start date time of the call segment stored in UTC'
  ,segment_end_datetime_utc string COMMENT 'The end date time of the call segment stored in UTC'
  ,segment_start_timestamp_utc bigint COMMENT 'The start timestamp of the call segment stored in UTC'
  ,segment_end_timestamp_utc bigint COMMENT 'The end timestamp of the call segment stored in UTC'
  ,segment_duration_seconds decimal(12,4) COMMENT 'The segment duration stored in seconds'
  ,segment_duration_minutes decimal(12,4) COMMENT 'The segment duration stored in minutes'
  ,segment_handled_flag boolean COMMENT 'A flag set based on customer_call_count_indicator, call_handled_flag, call_owner, and segment_status_disposition. This is equivalent to BI handled segments in Microstrategy'
  ,customer_call_count_indicator string COMMENT 'Used in logic for segment_handled_flag'
  ,call_handled_flag string COMMENT 'Used in logic for segment_handled_flag'
  ,call_owner string COMMENT 'The organization responsible for handling the call. Used in logic for segment_handled_flag'

  --Call/Segment attributes
  ,product string COMMENT 'The customers IVR determined reason for calling'
  ,account_number string COMMENT 'aes256 encrypted version of account'
  ,customer_account_number string COMMENT 'aes256 encrypted version of the customer account number'
  ,customer_type string COMMENT 'The customer type at the time of the call'
  ,customer_subtype string COMMENT 'The customer sub type at the time of the call'
  ,truck_roll_flag boolean COMMENT '1/true= Truck Roll Initiated 0/false= No Truck Intiated'
  ,notes_txt string COMMENT 'Notes entered by the Customer Care Representative'
  ,resolution_description string COMMENT 'Description of resolution provided by customer service mapped to categories by unknown code'
  ,cause_description string COMMENT 'Description of cause provided by customer service mapped to categories by unknown code'
  ,issue_description string COMMENT 'Description of issue provided by customer service mapped to categories by unknown code'
  ,company_code string COMMENT 'The company code associated with the call'
  ,service_call_tracker_id string
  ,created_on string COMMENT 'The created timestamp of the record provided by the BI team'
  ,created_by string COMMENT 'The employee identifier who handled the call'
  ,phone_number_from_tracker string COMMENT 'The phone number passed to the call tracker'
  ,call_type string COMMENT 'The call subject'
  ,split_sum_desc string COMMENT 'A more specific reason for calling within a particular product group'
  ,location_name string COMMENT 'The call center where the call was sent'
  ,care_center_management_name string COMMENT 'Department of the answering agent'
  ,agent_job_role_name string COMMENT 'The job role of the agent who handled the call'
  ,agent_effective_hire_date string COMMENT 'The hire date of the agent who handled the call'
  ,agent_mso string COMMENT 'The MSO of the agent who handled the call'
  ,eduid string COMMENT 'Used to research calls with CII (Customer Insights and Intelligence)'
  ,last_handled_segment_flag string COMMENT 'Last handled segment across unique call (call_inbound_key)'
  ,record_update_timestamp string COMMENT 'The timestamp of the record provided by the BI team'
  ,source string COMMENT 'Indicates the data source, whether extract or atom.'
  ,enhanced_account_number BOOLEAN COMMENT 'Indicates whether the account number has been determined through enhancement.'
)
COMMENT 'This table contains data about customer call care. The data is stored at the segment level and calls can have multiple segments.'
PARTITIONED BY
(
call_end_date_utc string COMMENT 'The date the call took place'
)
;


--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_atom_call_care_staging;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_atom_call_care_staging
(
  account_id_encrypted string,
  account_number_encrypted string,
  answered_disposition_description string,
  answered_disposition_id string,
  call_cause_description string,
  call_handled_flag boolean,
  call_inbound_key string,
  call_issue_description string,
  call_notes_text string,
  call_owner_name string,
  call_resolution_description string,
  caller_id string,
  call_segmentation_number string,
  call_type_code string,
  care_center_management_name string,
  legacy_corp_code string,
  created_by string,
  created_on_date string,
  customer_account_number_encrypted string,
  customer_call_center_flag boolean,
  edu_id string,
  effective_hire_date string,
  job_role_name string,
  location_name string,
  last_handled_segmentation_flag boolean,
  last_updated_date_time_stamp string,
  mso_agent_name string,
  product_lob_description string,
  call_segment_stop_date_time string,
  call_segment_stop_date_time_in_est string,
  call_segment_start_date_time string,
  call_segment_start_date_time_in_est string,
  split_sum_description string,
  service_call_tracker_id string,
  track_phone_number_encrypted string,
  truck_roll_flag boolean,
  unified_call_id string,
  partition_date string
)
PARTITIONED BY (
  call_end_date_utc string
)
;
