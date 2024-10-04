reface table names with cs (customer service)

--DROP TABLE IF EXISTS ${env:TMP_db}.cs_call_data_2017_raw;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.cs_call_data_2017_raw
(
  CallStartDt_NewYork string
  ,CallStopDt_NewYork string
  ,CallStartTime_NewYork string
  ,CallStopTime_NewYork string
  ,Product string
  ,AcctId string
  ,AcctNum string
  ,CustAccountNo string
  ,TruckRollFl string
  ,NotesTxt string
  ,CallResDesc string
  ,CallCauseDesc string
  ,CallIssDesc string
  ,SegNum string
  ,CallId string
  ,CompanyCode string
  ,ServiceCallTrackerID string
  ,CreatedOn string
  ,CreatedBy string
  ,PhoneNumberFromTracker string
  ,CallType string
  ,SplitSumDesc string
  ,LocNm string
  ,CareCtrMgmtNm string
  ,JobRoleNm string
  ,EffectiveHireDt string
  ,EDUId string
  ,MSOAGENT string
  ,AnsDispoDesc string
  ,CustCallCntctInd string
  ,CallOwner string
  ,CallInbKey string
  ,UniCallId string
  ,CallsHndlFl string
  ,LastHndlSegFl string
  ,UPDATETIMESTAMP string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  'skip.header.line.count'='1',
  'serialization.null.format'='')
  ;


--DROP TABLE IF EXISTS ${env:TMP_db}.cs_call_data_2017_managed;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.cs_call_data_2017_managed
(
  CallStartDate_NewYork string
  ,CallStopDate_NewYork string
  ,CallStartDatetime_NewYork string
  ,CallEndDatetime_NewYork string
  ,CallStartTime_NewYork string
  ,CallEndTime_NewYork string
  ,Product string
  ,AcctId string
  ,AcctNum string
  ,CustAccountNo string
  ,TruckRollFl string
  ,NotesTxt string
  ,CallResDesc string
  ,CallCauseDesc string
  ,CallIssDesc string
  ,SegNum string
  ,CallId string
  ,CompanyCode string
  ,ServiceCallTrackerID string
  ,CreatedOn string
  ,CreatedBy string
  ,PhoneNumberFromTracker string
  ,CallType string
  ,SplitSumDesc string
  ,LocNm string
  ,CareCtrMgmtNm string
  ,JobRoleNm string
  ,EffectiveHireDt string
  ,EDUId string
  ,MSOAGENT string
  ,AnsDispoDesc string
  ,CustCallCntctInd string
  ,CallOwner string
  ,CallInbKey string
  ,UniCallId string
  ,CallsHndlFl string
  ,LastHndlSegFl string
  ,UPDATETIMESTAMP string
)
;


--DROP TABLE IF EXISTS ${env:TMP_db}.cs_call_data_p270_raw;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.cs_call_data_p270_raw
(
  CallStartDt_NewYork string
  ,CallStopDt_NewYork string
  ,CallStartTime_NewYork string
  ,CallStopTime_NewYork string
  ,Product string
  ,AcctNum string
  ,CustAccountNo string
  ,TruckRollFl string
  ,NotesTxt string
  ,CallResDesc string
  ,CallCauseDesc string
  ,CallIssDesc string
  ,SegNum string
  ,CallId string
  ,CompanyCode string
  ,ServiceCallTrackerID string
  ,CreatedOn string
  ,CreatedBy string
  ,PhoneNumberFromTracker string
  ,CallType string
  ,SplitSumDesc string
  ,LocNm string
  ,CareCtrMgmtNm string
  ,JobRoleNm string
  ,EffectiveHireDt string
  ,EDUId string
  ,MSOAGENT string
  ,AnsDispoDesc string
  ,CustCallCntctInd string
  ,CallOwner string
  ,CallInbKey string
  ,UniCallId string
  ,CallsHndlFl string
  ,LastHndlSegFl string
  ,UPDATETIMESTAMP string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  'skip.header.line.count'='1',
  'serialization.null.format'='')
;


--DROP TABLE IF EXISTS ${env:TMP_db}.cs_call_data_p270_managed;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.cs_call_data_p270_managed
(
  CallStartDate_NewYork string
  ,CallStopDate_NewYork string
  ,CallStartDatetime_NewYork string
  ,CallEndDatetime_NewYork string
  ,CallStartTime_NewYork string
  ,CallEndTime_NewYork string
  ,Product string
  ,AcctId string
  ,AcctNum string
  ,CustAccountNo string
  ,TruckRollFl string
  ,NotesTxt string
  ,CallResDesc string
  ,CallCauseDesc string
  ,CallIssDesc string
  ,SegNum string
  ,CallId string
  ,CompanyCode string
  ,ServiceCallTrackerID string
  ,CreatedOn string
  ,CreatedBy string
  ,PhoneNumberFromTracker string
  ,CallType string
  ,SplitSumDesc string
  ,LocNm string
  ,CareCtrMgmtNm string
  ,JobRoleNm string
  ,EffectiveHireDt string
  ,EDUId string
  ,MSOAGENT string
  ,AnsDispoDesc string
  ,CustCallCntctInd string
  ,CallOwner string
  ,CallInbKey string
  ,UniCallId string
  ,CallsHndlFl string
  ,LastHndlSegFl string
  ,UPDATETIMESTAMP string
)
PARTITIONED BY
(
    call_end_date_newyork string
)
;



--p


--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_call_data_2017_preload;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_call_data_2017_preload
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
  ,account_id string COMMENT 'the aes256 encrypter version of the customers account ID'
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
)
COMMENT 'This table contains data about customer call care. The data is stored at the segment level and calls can have multiple segments.'
PARTITIONED BY
(
call_end_date_utc string COMMENT 'The date the call took place'
)
;


--DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_call_data_2017;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_call_data_2017
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
  ,account_id string COMMENT 'the aes256 encrypter version of the customers account ID'
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
)
COMMENT 'This table contains data about customer call care. The data is stored at the segment level and calls can have multiple segments.'
PARTITIONED BY
(
call_end_date_utc string COMMENT 'The date the call took place'
)
;
