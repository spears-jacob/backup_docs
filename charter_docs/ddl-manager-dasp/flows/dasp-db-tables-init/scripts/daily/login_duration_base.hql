CREATE EXTERNAL TABLE `login_duration_base`(
  `visit_id` string, 
  `application_name` string, 
  `login_duration` int, 
  `time_difference` int, 
  `message_name` string, 
  `event_case_id` string, 
  `message_sequence_number` int, 
  `message_timestamp` bigint, 
  `received_timestamp` bigint, 
  `current_page_name` string, 
  `current_page_sequence_number` int, 
  `operation_type` string, 
  `operation_int_value` int, 
  `operation_success` boolean, 
  `app_version` string, 
  `application_type` string, 
  `referrer_app_name` string)
PARTITIONED BY ( 
  `denver_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://pi-qtm-dasp-stg-aggregates-nopii/data/stg_dasp/login_duration_base'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661804541')
;
