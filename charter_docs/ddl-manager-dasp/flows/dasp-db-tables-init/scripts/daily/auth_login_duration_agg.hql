CREATE EXTERNAL TABLE `auth_login_duration_agg`(
  `application_name` string, 
  `avg_diff_message_timestamp` double, 
  `avg_login_duration` double)
PARTITIONED BY ( 
  `denver_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://pi-qtm-dasp-stg-aggregates-nopii/data/stg_dasp/auth_login_duration_agg'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661962349')