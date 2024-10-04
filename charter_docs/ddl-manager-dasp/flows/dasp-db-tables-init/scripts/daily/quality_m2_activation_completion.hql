CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quality_m2_activation_completion(
  `application_name` string,
  `device_id` string,
  `mobile_flag` string,
  `activation_start` int,
  `activation_success` int,
  `activation_fail` int
  )
PARTITIONED BY (
  `denver_date` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;