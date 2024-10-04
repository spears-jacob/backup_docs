CREATE TABLE IF NOT EXISTS ${db_name}.quality_m2_kpis(
  `metric_category` string, 
  `metric_name` string, 
  `metric_value` double, 
  `grouping_id` bigint, 
  `custom_grouping_id` string, 
  `decimal_custom_grouping_id` bigint, 
  `mobile_flag` string, 
  `application_name` string
  )
PARTITIONED BY ( 
  `denver_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;