CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.eft_device_catalog(
  `device_id` string
  )
PARTITIONED BY ( 
  `denver_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;