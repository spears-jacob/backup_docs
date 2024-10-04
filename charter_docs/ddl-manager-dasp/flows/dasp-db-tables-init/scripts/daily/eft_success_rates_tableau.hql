CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.eft_success_rates_tableau(
  `flow_group` string, 
  `application_name` string, 
  `metric_name` string, 
  `metric_value` double)
PARTITIONED BY ( 
  `denver_date` string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
