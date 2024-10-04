CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_billing_set_agg
(
  application_name STRING,
  mso STRING,
  autopay_status STRING,
  billing_status STRING,
  metric_name STRING,
  metric_value DOUBLE,
  grouping_id INT,
  process_date_time_denver STRING,
  process_identity STRING)
PARTITIONED BY (label_date_denver STRING, grain STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;