CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_digital_adoption_monitor_da
(
  source_table string,
  date_type string,
  customer_type string,
  metric_name string,
  metric_value BIGINT,
  run_time string
)
PARTITIONED BY (date_value STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
