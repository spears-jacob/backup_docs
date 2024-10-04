CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_digital_adoption_daily_check
(
  partition_date_denver string,
  metric_name string,
  digital_value bigint,
  metric_value bigint,
  diff double,
  percent_diff double
)
PARTITIONED BY (run_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
