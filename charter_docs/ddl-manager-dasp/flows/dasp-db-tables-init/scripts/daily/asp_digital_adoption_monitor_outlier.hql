create table ${db_name}.asp_digital_adoption_monitor_outlier
(
  source_table string,
  date_type string,
  date_value string,
  customer_type string,
  application_name string,
  metric_name string,
  metric_value string,
  metric_count_pct double,
  metric_limit double,
  run_time string
)
PARTITIONED BY (run_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
