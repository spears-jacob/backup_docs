create table ${db_name}.asp_digital_adoption_monitor_range
(
  source_table string,
  date_type string,
  day_of_week int,
  application_name string,
  metric_name string,
  min_value double,
  max_value double,
  median double,
  mean double,
  vari double,
  std_dev double,
  mean_minus_2std double,
  mean_plus_2std double,
  run_time string
)
PARTITIONED BY (run_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
