CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_digital_adoption_monitor_null
(
  source_table string,
  date_type string,
  application_name string,
  mso_set string,
  metric_name string,
  metric_total bigint,
  metric_count bigint,
  metric_count_enc_distinct bigint,
  metric_count_distinct bigint,
  metric_count_pct double,
  run_time string
)
PARTITIONED BY (date_value STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
