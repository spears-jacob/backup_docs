CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_msa_onboarding_metrics (
  mso STRING,
  device_type STRING,
  tutorial_completed STRING,
  aggregation STRING,
  metric_name STRING,
  metric_value DOUBLE,
  process_date_time_denver STRING,
  process_identity STRING
)

PARTITIONED BY (label_date_denver string, grain STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
