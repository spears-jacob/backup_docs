CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_viewership_monitor
(
  application_type STRING,
  playback_type STRING,
  app_version STRING,
  grouping_id INT,
  metric_name STRING,
  metric_value BIGINT)
PARTITIONED BY (utc_date STRING)
STORED AS ORC
    LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
