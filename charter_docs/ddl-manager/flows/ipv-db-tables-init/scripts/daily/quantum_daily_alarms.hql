CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_alarms
(
  platform STRING,
  alarm_cadence STRING,
  playback_type STRING,
  application_type STRING,
  metric_name STRING,
  metric_value DOUBLE,
  comparison_metric_value DOUBLE,
  alarm_trigger_value DECIMAL,
  metric_source STRING,
  raw_diff DOUBLE,
  abs_raw_diff DOUBLE,
  pct_diff DOUBLE,
  abs_pct_diff DOUBLE,
  alarm TINYINT,
  spike_alarm TINYINT,
  dip_alarm TINYINT,
  alarm_type STRING,
  grouping_id INT,
  comparison_denver_date STRING
)
  PARTITIONED BY (denver_date STRING)
  STORED AS ORC
  LOCATION '${s3_location}'
  TBLPROPERTIES ("orc.compress" = "SNAPPY");
