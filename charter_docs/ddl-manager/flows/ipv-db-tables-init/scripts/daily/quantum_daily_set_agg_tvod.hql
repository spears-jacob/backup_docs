CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_set_agg_tvod
(
  mso STRING,
  application_type STRING,
  device_type STRING,
  connection_type STRING,
  network_status STRING,
  playback_type STRING,
  cust_type STRING,
  application_group_type STRING,
  app_version STRING,
  grouping_id INT,
  metric_name STRING,
  metric_value DOUBLE
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
    LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
