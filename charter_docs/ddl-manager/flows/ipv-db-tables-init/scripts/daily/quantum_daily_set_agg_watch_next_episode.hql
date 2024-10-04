CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_set_agg_watch_next_episode
(
  mso string,
  application_type string,
  device_type string,
  connection_type string,
  network_status string,
  playback_type string,
  cust_type string,
  application_group_type string,
  app_version string,
  stream_subtype string,
  grouping_id int,
  metric_name string,
  metric_value double
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
    LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
