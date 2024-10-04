CREATE EXTERNAL TABLE if not exists ${db_name}.quantum_mos_tableau
(
  stream_level string,
  mso string,
  application_type string,
  device_type string,
  connection_type string,
  network_status string,
  playback_type string,
  cust_type string,
  application_group_type string,
  app_version string,
  streaming_format string,
  drm_type string,
  peak_flag string,
  failure_retries string,
  dai_insertion string,
  content_class string,
  grouping_id int,
  stream_id_count int,
  stream_init_failures int,
  stream_noninit_failures int,
  buffering_ratio double,
  pibbe_ms double,
  level2_downshifts int,
  pibd2_ms int,
  avg_bitrate_mbps double,
  tune_time_ms int,
  metric_name string,
  metric_value double,
  technology_type string,
  experiment_uuid string,
  variant_uuid string,
  playback_capping  string,
  stream_host string)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
    LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
