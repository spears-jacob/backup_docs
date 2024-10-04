-- call_count_24h is a string, because any non-visit unit needs to be able to handle the string indicating it's invalid
-- this means it will have to be cast as int to make CIR for visit-level analysis
-- should save to prod_dasp
CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_set_agg_portals_to_push_to_prod
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
  metric_value DOUBLE,
  logged_in_status STRING,
  application_name STRING,
  os_name STRING,
  os_version STRING,
  browser_name STRING,
  browser_version STRING,
  form_factor STRING,
  process_date_time_denver STRING,
  process_identity STRING,
  unit_type STRING,
  call_count_24h STRING
  )
PARTITIONED BY (label_date_denver STRING, grain STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
