CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_apple_device_agg
(
  mso STRING,
  application_type STRING,
  app_version STRING,
  device_type STRING,
  connection_type STRING,
  network_status STRING,
  playback_type STRING,
  cust_type STRING,
  visit_id STRING,
  device_id STRING,
  acct_id STRING,
  startup_tagging STRING,
  apple_tv_manual_access STRING,
  apple_tv_auto_sso_auth STRING,
  referrer_app_visit_id STRING,
  login_user_token STRING,
  watch_time_ms BIGINT
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
    LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
