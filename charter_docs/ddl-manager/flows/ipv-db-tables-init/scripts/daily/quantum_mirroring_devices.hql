CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_mirroring_devices
(
  application_type STRING,
  playback_type STRING,
  referrer_application_type STRING,
  visit_per_hh STRING,
  grouping_id INT,
  devices_cnt INT,
  hh_cnt INT,
  visit_cnt INT,
  watch_time_ms BIGINT,
  yesterday_unique_hh INT,
  returning_accounts INT
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
