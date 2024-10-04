CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_usage_ccpa_daily_table_c
(
  account STRING,
  billing_division_id STRING,
  billing_division STRING,
  company STRING,
  app_name STRING,
  app_type STRING,
  device_remove_count BIGINT,
  device_add_count BIGINT,
  network_name_change BIGINT
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
