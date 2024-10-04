CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_usage_ccpa_daily_table_b
(
  account STRING,
  billing_division_id STRING,
  billing_division STRING,
  company STRING,
  app_name STRING,
  app_type STRING,
  event_type STRING,
  event_name STRING,
  event_count BIGINT
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");

