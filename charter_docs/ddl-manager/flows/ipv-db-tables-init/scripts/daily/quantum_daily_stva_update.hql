CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_stva_update
(
  timeframe string,
  metric_name string,
  metric_value float
)
    PARTITIONED BY (denver_date STRING, metric_category STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
