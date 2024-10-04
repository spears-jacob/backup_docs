CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_app_entry_browsing_tableau
(
  metric_type        STRING,
  application_type   STRING,
  total              BIGINT,
  metric_name        STRING,
  metric_value       BIGINT
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
    LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
