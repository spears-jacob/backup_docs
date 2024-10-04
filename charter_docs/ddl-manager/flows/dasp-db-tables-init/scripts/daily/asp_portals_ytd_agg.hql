CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_portals_ytd_agg
(
  application_name   STRING,
  application_type   STRING,
  unit_type          STRING,
  metric_name        STRING,
  metric_value       DOUBLE)
PARTITIONED BY (label_date_denver STRING, grain STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
