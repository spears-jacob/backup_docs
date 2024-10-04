CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_daily_report_data_summary(
  reportday string,
  domain string,
  metric_type string,
  metric_count_threshold int,
  metric_count INT,
  status boolean
) PARTITIONED BY (
date_denver string
)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY"); 
