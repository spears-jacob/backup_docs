CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_api_agg (
  grouping_id BIGINT,
  hour_denver STRING,
  minute_group STRING,
  application_name STRING,
  mso STRING,
  cust_type STRING,
  api_category STRING,
  api_name STRING,
  stream_subtype String,
  current_page_name STRING,
  metric_name STRING,
  metric_value DOUBLE,
  week_end STRING,
  month_start STRING,
  technology_type STRING)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;