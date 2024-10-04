CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_idm_paths_metrics (
  platform string,
  referrer_link string,
  browser_name string,
  device_type string,
  pagename string,
  app_section string,
  std_name string,
  api_code string,
  api_text string,
  metric_name string,
  metric_value bigint
)
PARTITIONED BY (date_denver string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
