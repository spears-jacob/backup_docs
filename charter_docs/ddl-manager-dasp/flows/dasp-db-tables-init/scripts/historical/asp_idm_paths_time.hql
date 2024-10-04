CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_idm_paths_time (
  platform string,
  referrer_link string,
  browser_name string,
  device_type string,
  pagename string,
  page_viewed_time_sec double
)
PARTITIONED BY (date_denver string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");

