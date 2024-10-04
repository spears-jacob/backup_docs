CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_hourly_page_load_tenths_by_browser_quantum(
  pg_load_type string,
  browser_name string,
  page_name string,
  pg_load_sec_tenths double,
  unique_visits bigint,
  instances bigint)
PARTITIONED BY (
  date_denver string,
  date_hour_denver string,
  domain string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;