CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_page_render_time_seconds_page_views_visits(
  domain string,
  page_name string,
  count_page_views bigint,
  count_visits int,
  hot_pg_load_sec bigint,
  cold_pg_load_sec bigint)
PARTITIONED BY (date_denver STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
; 
