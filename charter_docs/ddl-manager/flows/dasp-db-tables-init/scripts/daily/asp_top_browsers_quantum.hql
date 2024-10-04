CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_top_browsers_quantum(
  pg_load_type string,
  rank_browser int,
  browser_name string,
  unique_visits bigint)
PARTITIONED BY (
  date_denver string,
  domain string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
