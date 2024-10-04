CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_agg_page_load_time_grouping_sets_quantum
 (
   pg_load_type string,
   grouping_id int,
   grouped_partition_date_denver string,
   grouped_partition_date_hour_denver string,
   grouped_browser_name string,
   grouped_breakpoints string,
   partition_date_hour_denver string,
   browser_name string,
   browser_size_breakpoint string,
   page_name string,
   section string,
   domain string,
   unique_visits int,
   page_views bigint,
   avg_pg_ld double,
   total_pg_ld double,
   05_percentile double,
   10_percentile double,
   15_percentile double,
   20_percentile double,
   25_percentile double,
   30_percentile double,
   35_percentile double,
   40_percentile double,
   45_percentile double,
   50_percentile double,
   55_percentile double,
   60_percentile double,
   65_percentile double,
   70_percentile double,
   75_percentile double,
   80_percentile double,
   85_percentile double,
   90_percentile double,
   95_percentile double,
   99_percentile double)
 PARTITIONED BY (
   partition_date_denver string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
; 
