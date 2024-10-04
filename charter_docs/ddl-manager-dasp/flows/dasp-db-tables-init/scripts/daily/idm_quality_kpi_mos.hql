CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.idm_quality_kpi_mos (
  timeframe string,
  application_name string,
  application_type string,
  metric_name string,
  score double,
  metric_value double,
  duration_bucket_filtered_ratio double,
  page_load_time_bucket_filtered_ratio double,
  duration_bucket double,
  page_load_time_bucket double,
  cya_both_derived double,
  cya_both double,
  cya_success double,
  cya_failure_not double,
  vyi_both_derived double,
  vyi_both double,
  vyi_success double,
  vyi_failure_not double,
  cun_both_derived double,
  cun_both double,
  cun_success double,
  cun_failure_not double,
  rpw_both_derived double,
  rpw_both double,
  rpw_success double,
  rpw_failure_not double,
  flow string
)
PARTITIONED BY (denver_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
