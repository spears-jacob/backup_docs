CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_support_article_call_prep (
  visit__visit_id STRING,
  visit__application_details__application_name STRING,
  call_inbound_key STRING,
  call_type STRING,
  product STRING,
  issue_group STRING,
  cause_group STRING,
  resolution_group STRING,
  state__view__current_page__page_title STRING

)
PARTITIONED BY (partition_date_utc STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
