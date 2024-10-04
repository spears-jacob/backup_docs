CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_monthly_referring_traffic
(
  pageview_count          bigint,
  platform_type           string,
  app_name                string,
  page_title              string,
  rollup_                 int
)
    PARTITIONED BY (label_date_denver string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
