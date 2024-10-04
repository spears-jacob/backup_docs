CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_api_responses_raw (
  application_name string,
  app_version string,
  mso string,
  api_category string,
  api_name string,
  api_code string,
  page_name string,
  response_ms int,
  response_s_tenths double,
  service_result string
)
PARTITIONED BY (date_denver string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
