CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_daily_error_code (
  domain string,
  url string,
  error_type string,
  error_code string,
  error_msg string,
  client_error_code string,
  date_denver_week string,
  date_denver_month string,
  error_count int
) PARTITIONED BY (
  date_denver string
)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
