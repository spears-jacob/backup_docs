CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_daily_report_data(
  reportday string,
  metric string,
  value string,
  vsavgprior8wkssamedayofwk decimal(15,5),
  review_comment string,
  additional_comment string,
  domain string
) PARTITIONED BY (
date_denver string
)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY"); 
