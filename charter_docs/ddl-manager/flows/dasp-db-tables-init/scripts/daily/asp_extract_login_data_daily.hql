CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_extract_login_data_daily (
  account_number string,
  application_name string,
  message__name string,
  operation__success string,
  division_id string,
  site_sys string,
  prn string,
  agn string,
  biller_type string,
  division string,
  acct_site_id string,
  acct_company string,
  acct_franchise string
)
PARTITIONED BY (date_denver string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
