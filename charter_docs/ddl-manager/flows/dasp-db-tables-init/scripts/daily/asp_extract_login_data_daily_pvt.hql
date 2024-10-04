CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_extract_login_data_daily_pvt (
  account_number string,
  myspectrum string,
  specnet string,
  spectrumbusiness string,
  division_id string,
  site_sys string,
  prn string,
  agn string,
  specmobile string,
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
