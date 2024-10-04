CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_extract_asapp_visitid_data_daily (
  external_session_id string,
  application_name string,
  acct_enc string,
  acct string,
  account_number string,
  biller_type string,
  division string,
  division_id string,
  sys string,
  prn string,
  agn string,
  acct_site_id string,
  acct_company string,
  acct_franchise string,
  flag_enriched boolean,
  mobile_acct_enc string
)
PARTITIONED BY (date_denver string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
