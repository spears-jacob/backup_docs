CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_scp_portals_acct_agg (
  acct_number_enc string,
  sys_enc string,
  prin_enc string,
  agent_enc string,
  msa_scp_flag string,
  action_string string,
  action_cnt int,
  app_api_response_code string,
  error_msg string,
  wifi_customer_type string)
PARTITIONED BY (data_utc_dt string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
