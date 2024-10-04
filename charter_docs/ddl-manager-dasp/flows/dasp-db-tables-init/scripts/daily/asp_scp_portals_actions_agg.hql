CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_scp_portals_actions_agg (
  action_string string,
  msa_scp_flag string,
  wifi_customer_type string,
  grain string,
  total_actions bigint,
  distinct_accounts bigint)
PARTITIONED BY (data_utc_dt string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
