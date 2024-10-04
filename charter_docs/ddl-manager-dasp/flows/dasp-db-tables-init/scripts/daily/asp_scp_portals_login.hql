CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_scp_portals_login (
  legacy_company string,
  account_number string,
  event_type string,
  instances int,
  scp_flag boolean,
  wifi_flag boolean,
  internet_flag boolean,
  future_connect_flag boolean,
  customer_type string,
  account_type string,
  account_key string)
PARTITIONED BY (partition_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
