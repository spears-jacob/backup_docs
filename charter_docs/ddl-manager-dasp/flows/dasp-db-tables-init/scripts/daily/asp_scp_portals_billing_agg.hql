CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_scp_portals_billing_agg (
  scp_flag boolean,
  wifi_flag boolean,
  internet_flag boolean,
  future_connect_flag boolean,
  account_type string,
  legacy_company string,
  customer_type string,
  total_hhs int,
  grouping_id int,
  grouping_set string,
  wifi_customer_type string,
  grain string)
PARTITIONED BY (partition_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
