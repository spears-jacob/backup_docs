CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_initiate_si_and_pro_install_accounts(
  sales_type string,
  home_shipment_flag string,
  store_pickup_flag string,
  account_id string,
  mso string,
  si_channel string,
  user_package string,
  subscriber_type string,
  interaction_identifier string,
  initiated_self_install bigint,
  any_service_activated bigint,
  table_source string)
PARTITIONED BY ( si_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "SNAPPY")
