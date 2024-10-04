CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_misc(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  my_subscription_status int,
  start_over_session_count int,
  start_over_total_duration bigint,
  offering_id int,
  time_into_show int,
  session_duration_s int,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
