CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_main_menu(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  launch_time string,
  duration int,
  total_sessions_count int,
  watch_tv_count int,
  guide_count int,
  on_demand_count int,
  kids_count int,
  search_count int,
  upgrade_now_count int,
  settings_count int,
  feature_tour_count int,
  dvr_count int,
  caller_id_count int,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
