CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_dvr_series(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  total_series_count int,
  active_series_count int,
  new_only_count int,
  new_and_repeat_count int,
  keep_one_count int,
  keep_three_count int,
  keep_five_count int,
  keep_seven_count int,
  keep_fifteen_count int,
  keep_twenty_five_count int,
  keep_all_count int,
  keep_all_new_only_count int,
  keep_all_new_and_repeat_count int,
  no_start_stop_edits_count int,
  start_edits_count int,
  stop_edits_count int,
  both_start_stop_edits_count int,
  series_sort_type int,
  series_sort_type_name string,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
