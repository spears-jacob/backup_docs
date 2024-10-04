CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_vod_error(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  server_error_count int,
  server_format_error_count int,
  unknown_server_error_count int,
  communication_error_count int,
  no_resource_error_count int,
  session_timed_out_error_count int,
  sgd_failure_error_count int,
  ac_package_failure_response_error_count int,
  invalid_session_response_error_count int,
  bsd_session_config_response_error_count int,
  tuner_lost_on_vod_session_error_count int,
  two_way_failure_error_count int,
  tuning_failure_error_count int,
  asset_title_mismatch_error_count int,
  no_credit_error_count int,
  rsn_ud_btm_stringent_error_count int,
  biaxial_channel_no_available_error_count int,
  all_other_errors_count int,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
