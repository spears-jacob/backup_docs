CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_dvr_error(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  series_conflict_count int,
  recording_conflict_count int,
  tuner_conflict_count int,
  in_progress_recording_conflict_count int,
  recording_contention_count int,
  can_not_delete_message_count int,
  can_not_cancel_message_count int,
  unable_to_record_count int,
  disk_full_warning_count int,
  can_not_launch_showlist_count int,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
