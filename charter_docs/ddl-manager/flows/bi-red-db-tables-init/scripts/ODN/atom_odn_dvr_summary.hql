CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_dvr_summary(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  disk_space_free bigint,
  disk_free_percentage int,
  active_files_count int,
  actual_disk_size bigint,
  total_minutes_recorded_count bigint,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
