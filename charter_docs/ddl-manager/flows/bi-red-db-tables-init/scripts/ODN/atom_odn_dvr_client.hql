CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_dvr_client(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  prelude_recordings_count int,
  guide_recordings_count int,
  instant_recordings_count int,
  rdvr_recordings_count int,
  mrdvr_recordings_count int,
  ppv_recordings_count int,
  conflict_resolver_recordings_count int,
  total_count int,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
