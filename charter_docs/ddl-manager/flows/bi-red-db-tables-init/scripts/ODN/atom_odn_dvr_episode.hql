CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_dvr_episode(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  total_recordings_count int,
  non_episodic_recordings_count int,
  episodic_recordings_count int,
  unenrichable_episodes_count int,
  enriched_episodes_count int,
  not_enriched_count int,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
