CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_dvr_tuner(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  tuners_count int,
  tuner_usage int,
  tuner_constraint int,
  minutes_not_recorded_count int,
  tuner_1_minutes_recorded_count int,
  tuner_2_minutes_recorded_count int,
  tuner_3_minutes_recorded_count int,
  tuner_4_minutes_recorded_count int,
  tuner_5_minutes_recorded_count int,
  tuner_6_minutes_recorded_count int,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
