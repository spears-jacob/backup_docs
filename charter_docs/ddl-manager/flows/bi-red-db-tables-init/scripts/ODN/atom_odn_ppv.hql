CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_ppv(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  adult_category_count int,
  special_events_category_count int,
  movies_category_count int,
  sports_category_count int,
  other_category_count int,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
