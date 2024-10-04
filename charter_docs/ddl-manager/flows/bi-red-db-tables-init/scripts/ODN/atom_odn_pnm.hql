CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_pnm(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  time_activated string,
  cached_count int,
  server_errors_count int,
  timeouts_count int,
  transactions_count int,
  percent_cached int,
  mean int,
  standard_deviation int,
  duration_min int,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
