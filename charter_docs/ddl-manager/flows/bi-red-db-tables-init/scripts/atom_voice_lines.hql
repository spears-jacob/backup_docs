CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_voice_lines (
  account_key STRING,
  encrypted_legacy_account_number_256 STRING,
  encrypted_phone_number STRING,
  status_code STRING,
  status_description STRING,
  encrypted_account_key_256 STRING,
  encrypted_normalized_phone_number_256 STRING,
  padded_encrypted_account_number_256 STRING,
  encrypted_padded_account_number_256 STRING)
PARTITIONED BY (
  partition_date_denver STRING,
  extract_source STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");