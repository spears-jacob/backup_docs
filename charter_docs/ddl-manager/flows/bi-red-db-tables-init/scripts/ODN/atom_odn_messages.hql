CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_messages(
  version int,
  protocol_header int,
  address_type string,
  encrypted_stb_address_256 string,
  flags int,
  message_transaction_id int,
  message_domain string,
  message_length int,
  encrypted_message_256 string,
  filename string,
  data_date string,
  controller string,
  analytics_group_name string,
  message_type string,
  invalid_message boolean,
  failure_reason string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
