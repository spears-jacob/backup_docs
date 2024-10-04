CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_instant_upgrade(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  unauthorized_channel_launch_count int,
  upgrade_channel_launch_count int,
  kids_portal_launch_count int,
  network_portal_launch_count int,
  vod_portal_launch_count int,
  showcase_launch_count int,
  search_launch_count int,
  dvr_app_launch_count int,
  main_menu_launch_count int,
  guide_launch_count int,
  offer_error_count int,
  purchase_error_count int,
  platform int,
  model int,
  guide_type int,
  guide_type_name string,
  guide_version string,
  customer_language int,
  customer_language_name string,
  aspect_ratio int,
  aspect_ratio_name string,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
