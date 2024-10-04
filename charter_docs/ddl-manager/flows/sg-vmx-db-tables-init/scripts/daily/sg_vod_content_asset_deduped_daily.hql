CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.sg_vod_content_asset_deduped_daily
(
  asset_class string,
  asset_id string,
  asset_name string,
  bitrate bigint,
  calsid bigint,
  content_asset_sid bigint,
  content_checksum string,
  content_file_name string,
  content_file_size bigint,
  content_format string,
  copy_protection boolean,
  create_load_sid bigint,
  creation_date bigint,
  description string,
  encryption boolean,
  hd_content boolean,
  image_aspect_ratio string,
  modify_load_sid bigint,
  product string,
  propagated_flag boolean,
  provider string,
  provider_id string,
  rec_create_time bigint,
  rec_modify_time bigint,
  run_time string,
  run_time_sec bigint,
  screen_format string,
  source_data_key string,
  status string,
  table_name string,
  title_asset_sid bigint,
  trick_modes_restricted string,
  type string,
  version string,
  version_major bigint,
  version_minor bigint,
  viewing_can_be_resumed boolean,
  action string,
  application string,
  batchsize string,
  modifyloadsid string,
  procid string,
  filename string,
  location_site string,
  display_name string,
  utc_offset string
)
    PARTITIONED BY (partition_date_denver STRING, legacy_company STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");

