

CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_deeplinking_agg
(
  mso string,
  billing_combo_key string,
  visit_id string,
  application_type string,
  link_playback_type string,
  link_report_group string,
  link_report_action string,
  link_guide_id string,
  link_provider_asset_id string,
  link_tms_program_id string,
  link_platform_series_id string,
  asset_starts int,
  asset_watch_time_ms bigint,
  other_starts int,
  other_watch_time_ms bigint,
  instance_link string,
  instance_location string,
  deeplinkstart_count int,
  deeplinkstop_true_count int,
  deeplinkstop_false_count int
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
