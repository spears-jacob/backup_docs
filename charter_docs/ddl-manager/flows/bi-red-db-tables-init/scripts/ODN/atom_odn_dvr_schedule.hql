CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_odn_dvr_schedule(
  version int,
  address_type string,
  encrypted_stb_address_256 string,
  message_domain string,
  message_transaction_id int,
  data_date string,
  tag_id_count int,
  reconcile_pending_recording_events_count int,
  reconciles_duration_ms bigint,
  recordings_reconciled_count int,
  schedule_series_events_count int,
  schedules_duration_ms bigint,
  recordings_scheduled_count int,
  update_series_events_count int,
  updates_duration_ms bigint,
  recordings_updated_count int,
  cancel_series_events_count int,
  cancels_duration_ms bigint,
  stale_episode_delete_events_count int,
  deletes_duration_ms bigint,
  recordings_deleted_count int,
  enrichment_requests_sent_count int,
  dvr_sort int,
  dvr_sort_name string,
  site_code string)
PARTITIONED BY (
  partition_date string,
  partition_hour string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "ZLIB");
