CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_scp_portals_product (
  scp_flag boolean,
  login_hhs int,
  pause_events int,
  unpause_events int,
  ssid_change_events int,
  remove_device_events int,
  edit_device_nickname_events int,
  connected_device_events int,
  session_rate double,
  equipment_page_view_hhs_per_hhs_rate double,
  pause_event_hhs_per_total_hhs_rate double,
  grain string)
PARTITIONED BY (partition_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
