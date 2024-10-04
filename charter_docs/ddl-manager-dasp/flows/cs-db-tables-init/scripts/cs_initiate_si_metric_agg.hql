CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_initiate_si_metric_agg(
  account_id string,
  mso string,
  sales_type string,
  home_shipment_flag string,
  store_pickup_flag string,
  si_channel string,
  user_package string,
  subscriber_type string,
  metric_date string,
  days_since_si_initiate bigint,
  initiated_self_install string,
  any_service_activated string,
  portals_manage_auto_payment_starts bigint,
  portals_manage_auto_payment_successes bigint,
  portals_set_up_auto_payment_starts bigint,
  portals_set_up_auto_payment_submits bigint,
  portals_set_up_auto_payment_successes bigint,
  portals_one_time_payment_starts bigint,
  portals_one_time_payment_submits bigint,
  portals_one_time_payment_successes bigint,
  portals_view_online_statments bigint,
  portals_equipment_confirm_edit_ssid_select_action bigint,
  portals_equipment_edit_ssid_select_action bigint,
  portals_scp_click_pause_device bigint,
  portals_scp_click_unpause_device bigint,
  portals_scp_click_cancel_pause_device bigint,
  portals_support_page_views bigint,
  portals_all_equipment_reset_flow_starts bigint,
  portals_all_equipment_reset_flow_successes bigint,
  visit_count bigint,
  days_w_call bigint,
  table_source string)
PARTITIONED BY (
  si_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "SNAPPY")
