CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_buy_flow_page_agg
(
  application_type string,
  visit_id string,
  page_name string,
  page_sequence string,
  campaign_id string,
  mso string,
  acct_id string,
  billing_division string,
  billing_id string,
  billing_combo_key string,
  cust_type string,
  device_type string,
  device_id string,
  app_version string,
  technology_type string,
  referrer_location string,
  tactic_temp_id string,
  tactic_temp string,
  purchase_successes int,
  playback_start_cnts int,
  min_received_timestamp bigint,
  max_received_timestamp bigint,
  last_complete_step string,
  total_views int,
  error_views int,
  back_views int,
  confirm_views int,
  continue_views int,
  abandon_errors int,
  abandon_page int,
  page_viewed_time_ms bigint,
  buyflow_eligible_api_path string,
  buyflow_eligible_api_response_text string,
  buyflow_eligible_api_response_count int,
  agg_group string,
  promo_offer_id string,
  promo_id string,
  promo_type string,
  promo_start_date bigint,
  promo_start_timestamp_denver string,
  promo_end_date bigint,
  promo_end_timestamp_denver string,
  sign_up_clicks int,
  previous_page string,
  next_page string,
  purchase_success_timestamp bigint,
  distinct_impressions_visits int,
  distinct_banner_clicks_visits int,
  start_session int,
  login_success int,
  offer_api_call_count int,
  first_entitled_has_watchlive boolean,
  stream2_visit int,
  stream2signup_visit int,
  cancel_views int,
  purchase_views int,
  watch_views int,
  package_name string,
  promo_tactic string,
  applicationplatform_partition string,
  preference_selections_updated string,
  buyflow_metrics_map map<string,bigint>,
  api_response_grouping string,
  activated_experiments map<string,string>,
  network_status string,
  promo_type_tactic_entry string,
  promo_offer_id_tactic_entry string,
  message_completed_steps array<string>,
  variant_uuid ARRAY<STRING>,
  visit_campaign_id STRING,
  onsite_campaign_id STRING,
  referrer_campaign_id STRING,
  latest_watchlive_timestamp BIGINT
)
    PARTITIONED BY (denver_date STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    WITH SERDEPROPERTIES ('orc.column.index.access'='false')
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
