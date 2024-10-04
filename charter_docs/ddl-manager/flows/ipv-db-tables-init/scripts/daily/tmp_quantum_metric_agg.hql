CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_metric_agg
(
    mso STRING,
    application_type STRING,
    device_type STRING,
    connection_type STRING,
    network_status STRING,
    playback_type STRING,
    cust_type STRING,
    stream_subtype String,
    android_type STRING,
    application_group_type STRING,
    stream_id STRING,
    visit_id STRING,
    device_id STRING,
    acct_id STRING,
    unique_stream_id STRING,
    unique_visit_id STRING,
    unique_device_id STRING,
    unique_account_number STRING,
    variant_uuids array<string>,
    episode_number STRING,
    season_number STRING,
    genres array<string>,
    linear_channel_name STRING,
    browser_name STRING,
    browser_user_agent STRING,
    browser_version STRING,
    tms_program_id STRING,
    min_received_timestamp BIGINT,
    max_received_timestamp BIGINT,
    manual_login_success BIGINT,
    manual_login_attempt BIGINT,
    resume_login_success BIGINT,
    resume_login_attempt BIGINT,
    verifier_login_success BIGINT,
    verifier_login_attempt BIGINT,
    manual_duration_ms_list ARRAY<INT>,
    verifier_duration_ms_list ARRAY<INT>,
    token_exchange_attempts BIGINT,
    stream_init_failures BIGINT,
    tune_time_ms_sum BIGINT,
    tune_time_count BIGINT,
    tune_time_ms_list ARRAY<INT>,
    watch_time_ms BIGINT,
    buffering_time_ms BIGINT,
    bitrate_downshifts BIGINT,
    stream_init_starts BIGINT,
    stream_noninit_failures BIGINT,
    bandwidth_consumed_mb DOUBLE,
    bitrate_content_elapsed_ms BIGINT,
    api_search_error_count INT,
    keyword_search INT,
    predictive_search INT,
    search_closed BIGINT,
    search_entered BIGINT,
    search_entered_space BIGINT,
    search_entered_total BIGINT,
    search_response_time_ms_list ARRAY<INT>,
    search_result_selected INT,
    search_result_type_list ARRAY<STRING>,
    search_started BIGINT,
    search_text_length BIGINT,
    zero_results BIGINT,
    zero_results_space BIGINT,
    billing_division STRING,
    billing_id STRING,
    location_city STRING,
    location_state STRING,
    location_zipcode STRING,
    app_version STRING,
    complete_buffering_time_ms_list ARRAY<INT>,
    complete_manual_duration_ms_list ARRAY<INT>,
    complete_tune_time_ms_list ARRAY<INT>,
    complete_verifier_duration_ms_list ARRAY<INT>,
    complete_watch_time_ms ARRAY<INT>,
    nonactive_account_number STRING,
    nonactive_device_id STRING,
    nonactive_stream_id STRING,
    nonactive_visit_id STRING,
    device_model STRING,
    critical_api_attempts INT,
    critical_api_fails INT,
    application_errors INT,
    critical_api_success INT,
    other_api_response_count INT,
    stream_choice STRING,
    content_class STRING,
    billing_division_id STRING,
    billing_combo_key STRING,
    app_vertical STRING,
    application_name STRING,
    logged_in STRING,
    operating_system STRING,
    form_factor STRING,
    portals_forgot_password_success int,
    portals_forgot_username_success int,
    portals_call_support_or_request_callback int,
    portals_view_support_section int,
    portals_view_online_statments int,
    portals_view_one_time_payment int,
    portals_set_up_autopay int,
    portals_refresh_digital_receiver_requests int,
    portals_modem_router_resets int,
    portals_rescheduled_service_appointments int,
    portals_cancelled_service_appointments int,
    portals_new_id_creation_attempts int,
    portals_new_sub_user_creation_attempts int,
    technology_type STRING,
    campaign_id STRING,
    activated_experiments map<string,string>,
    provider_asset_id STRING,
    content_title STRING,
    content_episode_title STRING,
    linear_network_name STRING,
    tms_guide_id            STRING,
    video_zone_division     STRING,
    video_zone_lineup       STRING,
    force_tune_boolean      BOOLEAN,
    tune_type               STRING,
    stream_init_triggered_by_user   INT,
    stream_init_triggered_by_app    INT
)
    PARTITIONED BY (denver_date STRING, enrich_status STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress"="SNAPPY");


