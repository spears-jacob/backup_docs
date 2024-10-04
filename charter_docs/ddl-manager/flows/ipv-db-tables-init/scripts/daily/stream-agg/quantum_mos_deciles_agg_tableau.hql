CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_mos_deciles_agg_tableau
(
    mso string,
    application_type string,
    device_type string,
    connection_type string,
    network_status string,
    playback_type string,
    cust_type string,
    application_group_type string,
    app_version string,
    streaming_format string,
    drm_type string,
    peak_flag string,
    failure_retries string,
    dai_insertion string,
    content_class string,
    technology_type string,
    experiment_uuid string,
    variant_uuid string,
    mos_score_bucket double,
    grouping_id int,
    stream_init_failures  bigint,
    stream_noninit_failures bigint,
    buffering_ratio double,
    pibbe_ms bigint,
    level2_downshifts bigint,
    pibd2_ms bigint,
    avg_bitrate_mbps double,
    tune_time_ms bigint,
    stream_id_count bigint,
    account_id_count bigint,
    device_count string,
    stream_length_gt_2_min string,
    account_sample string,
    metric_name string,
    metric_value double
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
