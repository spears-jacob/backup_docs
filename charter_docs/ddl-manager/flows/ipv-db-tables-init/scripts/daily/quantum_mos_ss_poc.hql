CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_mos_ss
(
    mso STRING,
    application_type STRING,
    device_type STRING,
    connection_type STRING,
    network_status STRING,
    playback_type STRING,
    cust_type STRING,
    application_group_type STRING,
    app_version STRING,
    streaming_format STRING,
    drm_type STRING,
    peak_flag STRING,
    failure_retries STRING,
    dai_insertion STRING,
    content_class STRING,
    grouping_id INT,
    stream_id_count INT,
    stream_init_failures INT,
    stream_noninit_failures INT,
    buffering_ratio DOUBLE,
    pibbe_ms DOUBLE,
    level2_downshifts INT,
    pibd2_ms INT,
    avg_bitrate_mbps DOUBLE,
    tune_time_ms INT,
    metric_name STRING,
    metric_value DOUBLE,
    technology_type STRING,
    experimentuuid STRING,
    variant_uuid STRING
)
    PARTITIONED BY (agg_level STRING, breakout_number INT, denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
