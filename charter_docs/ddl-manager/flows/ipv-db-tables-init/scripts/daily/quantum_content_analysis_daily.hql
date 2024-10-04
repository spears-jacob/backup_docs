-- Just empty script created to make 'Build' work
CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_content_analysis_daily
(
    grouping_id        STRING,
    cust_type          STRING,
    application_type   STRING,
    asset_id           STRING,
    network            STRING,
    provider           STRING,
    in_home            STRING,
    playback_type      STRING,
    title              STRING,
    episode_title      STRING,
    episode_number     STRING,
    season_number      STRING,
    stream_subtype     STRING,
    programmer_network STRING,
    unique_hh          BIGINT,
    total_streams      BIGINT,
    average_streams_hh DOUBLE,
    average_minutes    DOUBLE,
    total_minutes      DOUBLE
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
