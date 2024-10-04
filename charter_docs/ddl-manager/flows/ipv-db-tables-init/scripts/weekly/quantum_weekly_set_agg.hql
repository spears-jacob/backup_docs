CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_weekly_set_agg(
    mso                    STRING,
    application_type       STRING,
    device_type            STRING,
    connection_type        STRING,
    network_status         STRING,
    playback_type          STRING,
    cust_type              STRING,
    application_group_type STRING,
    app_version            STRING,
    stream_subtype         String,
    grouping_id            INT,
    metric_name            STRING,
    metric_value           DOUBLE,
    month_start            STRING,
    content_class          STRING,
    technology_type        STRING
)
    PARTITIONED BY (week_end STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
