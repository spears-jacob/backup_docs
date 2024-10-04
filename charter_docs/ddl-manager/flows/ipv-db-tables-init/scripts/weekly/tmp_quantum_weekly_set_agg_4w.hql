CREATE EXTERNAL TABLE if not exists ${db_name}.quantum_weekly_set_agg_4w(
    mso                    string,
    application_type       string,
    device_type            string,
    connection_type        string,
    network_status         string,
    playback_type          string,
    cust_type              string,
    application_group_type string,
    app_version            string,
    stream_subtype         string,
    grouping_id            int,
    metric_name            string,
    metric_value           double,
    content_class          string,
    technology_type        string
)
    PARTITIONED BY (week_end STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
