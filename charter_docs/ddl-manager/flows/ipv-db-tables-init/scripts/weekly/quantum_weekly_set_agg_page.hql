CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_weekly_set_agg_page
(
    mso                    string,
    application_type       string,
    device_type            string,
    connection_type        string,
    network_status         string,
    playback_type          string,
    cust_type              STRING,
    application_group_type string,
    app_version            STRING,
    grouping_id            int,
    metric_name            string,
    metric_value           double
)
    PARTITIONED BY (week_end STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");

