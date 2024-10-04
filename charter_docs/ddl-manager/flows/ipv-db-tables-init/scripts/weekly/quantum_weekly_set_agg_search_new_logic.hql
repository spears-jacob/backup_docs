CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_weekly_set_agg_search_new_logic
(
    grouping_id            int,
    mso                    string,
    application_type       string,
    device_type            string,
    connection_type        string,
    network_status         string,
    playback_type          string,
    cust_type              string,
    application_group_type string,
    app_version            string,
    metric_name            STRING,
    metric_value           DOUBLE
)
    PARTITIONED BY (week_end STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");