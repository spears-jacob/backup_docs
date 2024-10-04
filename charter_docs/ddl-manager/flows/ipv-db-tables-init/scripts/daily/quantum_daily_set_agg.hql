CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_set_agg
(
    mso                    string,
    application_type       string,
    device_type            string,
    connection_type        string,
    network_status         string,
    playback_type          string,
    cust_type              STRING,
    application_group_type STRING,
    app_version            STRING,
    stream_subtype         String,
    grouping_id            bigint,
    metric_name            string,
    metric_value           double,
    week_end               STRING,
    month_start            STRING,
    content_class          STRING,
    technology_type        STRING,
    legacy_grouping_id     int
    
)
    partitioned by (denver_date string)
    stored as orc
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
