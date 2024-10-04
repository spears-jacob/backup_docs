CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_monthly_set_agg
(
    mso                    STRING,
    application_type       STRING,
    device_type            STRING,
    connection_type        STRING,
    network_status         STRING,
    playback_type          STRING,
    cust_type              STRING,
    application_group_type STRING,
    app_version            STRING,
    grouping_id            INT,
    metric_name            STRING,
    metric_value           DOUBLE,
    logged_in_status       STRING,
    application_name       STRING,
    os_name                STRING,
    os_version             STRING,
    browser_name           STRING,
    browser_version        STRING,
    form_factor            STRING,
    stream_subtype         STRING,
    content_class          STRING,
    technology_type        STRING
)
    PARTITIONED BY (month_start STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
