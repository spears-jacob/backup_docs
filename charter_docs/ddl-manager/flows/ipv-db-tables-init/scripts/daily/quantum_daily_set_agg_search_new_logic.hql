CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_set_agg_search_new_logic
(
    grouping_id                   int,
    mso                           string,
    application_type              string,
    device_type                   string,
    connection_type               string,
    network_status                string,
    playback_type                 string,
    cust_type                     string,
    application_group_type        string,
    app_version                   string,
    applicationplatform_partition string,
    metric_name                   STRING,
    metric_value                  DOUBLE
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");

