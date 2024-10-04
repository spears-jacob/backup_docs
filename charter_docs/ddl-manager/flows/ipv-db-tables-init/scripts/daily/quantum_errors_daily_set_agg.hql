CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_errors_daily_set_agg
(
    mso               string,
    application_type  string,
    device_type       string,
    technology_type   string,
    connection_type   string,
    network_status    string,
    cust_type         string,
    app_version       string,
    content_class     string,
    current_page_name string,
    error_type        string,
    error_code        string,
    grouping_id       int,
    metric_name       string,
    metric_value      bigint
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
