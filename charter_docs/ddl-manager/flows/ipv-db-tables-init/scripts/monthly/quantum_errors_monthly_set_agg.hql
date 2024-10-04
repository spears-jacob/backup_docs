CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_errors_monthly_set_agg
(
    mso               STRING,
    application_type  STRING,
    device_type       STRING,
    technology_type   STRING,
    connection_type   STRING,
    network_status    STRING,
    cust_type         STRING,
    app_version       STRING,
    content_class     STRING,
    current_page_name STRING,
    error_type        STRING,
    error_code        STRING,
    grouping_id       INT,
    metric_name       STRING,
    metric_value      BIGINT
)
    PARTITIONED BY (month_start string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
