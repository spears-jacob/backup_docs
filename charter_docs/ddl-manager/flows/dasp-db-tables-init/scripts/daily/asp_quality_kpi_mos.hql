CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_quality_kpi_mos
(
    timeframe  STRING,
    grouping_id INT,
    application_name STRING,
    application_version STRING,
    metric_name  STRING,
    metric_value DOUBLE,
    duration_bucket_filtered_ratio DOUBLE,
    page_load_time_bucket_filtered_ratio DOUBLE,
    login_failure_bucket_filtered_ratio DOUBLE,
    duration_bucket DOUBLE,
    page_load_time_bucket DOUBLE,
    login_failure_bucket DOUBLE,
    login_success_derived DOUBLE,
    login_success DOUBLE,
    otp_both_derived DOUBLE,
    otp_both DOUBLE,
    otp_success DOUBLE,
    otp_failure_not DOUBLE,
    autopay_all_derived DOUBLE,
    autopay_all DOUBLE,
    autopay_success DOUBLE,
    autopay_failure_not DOUBLE
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
