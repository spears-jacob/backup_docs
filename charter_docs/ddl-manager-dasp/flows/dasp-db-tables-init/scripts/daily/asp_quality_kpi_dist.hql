CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_quality_kpi_dist
(
    grouping_id INT,
    application_name STRING,
    application_version STRING,
    metric_name  STRING,
    number_rows INT,
    number_visits INT,
    pct_uniq_visits DOUBLE,
    pct_failure DOUBLE,
    pct_duration DOUBLE,
    pct_time DOUBLE,
    quality_score DOUBLE,
    failure_bucket DOUBLE,
    duration_bucket DOUBLE,
    time_bucket DOUBLE,
    all_true_yes INT,
    success_true_yes INT,
    failure_true_not INT
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
