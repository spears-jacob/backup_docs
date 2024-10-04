CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_weekly_feature_set_agg
(
    application_type           STRING,
    watched_spectrum_originals STRING,
    grouping_id                INT,
    metric_name                STRING,
    metric_value               DOUBLE,
    feature_app_usage_name     STRING,
    feature_app_usage_value    ARRAY<STRING>
)
    PARTITIONED BY (week_end STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
