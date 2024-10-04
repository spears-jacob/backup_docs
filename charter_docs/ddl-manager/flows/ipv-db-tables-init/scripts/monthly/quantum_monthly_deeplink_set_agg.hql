CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_monthly_deeplink_set_agg
(
    mso                STRING,
    application_type   STRING,
    link_report_group  STRING,
    link_report_action STRING,
    grouping_id        INT,
    metric_name        STRING,
    metric_value       BIGINT
)
    PARTITIONED BY (month_start STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");

