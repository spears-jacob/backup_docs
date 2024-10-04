CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_product_monthly_last_6months
(
    metric_name        string,
    application_name   string,
    metric_order       string,
    application_order  string,
    report_metric_name string,
    3m_filter          string,
    metric_value       decimal(15, 3),
    run_date           date
)
    PARTITIONED BY (label_date_denver string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
