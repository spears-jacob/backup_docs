CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_product_monthly_metrics
(
    application_name string,
    metric_name      string,
    metric_value     decimal(15, 3),
    run_date         Date
)
    PARTITIONED BY (label_date_denver string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
