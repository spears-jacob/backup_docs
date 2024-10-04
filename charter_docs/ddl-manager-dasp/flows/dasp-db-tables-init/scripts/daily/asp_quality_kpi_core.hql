CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_quality_kpi_core
(
    timeframe             STRING,
    grouping_id           INT,
    application_name      STRING,
    application_version   STRING,
    metric_name           STRING,
    metric_value          DOUBLE
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
