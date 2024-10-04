CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_quality_kpi
(
    timeframe             STRING,
    application_name      STRING,
    metric_name           STRING,
    metric_value          DOUBLE
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
