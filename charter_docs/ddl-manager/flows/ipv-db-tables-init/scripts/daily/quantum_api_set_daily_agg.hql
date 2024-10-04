CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_api_set_daily_agg
(
    grouping_id       BIGINT,
    hour_denver       STRING,
    minute_group      STRING,
    application_type  STRING,
    mso               STRING,
    cust_type         STRING,
    api_category      STRING,
    api_name          STRING,
    stream_subtype    String,
    current_page_name STRING,
    metric_name       STRING,
    metric_value      DOUBLE,
    week_end          STRING,
    month_start       STRING,
    technology_type   STRING
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
