CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_api_set_weekly_agg
(
    grouping_id       BIGINT,
    application_type  STRING,
    mso               STRING,
    cust_type         STRING,
    api_category      STRING,
    api_name          STRING,
    stream_subtype    String,
    current_page_name STRING,
    metric_name       STRING,
    metric_value      DOUBLE,
    month_start       STRING,
    technology_type   STRING
)
    PARTITIONED BY (week_end STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
