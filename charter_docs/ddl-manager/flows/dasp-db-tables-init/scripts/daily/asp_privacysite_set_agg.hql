CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_privacysite_set_agg
(
    app_section        STRING,
    user_role          STRING,
    message_context    STRING,
    unit_type          STRING,
    metric_name        STRING,
    metric_value       DOUBLE
)
    PARTITIONED BY (partition_date_utc STRING, grain STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
