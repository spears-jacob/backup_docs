CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_weekly_sso_auth_set_agg
(
    auth_origin STRING,
    auth_type STRING,
    device_type STRING,
    grouping_id INT,
    metric_name STRING,
    metric_value BIGINT,
    oob_sold STRING
)
    PARTITIONED BY (week_end STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress"="SNAPPY");
