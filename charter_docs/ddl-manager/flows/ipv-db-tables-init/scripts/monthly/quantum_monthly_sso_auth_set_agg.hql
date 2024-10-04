CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_monthly_sso_auth_set_agg
(
    auth_origin  STRING,
    auth_type    STRING,
    device_type  STRING,
    grouping_id  INT,
    metric_name  STRING,
    metric_value BIGINT,
    oob_sold     STRING
)
    PARTITIONED BY (month_start STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
