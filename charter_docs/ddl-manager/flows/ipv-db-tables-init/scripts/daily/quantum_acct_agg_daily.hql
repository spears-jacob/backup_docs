CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_acct_agg_daily(
    mso               STRING,
    application_type  STRING,
    acct_id           STRING,
    billing_division  STRING,
    billing_id        STRING,
    no_identity_check INT,
    unique_day        INT,
    watch_time_ms     BIGINT
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")

