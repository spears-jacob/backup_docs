CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_acct_agg
(
    mso               STRING,
    application_type  STRING,
    acct_id           STRING,
    billing_division  STRING,
    billing_id        STRING,
    no_identity_check INT,
    days_active       INT,
    unique_days       INT,
    first_active      STRING,
    last_active       STRING,
    watch_time_ms     BIGINT
)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
