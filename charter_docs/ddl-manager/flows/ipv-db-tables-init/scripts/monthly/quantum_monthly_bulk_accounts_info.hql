CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_monthly_bulk_accounts_info(
    month_start          string,
    billing_id           string,
    location_zipcode     string,
    unique_devices       bigint,
    unique_visits        bigint,
    total_watch_time     bigint,
    consumed_vod_perc    double,
    consumed_linear_perc double
)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
