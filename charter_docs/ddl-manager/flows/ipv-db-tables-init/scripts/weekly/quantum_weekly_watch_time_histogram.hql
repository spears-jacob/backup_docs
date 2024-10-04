CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.QUANTUM_WEEKLY_WATCH_TIME_HISTOGRAM(
    granularity       string,
    application_type  string,
    bucket            int,
    watch_time_bucket double,
    bucket_count      bigint
)
    PARTITIONED BY (WEEK_END date)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
