CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_daily_tms_enrichment_report
(
    content_status_count bigint,
    cnt_all              bigint,
    app_type             string,
    app_version          string,
    playback             string,
    content_status       string,
    receiveddatetime     timestamp
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
