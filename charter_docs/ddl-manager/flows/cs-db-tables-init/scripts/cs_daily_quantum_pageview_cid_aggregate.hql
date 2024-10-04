CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_daily_quantum_pageview_cid_aggregate
(
    application_name      STRING,
    application_api_host  string,
    URL                   string,
    CID                   string,
    message_name          STRING,
    count_of_events       int,
    count_of_unique_users int,
    count_of_users        int,
    count_of_visits       int
)
    PARTITIONED BY
        (
        partition_date_utc date
        )
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
