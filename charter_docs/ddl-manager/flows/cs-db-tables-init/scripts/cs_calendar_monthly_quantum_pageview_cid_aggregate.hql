CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_calendar_monthly_quantum_pageview_cid_aggregate
(
    application_name      STRING,
    application_api_host  string,
    URL                   string,
    CID                   string,
    message_name          STRING,
    count_of_pageviews    int,
    count_of_unique_users int,
    count_of_users        int,
    count_of_visits       int
)
    PARTITIONED BY
        (
        calendar_month string
        )
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
