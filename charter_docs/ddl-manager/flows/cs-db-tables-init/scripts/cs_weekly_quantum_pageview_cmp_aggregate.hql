CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_weekly_quantum_pageview_cmp_aggregate
(
    application_name      STRING,
    application_api_host  string,
    URL                   string,
    CMP                   string,
    message_name          STRING,
    count_of_pageviews    int,
    count_of_unique_users int,
    count_of_users        int,
    count_of_visits       int,
    week_of_year          string,
    calendar_year         string
)
    PARTITIONED BY
        (
        week_starting string
        )
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
