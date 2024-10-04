CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_calendar_monthly_pageview_selectaction_aggregate
(
    application_name         string,
    current_page             string,
    element_name             string,
    calendar_year            int,
    count_of_buttonclicks    int,
    count_of_unique_visitors int,
    count_of_visitors        int,
    count_of_distinct_visits int
)
    PARTITIONED BY
        (
        calendar_month string
        )
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
