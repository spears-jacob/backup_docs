CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_weekly_pageview_selectaction_aggregate
(
    application_name         string,
    current_page             string,
    element_name             string,
    week_of_year             int,
    calendar_year            int,
    count_of_buttonclicks    int,
    count_of_unique_visitors int,
    count_of_visitors        int,
    count_of_distinct_visits int
)
    PARTITIONED BY
        (
        week_starting string
        )
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
