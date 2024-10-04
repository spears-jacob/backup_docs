CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_page_view_call_in_rate
(
    `application_name`                    string,
    `current_page_name`                   string,
    `current_app_section`                 string,
    `count_of_pageviews_with_calls`       bigint,
    `total_pageviews`                     bigint,
    `count_of_distinct_visits_with_calls` bigint,
    `total_distinct_visits`               bigint,
    `call_in_rate`                        double,
    `mso`                                 string,
    `source_system`                       string
)
    PARTITIONED BY ( `partition_date_utc` string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
