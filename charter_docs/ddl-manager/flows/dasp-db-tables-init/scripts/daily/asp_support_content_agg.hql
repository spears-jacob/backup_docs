CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_support_content_agg (
  application_name string,
    page_name string,
    pageviews_with_calls INT,
    visitors INT,
    authenticated_visitors INT,
    households INT,
    visits INT,
    pageviews INT,
    helpful_yes INT,
    helpful_no INT,
    wasnt_what_i_searched INT,
    incorrect_info INT,
    confusing INT,
    distinct_visits_with_calls INT,
    total_distinct_visits INT,
    call_in_rate decimal(12,4),
    search_text string,
    search_instances INT
)
PARTITIONED BY (denver_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;