CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_support_content_agg (
    application_name                      string, 
    page_name                             string, 
    journey                               string, 
    sub_journey                           string, 
    page_id                               string, 
    pageviews_with_calls                  int, 
    visitors                              int, 
    authenticated_visitors                int, 
    households                            int, 
    visits                                int, 
    pageviews                             int, 
    helpful_yes                           int, 
    helpful_no                            int, 
    wasnt_what_i_searched                 int, 
    incorrect_info                        int, 
    confusing                             int, 
    distinct_visits_with_calls            int, 
    total_distinct_visits                 int, 
    call_in_rate                          decimal(12,4), 
    search_text                           string, 
    search_instances                      int
)
PARTITIONED BY (denver_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
