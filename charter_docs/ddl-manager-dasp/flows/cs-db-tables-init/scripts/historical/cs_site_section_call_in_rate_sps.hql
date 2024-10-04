CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_site_section_call_in_rate_sps
(
    `application_name`                    string,
    `current_app_section`                 string,
    `mso`                                 string,
    `call_in_rate`                        double,
    `source_system`                       string,
    `total_sectionviews`                  bigint,
    `total_distinct_visits`               bigint,
    `count_of_sectionviews_with_calls`    bigint,
    `count_of_distinct_visits_with_calls` bigint,
    `sps_flag`                            string
)
    PARTITIONED BY (`partition_date_utc` string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY") 
