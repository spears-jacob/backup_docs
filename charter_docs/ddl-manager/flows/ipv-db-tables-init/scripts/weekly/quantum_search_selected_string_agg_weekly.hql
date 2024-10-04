CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_search_selected_string_agg_weekly
(
    grouping_id            int,
    mso                    string,
    application_type       string,
    device_type            string,
    connection_type        string,
    network_status         string,
    playback_type          string,
    cust_type              string,
    application_group_type string,
    app_version            string,
    search_operation       string,
    result_category        string,
    search_text            string,
    has_result             bigint,
    no_results             bigint,
    selections             bigint
)
    PARTITIONED BY (week_end STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
