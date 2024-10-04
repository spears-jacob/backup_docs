create external table IF NOT EXISTS ${db_name}.asp_msa_web_troubleshoot_all_agg (
  count_                              BIGINT
 ,operation_type                      STRING
 ,operation_name                      STRING
 ,avg_service_result                  FLOAT
 ,service_successes                   Int
 ,service_fails                       Int
 ,service_partFails                   INT
 ,service_NULLs                       INT
 ,avg_operation_success               FLOAT
 ,response_time_5percentile_ms        INT
 ,response_time_25percentile_ms       INT
 ,response_time_median_ms             INT
 ,response_time_75percentile_ms       INT
 ,response_time_95percentile_ms       BIGINT
 ,response_time_ms_bin                STRING
 ,tot_response_ms_in_bin              BIGINT
 ,nnull_count                         BIGINT
 ,response_time_mean_ms               INT
 ,response_time_total_ms              BIGINT
 ,error_code                          STRING
 ,error_type                          STRING
 ,page_name                           STRING
 ,tab_name                            STRING
 ,app_name                            STRING
 )
COMMENT 'aggregated information table to feed troubleshootBaselineMetrics tableau report'
PARTITIONED BY (partition_date_utc STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
