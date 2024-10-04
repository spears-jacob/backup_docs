CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_msaweb_troubleshoot_raw (
    operation_type            STRING
   ,operation_name            STRING
   ,service_result_successes  INT
   ,service_result_fails      INT
   ,service_result_partFails  INT
   ,service_result_NULLs      INT
   ,operation_success         INT
   ,response_time_ms          INT
   ,error_code                STRING
   ,error_type                STRING
   ,page_name                 STRING
  )
  COMMENT 'Raw table for aggregations to feed tableau report'
  PARTITIONED BY (
  partition_date_utc STRING
  )
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
