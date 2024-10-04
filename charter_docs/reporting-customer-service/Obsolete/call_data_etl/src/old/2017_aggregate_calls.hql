DROP TABLE IF EXISTS dev.steve_call_data_2017_calls;
CREATE TABLE dev.steve_call_data_2017_calls
(
  call_inbound_key bigint
  ,account_number_aes256 string
  ,call_start_date_utc date
  ,call_start_time_utc string
  ,call_start_datetime_utc string
  ,call_start_timestamp_utc bigint
  ,call_end_time_utc string
  ,call_end_datetime_utc string
  ,call_end_timestamp_utc bigint
  ,previous_call_time_utc string
  ,truck_roll_flag boolean
  ,total_call_duration_seconds decimal(10,2)
  ,handled_duration_seconds decimal(10,2)
  ,billing_category boolean
  ,repair_category boolean
  ,sales_category boolean
  ,dispatch_category boolean
  ,retention_category boolean
)
partitioned by
(
  call_end_date_utc date
);


WITH CALLS AS
(
  SELECT CALL_INBOUND_KEY, account_number, CALL_START_DATE_UTC, CALL_START_TIME_UTC, CALL_START_DATETIME_UTC, CALL_START_TIMESTAMP_UTC, CALL_END_DATE_UTC, CALL_END_TIME_UTC, CALL_END_DATETIME_UTC, CALL_END_TIMESTAMP_UTC
  ,previous_call_time_utc
  ,CASE WHEN SUM(CAST(TRUCK_ROLL_FLAG AS INT)) >= 1 THEN 1 ELSE 0 END TRUCK_ROLL_FLAG
  ,(CALL_END_TIMESTAMP_UTC-CALL_START_TIMESTAMP_UTC)/1000 total_call_duration_seconds
  ,SUM(CASE WHEN SEGMENT_HANDLED_FLAG = 1 THEN SEGMENT_DURATION_SECONDS ELSE 0 END) handled_duration_seconds
  FROM DEV.steve_CALL_DATA_2017
  GROUP BY CALL_INBOUND_KEY, account_number, CALL_START_DATE_UTC, CALL_START_TIME_UTC, CALL_START_DATETIME_UTC, CALL_END_DATE_UTC, CALL_END_TIME_UTC,  CALL_END_DATETIME_UTC, CALL_START_TIMESTAMP_UTC, CALL_END_TIMESTAMP_UTC
  ,previous_call_time_utc
)
,CATEGORIES AS
(
  SELECT CALL_INBOUND_KEY
  ,array_contains(B.category,'BILLING') billing_category
  ,array_contains(B.category,'REPAIR') repair_category
  ,array_contains(B.category,'SALES') sales_category
  ,array_contains(B.category,'DISPATCH') dispatch_category
  ,array_contains(B.category,'RETENTION') retention_category
  FROM
  (
    SELECT call_inbound_key
    ,collect_list(A.call_category[call_inbound_key]) as category
    FROM
    (
      SELECT CALL_INBOUND_KEY
      ,map(call_inbound_key, split_sum_desc) as call_category
      FROM DEV.steve_CALL_DATA_2017
      GROUP BY call_inbound_key, split_sum_desc
    ) A
    GROUP BY call_inbound_key
  ) B
)
INSERT INTO dev.steve_call_data_2017_calls PARTITION (call_end_date_utc)
SELECT calls.CALL_INBOUND_KEY, account_number, CALL_START_DATE_UTC, CALL_START_TIME_UTC, CALL_START_DATETIME_UTC, CALL_START_TIMESTAMP_UTC, CALL_END_TIME_UTC, CALL_END_DATETIME_UTC, CALL_END_TIMESTAMP_UTC
,previous_call_time_utc ,truck_roll_flag, total_call_duration_seconds, handled_duration_seconds
,categories.billing_category ,categories.repair_category, categories.sales_category, categories.dispatch_category, categories.retention_category
,call_end_date_utc
FROM CALLS
  LEFT JOIN CATEGORIES
    ON CALLS.CALL_INBOUND_KEY = CATEGORIES.CALL_INBOUND_KEY
;
