


TRUNCATE TABLE dev.cs_call_data_preload;

SELECT "**Begin loading table ${env:ENVIRONMENT}.cs_call_data_preload";
--Combine data sets and perform date functions on call and segment start/end dates times
--Purposely storing multiple formats of datetime/timestamp to support downstream processing
INSERT INTO ${env:ENVIRONMENT}.cs_call_data_preload PARTITION (call_end_date_utc)
SELECT
  tmp.call_inbound_key
  ,call_id
  ,substr(call_start_datetime_utc,0,10) call_start_date_utc
  ,substr(call_start_datetime_utc,12) call_start_time_utc
  ,substr(call_end_datetime_utc,12) call_end_time_utc
  ,call_start_datetime_utc
  ,call_end_datetime_utc
  ,prod.datetime_converter(cast(call_start_datetime_utc AS STRING),'UTC','UTC') call_start_timestamp_utc
  ,prod.datetime_converter(cast(call_end_datetime_utc AS STRING),'UTC','UTC') call_end_timestamp_utc
  ,cast('' as bigint) as previous_call_time

  ,concat(tmp.call_inbound_key,'-',call_id,'-',segment_number) segment_id
  ,segment_number
  ,segment_status_disposition
  ,substr(segment_start_datetime_utc,12) segment_start_time_utc
  ,substr(segment_end_datetime_utc,12) segment_end_time_utc
  ,segment_start_datetime_utc
  ,segment_end_datetime_utc
  ,prod.datetime_converter(cast(segment_start_datetime_utc AS STRING),'UTC','UTC') segment_start_timestamp_utc
  ,prod.datetime_converter(cast(segment_end_datetime_utc AS STRING),'UTC','UTC') segment_end_timestamp_utc
  ,abs(unix_timestamp(segment_start_datetime_utc)
  - unix_timestamp(segment_end_datetime_utc)) segment_duration_seconds
  ,abs(unix_timestamp(segment_start_datetime_utc)
  - unix_timestamp(segment_end_datetime_utc))/60 segment_duration_minutes

  -- Filter helps identify only handled call
  ,CASE WHEN customer_call_count_indicator = 1
           AND call_handled_flag = 1
           AND LOWER(call_owner) in ('customer operations')
           AND LOWER(segment_status_disposition) in ('answered by an agent')
           THEN 1 ELSE 0 END
  ,CASE WHEN customer_call_count_indicator = '?' THEN NULL ELSE customer_call_count_indicator END
  ,call_handled_flag
  ,CASE WHEN call_owner = '?' THEN NULL ELSE call_owner END
  ,CASE WHEN product = '?' THEN NULL ELSE product END
  ,account_number
  ,customer_account_number
  ,CASE WHEN TRIM(UPPER(spa.customer_type)) is null
          OR TRIM(UPPER(spa.customer_type)) = 'N/A'
            THEN 'UNMAPPED'
          ELSE TRIM(UPPER(spa.customer_type)) END as customer_type
  ,CASE WHEN TRIM(UPPER(spa.customer_subtype)) is null
          OR TRIM(UPPER(spa.customer_subtype)) = 'N/A'
            THEN 'UNMAPPED'
          ELSE TRIM(UPPER(spa.customer_subtype)) END as customer_subtype

  ,truck_roll_fl
  ,notes_txt
  ,CASE WHEN resolution_description = '?' THEN NULL ELSE resolution_description END
  ,CASE WHEN cause_description = '?' THEN NULL ELSE cause_description END
  ,CASE WHEN issue_description = '?' THEN NULL ELSE issue_description END
  ,company_code
  ,CASE WHEN service_call_tracker_id = '?' THEN NULL ELSE service_call_tracker_id END
  ,CASE WHEN created_on = '?' THEN NULL ELSE created_on END
  ,CASE WHEN created_by = '?' THEN NULL ELSE created_by END
  ,phone_number_from_tracker
  ,CASE WHEN call_type = '?' THEN NULL ELSE call_type END
  ,CASE WHEN split_sum_desc = '?' THEN NULL ELSE split_sum_desc END
  ,location_name
  ,care_center_management_name
  ,agent_job_role_name
  ,CASE WHEN agent_effective_hire_date = '1/1/1900' OR agent_effective_hire_date = '?'
    THEN NULL
    ELSE to_date(from_unixtime(unix_timestamp(agent_effective_hire_date,'MM/dd/yyyy')))
    END agent_effective_hire_date
  ,agent_mso
  ,eduid
  --Logic to correctly identify the Last Handled Segment using the Handled Segment logic provided by BI and ordering by the new segment_end_datetime
  ,case when row_number() over (partition by tmp.call_inbound_key order by CASE WHEN customer_call_count_indicator = 1
           AND call_handled_flag = 1
           AND LOWER(call_owner) in ('customer operations')
           AND LOWER(segment_status_disposition) in ('answered by an agent')
           THEN 0 ELSE 1 END,segment_end_datetime_utc desc) = 1 then 1 else 0 end
  ,record_update_timestamp
  ,'extract' AS source
  ,0 AS enhanced_account_number
  ,to_date(call_end_datetime_utc)
FROM ${env:TMP_db}.cs_call_data_P270_tmp tmp
--Join to the customer_type dataset created in the load_customer_type.hql script
  LEFT JOIN
  (
    SELECT CALL_INBOUND_KEY, cs_CUSTOMER_TYPE customer_type, cs_CUSTOMER_SUBTYPE customer_subtype
    FROM dev.cs_spa_calls_and_customer_types
    WHERE CS_CUSTOMER_TYPE IS NOT NULL
    AND FRANCHISE_UNIQUE_ID IS NOT NULL
  ) spa
    ON tmp.call_inbound_key = SPA.call_inbound_key
;



SELECT "**End preload table load";
