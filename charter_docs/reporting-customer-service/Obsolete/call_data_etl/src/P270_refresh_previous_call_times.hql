
use ${env:ENVIRONMENT};

TRUNCATE TABLE ${env:ENVIRONMENT}.cs_call_data_preload;

CREATE TEMPORARY TABLE ${env:ENVIRONMENT}.cs_call_data_previouscalltime AS
SELECT call_inbound_key
,account_number
,LAG(min_call_time)
  OVER (PARTITION BY account_number ORDER BY min_call_time) previous_call_time_utc
FROM
(
SELECT call_inbound_key, account_number, min(call_start_timestamp_utc) min_call_time
FROM ${env:ENVIRONMENT}.cs_call_data
WHERE last_handled_segment_flag = 1
AND lower(dev.aes_decrypt256(account_number)) <> 'unknown'
AND dev.aes_decrypt256(account_number) IS NOT NULL
GROUP BY call_inbound_key, account_number
) dt;



INSERT INTO ${env:ENVIRONMENT}.CS_CALL_DATA_PRELOAD PARTITION (CALL_END_DATE_UTC)
SELECT
cd.call_inbound_key,
call_id,
call_start_date_utc,
call_start_time_utc,
call_end_time_utc,
call_start_datetime_utc,
call_end_datetime_utc,
call_start_timestamp_utc,
call_end_timestamp_utc,
pct.previous_call_time_utc,
segment_id,
segment_number,
segment_status_disposition,
segment_start_time_utc,
segment_end_time_utc,
segment_start_datetime_utc,
segment_end_datetime_utc,
segment_start_timestamp_utc,
segment_end_timestamp_utc,
segment_duration_seconds,
segment_duration_minutes,
segment_handled_flag,
customer_call_count_indicator,
call_handled_flag,
call_owner,
product,
cd.account_number,
customer_account_number,
customer_type,
customer_subtype,
truck_roll_flag,
notes_txt,
resolution_description,
cause_description,
issue_description,
company_code,
service_call_tracker_id,
created_on,
created_by,
phone_number_from_tracker,
call_type,
split_sum_desc,
location_name,
care_center_management_name,
agent_job_role_name,
agent_effective_hire_date,
agent_mso,
eduid,
last_handled_segment_flag,
record_update_timestamp,
call_end_date_utc
FROM ${env:ENVIRONMENT}.CS_CALL_DATA cd
  LEFT JOIN ${env:ENVIRONMENT}.cs_call_data_previouscalltime pct
    ON cd.call_inbound_key = pct.call_inbound_key
;
