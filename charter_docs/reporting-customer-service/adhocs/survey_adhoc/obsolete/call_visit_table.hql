set mapreduce.input.fileinputformat.split.maxsize=100000000;
set mapreduce.input.fileinputformat.split.minsize=100000000;
set hive.auto.convert.join=false;

DROP TABLE IF EXISTS dev_tmp.cs_calls_with_prior_visit_adhoc;
CREATE TABLE dev_tmp.cs_calls_with_prior_visit_adhoc
  AS
  SELECT
         a.account_number as visit_account_number,
         a.visit_id,
         a.customer_type as visit_customer_type,
         a.message__category,
         a.message__name,
         a.received__timestamp,
         a.partition_date_utc,
         a.visit_type,
         b.account_number,
         b.agent_effective_hire_date,
         b.agent_job_role_name,
         b.agent_mso,
         b.call_end_date_utc,
         b.call_end_datetime_utc,
         b.call_end_time_utc,
         b.call_end_timestamp_utc,
         b.call_id,
         b.call_inbound_key,
         b.call_start_date_utc,
         b.call_start_datetime_utc,
         b.call_start_time_utc,
         b.call_start_timestamp_utc,
         b.call_type,
         b.care_center_management_name,
         b.cause_description,
         b.company_code,
         b.created_by,
         b.created_on,
         b.customer_account_number,
         b.segment_handled_flag,
         b.customer_subtype,
         b.customer_type,
         b.eduid,
         b.issue_description,
         b.last_handled_segment_flag,
         b.location_name,
         b.notes_txt,
         b.phone_number_from_tracker,
         b.previous_call_time_utc,
         b.product,
         b.record_update_timestamp,
         b.resolution_description,
         b.segment_duration_minutes,
         b.segment_duration_seconds,
         b.segment_end_datetime_utc,
         b.segment_end_time_utc,
         b.segment_end_timestamp_utc,
         b.segment_number,
         b.segment_start_datetime_utc,
         b.segment_start_time_utc,
         b.segment_start_timestamp_utc,
         b.service_call_tracker_id,
         b.split_sum_desc,
         b.truck_roll_flag
  FROM prod_tmp.cs_page_visits_quantum a                                                 -- Table containing the spectrum.net, SB.net, and myspectrum app visits for the latest extract timeframe
    INNER JOIN prod.cs_call_data b ON prod.aes_decrypt(a.account_number) = prod.aes_decrypt256(b.account_number) --Calls and visits are linked together based on customer account information
  WHERE b.enhanced_account_number = 0
  AND b.call_end_date_utc > '2019-04-01' --limiting calls to cover the new extract dates
  AND (call_start_timestamp_utc/1000 - received__timestamp) BETWEEN '0' AND '86400'

;

set hive.cli.print.header=true;
SELECT 
visit_id
,prod.epoch_converter(received__timestamp,'America/Denver')
,prod.epoch_convereter(call_start_timestamp_utc,'America/Denver')
 FROM dev_tmp.cs_calls_with_prior_visit_adhoc WHERE call_end_date_utc>='2019-04-01' LIMIT 10;
