use ${env:ENVIRONMENT};

--updates table with data from previous seven days (relative to RUN_DATE)

insert overwrite table cs_call_care_data_agg
partition (call_end_date_utc)
SELECT
customer_type
,agent_mso
,account_agent_mso
,issue_description
,cause_description
,resolution_description
,truck_roll_flag
,segment_handled_flag
,count(segment_id) as segments
,count(distinct call_inbound_key) as calls
,count(distinct encrypted_account_number_256) as accounts
,sum(case when segment_duration_minutes>0 then segment_duration_minutes else 0 end) as segment_duration_minutes
,call_end_date_utc
from prod.red_cs_call_care_data_v
where call_end_date_utc >='${hiveconf:load_date}'
group by
customer_type
,agent_mso
,account_agent_mso
,issue_description
,cause_description
,resolution_description
,truck_roll_flag
,segment_handled_flag
,call_end_date_utc
;
