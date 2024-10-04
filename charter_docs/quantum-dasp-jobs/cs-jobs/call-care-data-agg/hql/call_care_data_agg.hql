USE ${env:DASP_db};

set hive.exec.dynamic.partition.mode=nonstrict;

--updates table with data from previous seven days (relative to RUN_DATE)


MSCK REPAIR TABLE ${env:ENVIRONMENT}.atom_cs_call_care_data_3;

insert overwrite table ${env:DASP_db}.cs_call_care_data_agg
partition (call_end_date_utc)
SELECT
customer_type,
agent_mso,
account_agent_mso,
primary_auto_disposition_issue,
primary_auto_disposition_cause,
issue_description as primary_manual_disposition_issue,
cause_description as primary_manual_disposition_cause,
resolution_description as primary_manual_disposition_resolution,
coalesce(primary_auto_disposition_issue,issue_description) as issue_description,
coalesce(primary_auto_disposition_cause,cause_description) as cause_description,
resolution_description,
truck_roll_flag,
segment_handled_flag,
call_type, --added 2020-07-10
count(segment_id) as segments,
count(distinct call_inbound_key) as calls,
count(distinct encrypted_account_number_256) as accounts,
sum(case when segment_duration_minutes>0 then segment_duration_minutes else 0 end) as segment_duration_minutes,
call_end_date_utc
from ${env:ENVIRONMENT}.atom_cs_call_care_data_3
where call_end_date_utc >='${hiveconf:load_date}'
group by
customer_type,
agent_mso,
account_agent_mso,
primary_auto_disposition_issue,
primary_auto_disposition_cause,
coalesce(primary_auto_disposition_issue,issue_description),
coalesce(primary_auto_disposition_cause,cause_description),
issue_description,
cause_description,
resolution_description,
truck_roll_flag,
segment_handled_flag,
call_type,
call_end_date_utc
;
