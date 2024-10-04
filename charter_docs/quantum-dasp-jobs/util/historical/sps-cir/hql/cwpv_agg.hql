use ${env:DASP_db};

set hive.exec.dynamic.partition.mode=nonstrict;
--updates table with data from previous day (relative to RUN_DATE)
set tez.grouping.split-count=50;
insert overwrite table cs_calls_with_prior_visits_agg_sps
-- partition (call_date = '${hiveconf:prior_day}')
partition (call_date)
select
customer_type
,agent_mso
,account_agent_mso
,visit_type
,issue_description
,cause_description
,resolution_description
--,truck_roll_flag
,count(1) as segments
,count(distinct call_inbound_key) as calls
,count(distinct account_number) as accounts
,sps_flag
,call_date
--from dev.cs_calls_with_prior_visits_ondeck
from cs_calls_with_prior_visits_sps
where call_date >='${hiveconf:load_date}'
group by
customer_type
,agent_mso
,account_agent_mso
,visit_type
,issue_description
,cause_description
,resolution_description
--,truck_roll_flag
,call_date
,sps_flag
;
-- revert default value of mappers
set tez.grouping.split-count=0;
