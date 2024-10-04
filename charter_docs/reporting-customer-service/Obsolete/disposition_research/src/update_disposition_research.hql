use ${env:ENVIRONMENT};

--updates table with data from previous day (relative to RUN_DATE)

insert overwrite table cs_disposition_research
partition (call_end_date_utc = '${hiveconf:prior_day}')
select
cd.customer_type
,cd.issue_description
,ig.issue_group
,cd.cause_description
,cg.cause_group
,cd.resolution_description
,rg.resolution_group
,cd.truck_roll_flag
,count(1) as segments
,count(distinct cd.call_inbound_key) as calls
,count(distinct cd.account_number) as accounts
,sum(case when cd.segment_end_datetime_utc<cd.segment_start_datetime_utc  then 0 else cd.segment_duration_minutes end) as talk_time
from prod.cs_call_care_data cd
inner join ${env:LKP_db}.cs_dispositions_issue_groups ig on cd.issue_description = ig.issue_description
  and ig.version = 'current'
inner join ${env:LKP_db}.cs_dispositions_cause_groups cg on cd.cause_description = cg.cause_description
  and cg.version = 'current'
inner join ${env:LKP_db}.cs_dispositions_resolution_groups rg on cd.resolution_description = rg.resolution_description
  and rg.version = 'current'
where cd.segment_handled_flag = 1
and cd.call_end_date_utc = '${hiveconf:prior_day}'
group by
cd.customer_type
,cd.issue_description
,ig.issue_group
,cd.cause_description
,cg.cause_group
,cd.resolution_description
,rg.resolution_group
,cd.truck_roll_flag
;
