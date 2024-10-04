use ${env:ENVIRONMENT};

--table that aggregates calls & segments by call and visit data from cs_calls_with_prior_visits

create table if not exists cs_calls_with_prior_visits_agg
(
customer_type             string  comment "Indicates whether caller was identified as residential, small business, etc."
,agent_mso                string  comment "The MSO of the agent who took the call"
,account_agent_mso        string  comment "When available, the mso of the account. If that's not available, agent_mso"
,visit_type               string  comment "Which Portal was visited"
,issue_description        string  comment "Lowest level of disposition granularity"
,cause_description        string  comment "Medium level of disposition granularity"
,resolution_description  string  comment "Highest level of dispositon granularity"
,segments                 bigint     comment "COUNT of segments where segment_handled_flag = 1 and account_number is not enhanced"
,calls                    bigint     comment "COUNT of distinct calls where segment_handled_flag = 1 and account_number is not enhanced"
,accounts                 bigint     comment "COUNT of distinct accounts where segment_handled_flag = 1 and account_number is not enhanced"
)
partitioned by
(
call_date         date    comment "Date when call ended"
)
TBLPROPERTIES ('retention_policy'='Aggregate - Without PII (3 Years)');

insert into table cs_calls_with_prior_visits_agg
partition (call_date)
select
customer_type
,agent_mso
,account_agent_mso
,visit_type
,issue_description
,cause_description
,resolution_description
,count(1) as segments
,count(distinct cd.call_inbound_key) as calls
,count(distinct cd.account_number) as accounts
,call_date
from dev.cs_calls_with_prior_visits cd
where call_date <= '${hiveconf:prior_day}'
group by
customer_type
,agent_mso
,account_agent_mso
,visit_type
,issue_description
,cause_description
,resolution_description
,call_date
;

--view that uses above table and brings in new disposition grouping tables to always keep versioning up to date

create view if not exists cs_calls_with_prior_visits_research_v as
select
cwpva.*
,ig.issue_group
,cg.cause_group
,rg.resolution_group
from ${env:ENVIRONMENT}.cs_calls_with_prior_visits_agg cwpva
left join prod_lkp.cs_dispositions_issue_groups ig on cwpva.issue_description = ig.issue_description
  and ig.version = "current"
left join prod_lkp.cs_dispositions_cause_groups cg on cwpva.cause_description = cg.cause_description
  and cg.version = "current"
left join prod_lkp.cs_dispositions_resolution_groups rg on cwpva.resolution_description = rg.resolution_description
  and rg.version = "current"
;
