use ${env:ENVIRONMENT};

--table that aggregates calls & segments by call and visit data from cs_calls_with_prior_visits

create table if not exists cs_call_care_data_agg
(
customer_type             string  comment "Indicates whether caller was identified as residential, small business, etc."
,agent_mso                string  comment "The MSO of the agent who took the call"
,account_agent_mso        string  comment "When available, the mso of the account. If that's not available, agent_mso"
,issue_description        string  comment "Lowest level of disposition granularity"
,cause_description        string  comment "Medium level of disposition granularity"
,resolution_description   string  comment "Highest level of dispositon granularity"
,truck_roll_flag          boolean  comment "Whether or not a service call was created from this call"
,segment_handled_flag     int  comment "Whether the segment was handled by an agent or not"
,segments                 bigint     comment "COUNT of segments that fit into these categories"
,calls                    bigint     comment "COUNT of distinct calls that fit into these categories"
,accounts                 bigint     comment "COUNT of distinct accounts that fit into these categories"
,segment_duration_minutes double  comment "SUM of total talk time"
)
partitioned by
(
call_end_date_utc         date    comment "Date when call ended"
)
TBLPROPERTIES ('retention_policy'='Aggregate - Without PII (3 Years)');

insert into table cs_call_care_data_agg
partition (call_end_date_utc)
select
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
where call_end_date_utc <= '${hiveconf:prior_day}'
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

--view that uses above table and brings in new disposition grouping tables to always keep versioning up to date

create view if not exists cs_call_care_data_research_v as
select
cca.*
,ig.issue_group
,cg.cause_group
,rg.resolution_group
from ${env:ENVIRONMENT}.cs_call_care_data_agg cca
left join prod_lkp.cs_dispositions_issue_groups ig on cca.issue_description = ig.issue_description
  and ig.version = "current"
left join prod_lkp.cs_dispositions_cause_groups cg on cca.cause_description = cg.cause_description
  and cg.version = "current"
left join prod_lkp.cs_dispositions_resolution_groups rg on cca.resolution_description = rg.resolution_description
  and rg.version = "current"
;
