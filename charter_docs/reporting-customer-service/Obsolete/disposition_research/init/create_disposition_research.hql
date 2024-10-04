use ${env:ENVIRONMENT};

--table for analysis of Issue, Cause, and Resolution (ICR) data over time

create table if not exists cs_disposition_research
(
customer_type             string  comment "Indicates whether caller was identified as residential, small business, etc."
,issue_description        string  comment "Raw issue_description originating from cs_call_care_data"
,issue_group              string  comment "Issue Group name created in disposition_groupings code and stored in cs_dispositions_issue_groups table"
,cause_description        string  comment "Raw cause_description originating from cs_call_care_data"
,cause_group              string  comment "Cause Group name created in disposition_groupings code and stored in cs_dispositions_cause_groups table"
,resolution_description   string  comment "Raw resolution_description originating from cs_call_care_data"
,resolution_group         string  comment "Resolution Group name created in disposition_groupings code and stored in cs_dispositions_resolution_groups table"
,truck_roll_flag          string  comment "Binary indication of whether the call agent decided a truck roll to be necessary to resolve the customer issue."
,segments                 int     comment "COUNT of segments where segment_handled_flag = 1"
,calls                    int     comment "COUNT of distinct calls where segment_handled_flag = 1"
,accounts                 int     comment "COUNT of distinct accounts where segment_handled_flag = 1"
,talk_time                float   comment "SUM of segment duration in minutes where segment_handled_flag = 1"
)
partitioned by
(
call_end_date_utc         date    comment "Date when call ended"
)
TBLPROPERTIES ('retention_policy'='Aggregate - Without PII (3 Years)');

insert into table cs_disposition_research
partition (call_end_date_utc)
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
,sum(case when cd.segment_end_datetime_utc<cd.segment_start_datetime_utc then 0 else cd.segment_duration_minutes end) as talk_time
,cd.call_end_date_utc
from prod.cs_call_care_data cd
inner join ${env:LKP_db}.cs_dispositions_issue_groups ig on cd.issue_description = ig.issue_description
  and ig.version = 'current'
inner join ${env:LKP_db}.cs_dispositions_cause_groups cg on cd.cause_description = cg.cause_description
  and cg.version = 'current'
inner join ${env:LKP_db}.cs_dispositions_resolution_groups rg on cd.resolution_description = rg.resolution_description
  and rg.version = 'current'
where cd.segment_handled_flag = 1
and cd.call_end_date_utc between '2018-01-01' and '${env:RUN_DATE}'
group by
cd.customer_type
,cd.issue_description
,ig.issue_group
,cd.cause_description
,cg.cause_group
,cd.resolution_description
,rg.resolution_group
,cd.truck_roll_flag
,cd.call_end_date_utc
;
