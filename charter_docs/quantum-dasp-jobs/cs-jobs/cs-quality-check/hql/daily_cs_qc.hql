use ${env:ENVIRONMENT};

SELECT"

\n\n*****\n\nQC Table = atom_cs_call_care_data_3 QC check = #day+ segment duration\n\n*****\n\n

"
;

insert overwrite table cs_qc
partition (qc_date = '${env:RUN_DATE}',qc_table = 'atom_cs_call_care_data_3',qc_check = '#day+ segment duration')
select
call_end_date_utc as data_date
,NULL as data_desc
,COUNT(1) as numeric_data
from atom_cs_call_care_data_3
where segment_handled_flag = 1
and segment_duration_minutes >= 1440
and call_end_date_utc between '${hiveconf:thirtyone_days}' and '${hiveconf:two_days}'
group by call_end_date_utc;

SELECT"

\n\n*****\n\nQC Table = atom_cs_call_care_data_3 QC check = max segment duration\n\n*****\n\n

"
;

insert overwrite table cs_qc
partition (qc_date = '${env:RUN_DATE}',qc_table = 'atom_cs_call_care_data_3',qc_check = 'max segment duration')
select
call_end_date_utc as data_date
,NULL as data_desc
,MAX(segment_duration_minutes) as numeric_data
from atom_cs_call_care_data_3
where segment_handled_flag = 1
and segment_duration_minutes >= 1440
and call_end_date_utc between '${hiveconf:thirtyone_days}' and '${hiveconf:two_days}'
group by call_end_date_utc;

SELECT"

\n\n*****\n\nQC Table = atom_cs_call_care_data_3 QC check = record count\n\n*****\n\n

"
;

insert overwrite table cs_qc
partition (qc_date = '${env:RUN_DATE}',qc_table = 'atom_cs_call_care_data_3',qc_check = 'record count')
select
call_end_date_utc as data_date
,NULL as data_desc
,COUNT(1) as numeric_data
from atom_cs_call_care_data_3
where segment_handled_flag = 1
and call_end_date_utc between '${hiveconf:thirtyone_days}' and '${hiveconf:two_days}'
group by call_end_date_utc;

SELECT"

\n\n*****\n\nQC Table = cs_call_in_rate QC check = record count\n\n*****\n\n

"
;

insert overwrite table cs_qc
partition (qc_date = '${env:RUN_DATE}',qc_table = 'cs_call_in_rate',qc_check = 'record count')
select
call_date as data_date
,NULL as data_desc
,COUNT(1) as numeric_data
from cs_call_in_rate
where visit_type <> 'IDMANAGEMENT'
and call_date between '${hiveconf:thirtyone_days}' and '${hiveconf:two_days}'
group by call_date;

SELECT"

\n\n*****\n\nQC Table = cs_call_in_rate QC check = NULLs & 0s\n\n*****\n\n

"
;

insert overwrite table cs_qc
partition (qc_date = '${env:RUN_DATE}',qc_table = 'cs_call_in_rate',qc_check = 'NULLs & 0s')
select
call_date as data_date
,'number of bad NULLs or 0s' as data_desc
,COUNT(1) as numeric_data
from cs_call_in_rate
where visit_type <> 'IDMANAGEMENT'
and call_date between '${hiveconf:thirtyone_days}' and '${hiveconf:two_days}'
and (
(agent_mso is null or agent_mso not in ('UNMAPPED','CHR','TWC','BHN','SPE'))
or (visit_type is null or visit_type not in ('SMB','SPECNET','MYSPECTRUM','UNKNOWN'))
or (customer_type is null or customer_type not in ('COMMERCIAL','RESIDENTIAL','UNMAPPED'))
or (calls_with_visit is null or (calls_with_visit = 0 and visit_type <> 'UNKNOWN' and agent_mso <> 'UNMAPPED'))
or (total_acct_visits is null or (total_acct_visits = 0 and visit_type <> 'UNKNOWN' and agent_mso <> 'UNMAPPED'))
or (total_visits is null or (total_visits = 0 and visit_type <> 'UNKNOWN'))
or (handled_acct_calls is null or (handled_acct_calls = 0 and agent_mso <> 'UNMAPPED'))
or (total_acct_calls is null or (total_acct_calls = 0 and agent_mso <> 'UNMAPPED'))
or (total_calls is null or (total_calls = 0 and agent_mso <> 'UNMAPPED'))
)
group by call_date;

SELECT"

\n\n*****\n\nQC Table = cs_calls_with_prior_visits QC check = record count\n\n*****\n\n

"
;

insert overwrite table cs_qc
partition (qc_date = '${env:RUN_DATE}',qc_table = 'cs_calls_with_prior_visits',qc_check = 'record count')
select
call_date as data_date
,NULL as data_desc
,COUNT(1) as numeric_data
from cs_calls_with_prior_visits
where call_date between '${hiveconf:thirtyone_days}' and '${hiveconf:two_days}'
group by call_date;

SELECT"

\n\n*****\n\nQC Table = cs_page_view_call_in_rate QC check = record count\n\n*****\n\n

"
;

insert overwrite table cs_qc
partition (qc_date = '${env:RUN_DATE}',qc_table = 'cs_page_view_call_in_rate',qc_check = 'record count')
select
partition_date_utc as data_date
,NULL as data_desc
,COUNT(1) as numeric_data
from cs_page_view_call_in_rate
where partition_date_utc between '${hiveconf:thirtyone_days}' and '${hiveconf:two_days}'
group by partition_date_utc;

SELECT"

\n\n*****\n\nQC Table = cs_site_section_call_in_rate QC check = record count\n\n*****\n\n

"
;

insert overwrite table cs_qc
partition (qc_date = '${env:RUN_DATE}',qc_table = 'cs_site_section_call_in_rate',qc_check = 'record count')
select
partition_date_utc as data_date
,NULL as data_desc
,COUNT(1) as numeric_data
from cs_site_section_call_in_rate
where partition_date_utc between '${hiveconf:thirtyone_days}' and '${hiveconf:two_days}'
group by partition_date_utc;
