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
group by call_end_date_utc
;
