use ${env:ENVIRONMENT};

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
