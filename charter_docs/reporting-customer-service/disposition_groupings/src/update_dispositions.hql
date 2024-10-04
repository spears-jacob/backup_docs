use ${env:LKP_db};
--set prior_month_start_date = add_months(trunc('${env:RUN_DATE}','MM'),-1)
--set prior_month_end_date = date_sub(trunc('${env:RUN_DATE}','MM'),1)
--inserts a new row in to cs_dispositions for any new issues, causes, or resolutions

insert overwrite table cs_dispositions
partition (partition_month = '${hiveconf:prior_month_start_date}')
select
distinct
issue_description
,regexp_replace(regexp_replace(issue_description,'[^a-zA-Z0-9\\s]',''),'  ',' ')
,cause_description
,regexp_replace(regexp_replace(cause_description,'[^a-zA-Z0-9\\s]',''),'  ',' ')
,resolution_description
,regexp_replace(regexp_replace(resolution_description,'[^a-zA-Z0-9\\s]',''),'  ',' ')
from prod.red_cs_call_care_data_v cd
where cd.call_end_date_utc between '${hiveconf:prior_month_start_date}' and '${hiveconf:prior_month_end_date}'
;
