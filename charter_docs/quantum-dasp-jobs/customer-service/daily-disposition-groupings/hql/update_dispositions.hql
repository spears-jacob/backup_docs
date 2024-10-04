USE ${env:TMP_db};

-- Create Temp Table for holding everything we're adding this time round
CREATE TABLE IF NOT EXISTS cs_dispositions_${hiveconf:CLUSTER}
  (
      issue_description      string,
      id_form                string,
      cause_description      string,
      cd_form                string,
      resolution_description string,
      rd_form                string
  )
;

-- Insert manual dispositions into that temp table
insert into cs_dispositions_${hiveconf:CLUSTER}
select
distinct
trim(issue_description)
,trim(regexp_replace(regexp_replace(issue_description,'[^a-zA-Z0-9\\s]',''),'  ',' '))
,trim(cause_description)
,trim(regexp_replace(regexp_replace(cause_description,'[^a-zA-Z0-9\\s]',''),'  ',' '))
,trim(resolution_description)
,trim(regexp_replace(regexp_replace(resolution_description,'[^a-zA-Z0-9\\s]',''),'  ',' '))
from ${env:ENVIRONMENT}.atom_cs_call_care_data_3 cd
where cd.call_end_date_utc between '${hiveconf:prior_month_start_date}' and '${hiveconf:prior_month_end_date}'
;

--insert auto dispositions into that temp table
insert into cs_dispositions_${hiveconf:CLUSTER}
select
distinct
trim(primary_auto_disposition_issue)
,trim(regexp_replace(regexp_replace(primary_auto_disposition_issue,'[^a-zA-Z0-9\\s]',''),'  ',' '))
,trim(primary_auto_disposition_cause)
,trim(regexp_replace(regexp_replace(primary_auto_disposition_cause,'[^a-zA-Z0-9\\s]',''),'  ',' '))
,cast(NULL as string)
,cast(NULL as string)
from ${env:ENVIRONMENT}.atom_cs_call_care_data_3 cd
where cd.call_end_date_utc between '${hiveconf:prior_month_start_date}' and '${hiveconf:prior_month_end_date}'
;

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${env:DASP_db}.cs_dispositions
partition (partition_month = '${hiveconf:prior_month_start_date}')
select * FROM cs_dispositions_${hiveconf:CLUSTER}
;

SELECT * FROM ${env:DASP_db}.cs_dispositions
WHERE partition_month='${hiveconf:prior_month_start_date}'
LIMIT 10
;
