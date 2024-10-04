use ${env:LKP_db};

--creates table of distinct combinations of issue, cause, and resolution descriptions. Adds in columns for each that have been purged of funky characters.
--could be done in three separate tables to avoid duplication of rows.

create table if not exists cs_dispositions
(
issue_description         string  comment "Raw issue_description taken straight from cs_call_care_data"
,id_form                  string  comment "Issue description with non-letters and non-numbers removed"
,cause_description        string  comment "Raw cause_description taken straight from cs_call_care_data"
,cd_form                  string  comment "Cause description with non-letters and non-numbers removed"
,resolution_description   string  comment "Raw resolution_description taken straight from cs_call_care_data"
,rd_form                  string  comment "Resolution description with non-letters and non-numbers removed"
)
partitioned by
(
partition_month           date    comment "First day of month for call_end_date_utc"
);

insert into table cs_dispositions
partition (partition_month)
select
distinct
issue_description
,regexp_replace(regexp_replace(issue_description,'[^a-zA-Z0-9\\s]',''),'  ',' ')
,cause_description
,regexp_replace(regexp_replace(cause_description,'[^a-zA-Z0-9\\s]',''),'  ',' ')
,resolution_description
,regexp_replace(regexp_replace(resolution_description,'[^a-zA-Z0-9\\s]',''),'  ',' ')
,date_format(call_end_date_utc,'yyyy-MM-01')
from prod.red_cs_call_care_data_v
where call_end_date_utc >= '2019-01-01'
and call_end_date_utc <= '2020-01-31';
