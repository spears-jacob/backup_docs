
USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
--CREATE TEMPORARY FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256';
--CREATE TEMPORARY FUNCTION aes_encrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesEncrypt256';

--------------------------------------------------------------------------------
------------ ***** Create temporary table for job data set ***** ---------------
--------------------------------------------------------------------------------

/* This CREATE table was accomplished in repo ddl-manager-dasp via XGANALYTIC-37075
CREATE EXTERNAL TABLE ${env:DASP_db}.asp_onlineform_errors_dashboard
(
visit__visit_id                                                      string
,visit__application_details__application_name                        string
,visit__login__logged_in                                             boolean
,PAGEVIEW__sequence_number                                           int
,PAGEVIEW__count_binary                                              int
,state__view__current_page__page_sequence_number                     int
,state__view__current_page__dec_page_title                           string
,SUBMIT__sequence_number                                             int
,SUBMIT_state__view__current_page__elements__element_string_value    string
,SUBMIT__count_binary                                                int
,ERROR__sequence_number                                              int
,application__error__enc_error_extras                                map<string,string>
,field                                                               string
,message                                                             string
,CUSTOM_visit_had_call                                               int
,issue_description                                                   string
,cause_description                                                   string
,call_date                                                           string
)
PARTITIONED BY
(
partition_date_utc                                                  string
)*/

--------------------------------------------------------------------------------
--------- ***** Insert and aggregate into Tableau feeder table ***** -----------
--------------------------------------------------------------------------------


WITH calls AS
(
  select distinct
         visit_type
        ,visit_id
        ,call_inbound_key
        ,minutes_to_call
        ,issue_description
        ,cause_description
        ,call_date
   from ${env:DASP_db}.cs_calls_with_prior_visits c
  inner join `${env:CQES}` e --prod.core_quantum_events_sspp e
     on c.visit_id = e.visit__visit_id
  where call_date between '${env:START_DATE}' and DATE_ADD('${env:END_DATE}',1) -- did you submit or have a form error and then call in within 24 hours
    and visit_type in ('specnet','smb')
    AND e.visit__application_details__application_name IN ('SpecNet', 'SMB')
    AND e.partition_date_utc between '${env:START_DATE}' and '${env:END_DATE}'
    AND e.message__event_case_id in ('SPECTRUM_selectAction_onlineForm_submit','SPECTRUM_error_support_inlineError')
    AND e.visit__account__enc_account_number IS NOT NULL
),
calls2 AS
(
  select
         visit_id
        ,call_inbound_key
        ,minutes_to_call
        ,ROW_NUMBER() OVER(PARTITION BY visit_type, call_inbound_key ORDER BY minutes_to_call) as call_number
        ,issue_description
        ,cause_description
        ,call_date
   from calls
),
s_e AS
(
  select
  visit__visit_id
  ,state__view__current_page__page_sequence_number
  ,message__event_case_id
  ,message__sequence_number
  ,state__view__current_page__elements__element_string_value
  ,LEAD(application__error__enc_error_extras)
    OVER(PARTITION BY visit__visit_id
    ORDER BY message__sequence_number) as LEAD_application__error__enc_error_extras
  ,LEAD(message__sequence_number)
    OVER(PARTITION BY visit__visit_id
    ORDER BY message__sequence_number) as LEAD_message__sequence_number
  from `${env:CQES}` --${env:DASP_db}.core_quantum_events_sspp
  where ((partition_date_utc between '${env:START_DATE}' and '${env:END_DATE}')
  and visit__application_details__application_name in ('SpecNet', 'SMB'))
  and message__event_case_id in ('SPECTRUM_selectAction_onlineForm_submit','SPECTRUM_error_support_inlineError')
),
exp AS
(
  select
  visit__visit_id
  ,message__sequence_number
  ,application__error__enc_error_extras
  ,field
  ,message
  from `${env:CQES}` --${env:DASP_db}.core_quantum_events_sspp
  LATERAL VIEW EXPLODE (application__error__enc_error_extras) b AS field, message
  where ((partition_date_utc between '${env:START_DATE}' and '${env:END_DATE}')
  and visit__application_details__application_name in ('SpecNet', 'SMB'))
  and message__event_case_id = 'SPECTRUM_error_support_inlineError'
)
INSERT OVERWRITE TABLE ${env:DASP_db}.asp_onlineform_errors_dashboard PARTITION (partition_date_utc)
SELECT
pv.visit__visit_id
,pv.visit__application_details__application_name
,pv.visit__login__logged_in
,pv.message__sequence_number as pageView__sequence_number
,CASE
WHEN ROW_NUMBER()
    OVER(PARTITION BY pv.visit__visit_id, pv.message__sequence_number
          ORDER BY NVL(s_e.message__sequence_number, pv.message__sequence_number)) = 1 THEN 1 ELSE 0 END as PAGEVIEW__count_binary
,pv.state__view__current_page__page_sequence_number
,pv.state__view__current_page__dec_page_title
,s_e.message__sequence_number as SUBMIT__sequence_number
,s_e.state__view__current_page__elements__element_string_value as SUBMIT_state__view__current_page__elements__element_string_value
,CASE
WHEN s_e.message__sequence_number IS NULL THEN NULL
WHEN ROW_NUMBER()
    OVER(PARTITION BY pv.visit__visit_id, s_e.message__sequence_number
          ORDER BY s_e.message__sequence_number) = 1 THEN 1 ELSE 0 END as SUBMIT__count_binary
,case
  when s_e.state__view__current_page__elements__element_string_value = 'Online Form Submit: false'
    then s_e.LEAD_message__sequence_number
    else NULL end as ERROR__sequence_number
,case
  when s_e.state__view__current_page__elements__element_string_value = 'Online Form Submit: false'
    then s_e.LEAD_application__error__enc_error_extras
    else NULL end as application__error__enc_error_extras
,exp.field
,exp.message
,case when calls2.visit_id is not null and calls2.call_number = 1 then 1 else 0 end as CUSTOM_visit_had_call
,issue_description
,cause_description
,call_date
,pv.partition_date_utc
FROM `${env:CQES}` pv --prod.core_quantum_events_sspp 
left join calls2 on pv.visit__visit_id = calls2.visit_id
left join s_e on pv.visit__visit_id = s_e.visit__visit_id
  and pv.state__view__current_page__page_sequence_number = s_e.state__view__current_page__page_sequence_number
  and s_e.message__event_case_id = 'SPECTRUM_selectAction_onlineForm_submit'
left join exp on s_e.visit__visit_id = exp.visit__visit_id
  and s_e.LEAD_message__sequence_number = exp.message__sequence_number
where ((pv.partition_date_utc between '${env:START_DATE}' and '${env:END_DATE}')
and pv.visit__application_details__application_name in ('SpecNet', 'SMB'))
and pv.message__event_case_id = 'SPECTRUM_pageView_onlineForm';
--------------------------------------------------------------------------------
----------------------- ***** Dropping temp tables ***** -----------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.ABC_XYZ_${env:CLUSTER} PURGE;