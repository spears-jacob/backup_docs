USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=512000000;
set mapreduce.input.fileinputformat.split.minsize=512000000;
set hive.optimize.sort.dynamic.partition = false;
set hive.vectorized.execution.enabled = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;

set EIDS='SPECTRUM_pageAlertView','SPECTRUM_selectAction_pageAlert','SPECTRUM_selectAction_requestCallback_pageAlert','SPECTRUM_selectAction_callbackRequest_confirm','SPECTRUM_error_callbackRequest','SPECTRUM_pageView_outageCallback','SPECTRUM_modalView_callbackRequest','SPECTRUM_selectAction_requestCallback_tile','SPECTRUM_selectAction_callbackRequest_cancel', 'mySpectrum_selectAction_outageReportedBanner','mySpectrum_applicationActivity_pageAlertView';

SELECT "\n\nFor: outage_alerts \n\n";

with calls as --these calls CTEs allows for aligning to the recommended CIR calculation for matching a prior visit to a call
(
  select distinct
         visit_type
        ,visit_id
        ,call_inbound_key
        ,minutes_to_call
   from ${env:DASP_db}.cs_calls_with_prior_visits c
  inner join ${env:TMP_db}.asp_adhoc_events_${env:CLUSTER} e
     on c.visit_id = e.visit__visit_id
  where call_date between '${hiveconf:START_DATE}' and DATE_ADD('${hiveconf:END_DATE}',1)
    and visit_type in ('specnet','smb', 'myspectrum')
    AND e.message__event_case_id in (${hiveconf:EIDS})
)
,calls2 as
(
  select
         visit_id
        ,call_inbound_key
        ,minutes_to_call
        ,ROW_NUMBER()
         OVER(PARTITION BY visit_type, call_inbound_key ORDER BY minutes_to_call) as call_number
   from calls
)
insert overwrite table ${env:DASP_db}.asp_adhoc_outage_alerts partition (partition_date_utc)
select
       e.visit__visit_id
      ,visit__account__enc_account_number
      ,visit__account__details__mso
      ,visit__application_details__application_name
      ,visit__application_details__app_version
      ,message__sequence_number
      ,message__event_case_id
      ,received__timestamp
      ,operation__additional_information
      ,state__view__current_page__elements__element_string_value
      ,application__error__error_type
      ,application__api__response_code
      ,case when calls2.visit_id is not null and calls2.call_number = 1 then 1 else 0 end as CUSTOM_visit_had_call
      ,case when calls2.visit_id is not null then 1 else 0 end as CUSTOM_visit_had_call_inclusive
      ,partition_date_utc
 from ${env:TMP_db}.asp_adhoc_events_${env:CLUSTER} e
 left join calls2
   on e.visit__visit_id = calls2.visit_id
WHERE e.message__event_case_id in (${hiveconf:EIDS})
;
