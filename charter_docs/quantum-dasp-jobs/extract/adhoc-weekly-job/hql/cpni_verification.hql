USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=512000000;
set mapreduce.input.fileinputformat.split.minsize=512000000;
set hive.optimize.sort.dynamic.partition = false;
set hive.vectorized.execution.enabled = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;

set EIDS='SPECTRUM_selectAction_verifyAccountBack','SPECTRUM_selectAction_verifyAccountSubmit','SPECTRUM_assetDisplayed_verifyAccountCodeSent_success','SPECTRUM_assetDisplayed_verifyAccountCodeSent_failure','SPECTRUM_error_cpni_pageLevelError','SPECTRUM_selectAction_verifyAccountCont','SPECTRUM_appActivity_verifyAccountSuccess','SPECTRUM_appActivity_verifyAccountFailure','SPECTRUM_modalView_verifyAccount','SPECTRUM_pageView_verifyAccount','SPECTRUM_selectAction_verifyAccountCancel','SPECTRUM_selectAction_verifyAccountMoreInfo','SPECTRUM_selectAction_verifyAccountResendCode';

SELECT "\n\nFor: CPNI_VERIFICATION \n\n";

with calls as
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
    and visit_type in ('specnet','smb')
    AND e.message__event_case_id in (${hiveconf:EIDS})
)
,calls2 as
(
  select
         visit_id
        ,call_inbound_key
        ,minutes_to_call
        ,ROW_NUMBER() OVER(PARTITION BY visit_type, call_inbound_key ORDER BY minutes_to_call) as call_number
   from calls
)
,sub as
(
  select distinct
         visit__visit_id
        ,status
   from ${env:TMP_db}.asp_adhoc_events_${env:CLUSTER}
        lateral view explode(visit__account__details__service_subscriptions) voice as service, status
  where message__event_case_id in (${hiveconf:EIDS})
    and service = 'voice'
    and status is not null
)
,sub2 as
(
  select visit__visit_id, count(1) as num_records
    from sub
   group by visit__visit_id having count(1) > 1
)
,lag as
(
  select distinct
         visit__visit_id
        ,message__event_case_id
        ,message__sequence_number
        ,state__view__current_page__elements__selected_options
        ,LAG(state__view__current_page__elements__selected_options) OVER(PARTITION BY visit__visit_id ORDER BY message__sequence_number) as lag
  from ${env:TMP_db}.asp_adhoc_events_${env:CLUSTER}
 WHERE message__event_case_id in (${hiveconf:EIDS})
   and message__event_case_id in ('SPECTRUM_selectAction_verifyAccountCont','SPECTRUM_appActivity_verifyAccountSuccess','SPECTRUM_appActivity_verifyAccountFailure')
)
,attempt as
(
  select
         visit__visit_id
        ,message__sequence_number
        ,ROW_NUMBER()
          OVER(PARTITION BY visit__visit_id ORDER BY message__sequence_number) as attempt_number
   from ${env:TMP_db}.asp_adhoc_events_${env:CLUSTER}
  WHERE message__event_case_id in (${hiveconf:EIDS})
    and message__event_case_id in ('SPECTRUM_appActivity_verifyAccountSuccess','SPECTRUM_appActivity_verifyAccountFailure')
)
insert overwrite table ${env:DASP_db}.asp_adhoc_cpni_verification partition (partition_date_utc)
select distinct
       e.visit__visit_id
      ,e.visit__account__enc_account_number
      ,e.visit__account__details__mso
      ,e.visit__application_details__application_name
      ,e.visit__application_details__app_version
      ,e.visit__account__details__statement_method
      ,e.visit__login__login_completed_timestamp
      ,e.visit__visit_start_timestamp
      ,case when sub2.num_records is not null then 'Multi' else sub.status end as SUB_voice_status
      ,e.message__sequence_number
      ,e.message__event_case_id
      ,e.received__timestamp
      ,concat_ws(',', e.state__view__current_page__elements__selected_options) as state__view__current_page__elements__selected_options
      ,concat_ws(',', lag.lag) as LAG_state__view__current_page__elements__selected_options
      ,e.state__view__current_page__elements__element_string_value
      ,e.application__error__client_error_code
      ,e.state__view__current_page__page_name
      ,e.state__view__previous_page__page_name
      ,e.state__view__previous_page__app_section
      ,case when calls2.visit_id is not null and calls2.call_number = 1 then 1 else 0 end as CUSTOM_visit_had_call
      ,case when calls2.visit_id is not null then 1 else 0 end as CUSTOM_visit_had_call_inclusive
      ,attempt.attempt_number as CUSTOM_attempt_number
      ,e.partition_date_utc
 from ${env:TMP_db}.asp_adhoc_events_${env:CLUSTER} e
 left join calls2 on e.visit__visit_id = calls2.visit_id
 left join sub on e.visit__visit_id = sub.visit__visit_id
 left join sub2 on e.visit__visit_id = sub2.visit__visit_id
 left join lag on e.visit__visit_id = lag.visit__visit_id
  and e.message__sequence_number = lag.message__sequence_number
  and e.message__event_case_id in ('SPECTRUM_appActivity_verifyAccountSuccess','SPECTRUM_appActivity_verifyAccountFailure')
 left join attempt on e.visit__visit_id = attempt.visit__visit_id
  and e.message__sequence_number = attempt.message__sequence_number
WHERE e.message__event_case_id in (${hiveconf:EIDS})
;
