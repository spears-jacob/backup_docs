USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=512000000;
set mapreduce.input.fileinputformat.split.minsize=512000000;
set hive.optimize.sort.dynamic.partition = false;
set hive.vectorized.execution.enabled = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;

MSCK REPAIR TABLE cs_calls_with_prior_visits;

SELECT "\n\n Creating CQE TEMPORARY Table \n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_adhoc_events_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_adhoc_events_${env:CLUSTER}
LOCATION 'hdfs:///tmp/tmp_adhoc_weekly_job/asp_adhoc_events' AS
select
      cqe.visit__visit_id
      ,visit__account__enc_account_number
      ,visit__account__details__mso
      ,visit__application_details__application_name
      ,visit__application_details__app_version
      ,visit__account__details__statement_method
      ,visit__login__login_completed_timestamp
      ,visit__visit_start_timestamp
      ,message__sequence_number
      ,received__timestamp
      ,state__view__current_page__elements__element_string_value
      ,state__view__current_page__elements__selected_options
      ,visit__account__details__service_subscriptions
      ,application__error__client_error_code
      ,state__view__current_page__page_name
      ,state__view__previous_page__page_name
      ,state__view__previous_page__app_section
      ,state__view__current_page__elements__standardized_name
      ,translate(operation__additional_information, chr(10)||chr(11)||chr(13), ' ') as operation__additional_information
      ,application__error__error_type
      ,application__api__response_code
      ,message__event_case_id
      ,partition_date_utc
 from ${env:ENVIRONMENT}.core_quantum_events_sspp cqe
where ((partition_date_utc between '${hiveconf:START_DATE}' and '${hiveconf:END_DATE}')
  and visit__application_details__application_name in ('SpecNet','SMB', 'MySpectrum'))
;
