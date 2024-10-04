USE ${env:DASP_db};

SET hive.auto.convert.join=false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=2048000000;
SET hive.merge.smallfiles.avgsize=2048000000;
SET hive.merge.tezfiles=true;
SET hive.stats.fetch.column.stats=false;
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.vectorized.execution.enabled=false;
SET mapreduce.input.fileinputformat.split.maxsize=536870912;
SET mapreduce.input.fileinputformat.split.minsize=536870912;
SET orc.force.positional.evolution=true;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

SELECT '***** getting voice of customer - cable data ******'
;

INSERT OVERWRITE TABLE asp_extract_voice_of_customer_cable PARTITION (denver_date)
SELECT --replace with the required list
       *
  FROM
(Select
      partition_date_utc,
      visit__account__enc_account_number as acct_number_enc,
      visit__account__enc_mobile_account_number as mobile_acct_enc,
      visit__visit_id as visit_id,
      visit__application_details__application_name as app,
      state__view__current_page__user_journey as journey,
      state__view__current_page__user_sub_journey as sub_journey,
      state__view__current_page__user_full_journey as full_journey,
      state__view__current_page__components__component_name as component_name,
      state__view__current_page__components__standardized_name as component_std_name,
      state__view__current_page__components__ui_name as component_ui_name,
      received__timestamp as transaction_timestamp,
      visit__account__enc_account_billing_division_id as division_id,
      visit__account__enc_system_sys as site_sys,
      visit__account__enc_system_prin as prn,
      visit__account__enc_system_agent as agn,
      visit__account__enc_account_billing_division as division,
      state__view__current_page__biller_type as biller_type,
      visit__account__enc_account_site_id as acct_site_id,
      visit__account__enc_account_company as acct_company,
      visit__account__enc_account_franchise as acct_franchise,
      visit__account__details__mso as mso,
      message__event_case_id as case_id,
      state__view__current_page__elements__element_string_value as page_element,
      message__name,
      operation__success,
      case
          when visit__application_details__application_name='SpecNet'
           AND message__event_case_id = 'SPECNET_selectAction_askSpectrum'
          then 'SpecNet_IVA_Chat'  --yes
          when visit__application_details__application_name='MySpectrum'
           AND message__event_case_id = 'mySpectrum_selectAction_support_launchChat'
          then 'MySpectrum_IVA_Chat'
          when visit__application_details__application_name='SpecNet'
           AND message__name = 'loginStop' AND operation__success = TRUE
          then 'SpecNet_Login' --yes
          when visit__application_details__application_name='SMB'
           AND message__name = 'loginStop' AND operation__success = TRUE
          then 'SMB_Login' --yes
          when visit__application_details__application_name='SelfInstall'
           AND message__name = 'loginStop' AND operation__success = TRUE
          then 'SelfInstall_Login' --yes
          when visit__application_details__application_name='MySpectrum'
           AND message__name = 'loginStop' AND operation__success = TRUE
          then 'MySpectrum_Login' --yes
          when visit__application_details__application_name='MySpectrum'
           AND message__name = 'pushNotification'
          then 'MySpectrum_Outage_Push_Notifications' --yes
          when visit__application_details__application_name='SpecNet'
           AND message__event_case_id = 'SPECTRUM_pageAlertView'
           AND state__view__current_page__elements__element_string_value = 'ETR'
          then 'SpecNet_Outage_Banner' --yes
    end AS event_type,
    epoch_converter(received__timestamp,'America/Denver') as denver_date
  from `${env:CQE}`
  where (partition_date_hour_utc >= '${env:START_DATE_TZ}'
     and partition_date_hour_utc <  '${env:END_DATE_TZ}')
    and visit__account__enc_account_number is not null
    and visit__account__enc_account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') -- exclude blank or 'pending login'
    AND visit__application_details__application_name in ('SpecNet','SMB','MySpectrum','SelfInstall')
    AND epoch_converter(received__timestamp,'America/Denver') is not null
  ) a
  where event_type is not null
;
