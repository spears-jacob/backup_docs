USE ${env:DASP_db};

set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.exec.max.dynamic.partitions=8000;
SET mapreduce.reduce.memory.mb=50000;
SET mapreduce.reduce.java.opts=-Xmx5000m;

--------------------------------------------------------------------------------
----------------------- ***** Declined Invitations ***** -----------------------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_medallia_interceptsurvey PARTITION (partition_date_utc)
SELECT Distinct
visit__application_details__application_name as application_name,
visit__account__enc_account_number as acct_number,
CASE
  WHEN  message__event_case_id IN ('SPECTRUM_selectAction_medallia_interceptSurvey_formSubmit')
  THEN 'formSubmit'
  ELSE 'inviteDeclined'
  END AS survey_action,
state__view__current_page__biller_type as biller_type,
visit__account__enc_account_billing_division AS division,
visit__account__enc_account_billing_division_id AS division_id,
visit__account__enc_system_sys as sys,
visit__account__enc_system_prin as prin,
visit__account__enc_system_agent as agent,
visit__account__enc_account_site_id as acct_site_id,
visit__account__enc_account_company as acct_company,
visit__account__enc_account_franchise as acct_franchise,
NULL AS day_diff,
CAST('${env:RUN_DATE}' AS STRING) rpt_dt,
partition_date_utc
from `${env:PCQE}`
WHERE (partition_date_utc >= '${env:START_DATE}' and partition_date_utc < '${env:END_DATE}')
  AND visit__application_details__application_name IN ('SpecNet','SMB','SpectrumCommunitySolutions')
  and message__event_case_id in ('SPECTRUM_selectAction_medallia_interceptSurvey_formSubmit','SPECTRUM_selectAction_medallia_interceptSurvey_inviteDeclined')
  and visit__account__enc_account_number is not null
;

--------------------------------------------------------------------------------
------------------------------- ***** END ***** --------------------------------
--------------------------------------------------------------------------------
