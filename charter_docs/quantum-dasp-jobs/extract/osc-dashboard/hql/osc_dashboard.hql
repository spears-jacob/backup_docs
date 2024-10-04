
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

DROP TABLE IF EXISTS ${env:TMP_db}.cte_primary_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.cte_primary_${env:CLUSTER} AS
SELECT
state__view__current_page__render_details__fully_rendered_ms
,visit__login__login_duration_ms
,state__view__current_page__app_section
,state__view__current_page__page_name
,visit__application_details__application_type
,visit__application_details__application_name
,message__event_case_id
,message__name
,state__view__current_page__elements__element_string_value
,state__view__current_page__elements__ui_name
,visit__application_details__referrer_app__application_name
,state__view__current_page__elements__standardized_name
,partition_date_utc
FROM `${env:CQES}`
WHERE (partition_date_utc >= '${env:START_DATE}'
  AND partition_date_utc < '${env:END_DATE}')
AND visit__application_details__application_name IN('SpecNet', 'IDManagement', 'SMB')
;

--------------------------------------------------------------------------------
--------- ***** Insert and aggregate into Tableau feeder table ***** -----------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_osc_metrics PARTITION (partition_date_utc)
SELECT
ROUND(AVG(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND state__view__current_page__app_section = 'accountSummary'
      AND visit__application_details__application_type = 'Web'
      THEN state__view__current_page__render_details__fully_rendered_ms
  END),4) AS avg_accountSummaryLoadPage_ms
,ROUND(AVG(CASE WHEN visit__application_details__application_type = 'Web'
      AND message__event_case_id IN ('IdentityManagement_Manual_Login_Success','IdentityManagement_Manual_Login_Failure')
      AND visit__application_details__referrer_app__application_name = 'consumer_portal'
      THEN visit__login__login_duration_ms
  END),4) AS avg_loginDurationResiIDM_ms
,ROUND(AVG(CASE WHEN visit__application_details__application_name = 'IDManagement'
      AND message__name = 'loginStop'
      AND visit__application_details__application_type = 'Web'
      AND message__event_case_id IN ('IdentityManagement_Manual_Login_Success','IdentityManagement_Manual_Login_Failure')
      AND visit__application_details__referrer_app__application_name = 'commercial_portal'
      THEN visit__login__login_duration_ms
  END),4) AS avg_loginDurationSMBIDM_ms
,ROUND(AVG(CASE WHEN visit__application_details__application_name = 'SMB'
      AND state__view__current_page__app_section = 'accountSummary'
      AND visit__application_details__application_type = 'Web'
      THEN state__view__current_page__render_details__fully_rendered_ms
  END),4) AS avg_accountSummaryLoadPageSMB_ms
,ROUND(AVG(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND message__name = 'loginStop'
      AND visit__application_details__application_type = 'Web'
      AND message__event_case_id IN ('SPECTRUM_loginStop_centralizedAuth_success','SPECTRUM_loginStop_centralizedAuth_failure')
      THEN visit__login__login_duration_ms
  END),4) AS avg_loginDurationSpecNet_ms
,ROUND(AVG(CASE WHEN visit__application_details__application_name = 'SMB'
      AND message__name = 'loginStop'
      AND visit__application_details__application_type = 'Web'
      AND message__event_case_id IN ('SPECTRUM_loginStop_centralizedAuth_success','SPECTRUM_loginStop_centralizedAuth_failure')
      THEN visit__login__login_duration_ms
  END),4) AS avg_loginDurationSMB_ms
,ROUND(AVG(CASE WHEN visit__application_details__application_name = 'SMB'
      AND state__view__current_page__app_section = 'accountSummary'
      AND visit__application_details__application_type = 'Web'
      THEN state__view__current_page__render_details__fully_rendered_ms
  END),4) AS avg_accountSummaryUpgradeSMB_ms
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND state__view__current_page__page_name = 'campaignAutoPay'
      AND visit__application_details__application_type = 'Web'
      THEN state__view__current_page__page_name
  END) AS ASDAutoPayPageViews
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND message__event_case_id = 'SPECTRUM_selectAction_campaign_accountSummaryDashboard_icon'
      AND state__view__current_page__elements__ui_name IN ('ENROLL_AUTOPAY', 'UPDATE_AUTOPAY')
      AND visit__application_details__application_type = 'Web'
      THEN message__event_case_id
  END) AS ASDAutoPaySelectActions
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND state__view__current_page__page_name = 'campaignAutoPay'
      AND message__event_case_id = 'SPECTRUM_selectAction_campaign_accountSummaryDashboard_continue'
      AND visit__application_details__application_type = 'Web'
      AND state__view__current_page__elements__element_string_value IN ('Continue Paying Manually', 'Enroll in AutoPay')
      THEN message__event_case_id
  END) AS ASDAutoPayEnrollments
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND state__view__current_page__page_name = 'campaignPaperless'
      AND visit__application_details__application_type = 'Web'
      THEN state__view__current_page__page_name
  END) AS ASDPaperlessPageViews
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND message__event_case_id = 'SPECTRUM_selectAction_campaign_accountSummaryDashboard_icon'
      AND state__view__current_page__elements__ui_name IN ('UPDATE_PAPERLESS','ENROLL_PAPERLESS')
      AND visit__application_details__application_type = 'Web'
      THEN message__event_case_id
  END) AS ASDPaperlessSelectActions
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND state__view__current_page__page_name = 'campaignPaperless'
      AND message__event_case_id = 'SPECTRUM_selectAction_campaign_accountSummaryDashboard_optIn'
      AND visit__application_details__application_type = 'Web'
      THEN message__event_case_id
  END) AS ASDPaperlessEnrollments
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND state__view__current_page__page_name = 'campaignSecurity'
      AND visit__application_details__application_type = 'Web'
      THEN state__view__current_page__page_name
  END) AS ASDSecurityPageViews
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND state__view__current_page__page_name = 'campaignSecurity'
      AND message__event_case_id IN('SPECTRUM_selectAction_campaign_accountSummaryDashboard_save','SPECTRUM_selectAction_campaign_accountSummaryDashboard_saveAndContinue')
      AND visit__application_details__application_type = 'Web'
      THEN message__event_case_id
  END) AS ASDSecurityEngagements
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND state__view__current_page__page_name = 'campaignContact'
      AND visit__application_details__application_type = 'Web'
      THEN state__view__current_page__page_name
  END) AS ASDContactPageViews
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND state__view__current_page__page_name = 'campaignContact'
      AND message__event_case_id IN('SPECTRUM_selectAction_campaign_accountSummaryDashboard_save','SPECTRUM_selectAction_campaign_accountSummaryDashboard_saveAndContinue')
      AND visit__application_details__application_type = 'Web'
      THEN message__event_case_id
  END) AS ASDContactEngagements
,COUNT(CASE WHEN visit__application_details__application_name = 'IDManagement'
      AND visit__application_details__referrer_app__application_name = 'consumer_portal'
      AND message__event_case_id = 'IdentityManagement_Manual_Login_Success'
      THEN message__event_case_id
  END) AS ResiIDMLoginSuccess
,COUNT(CASE WHEN visit__application_details__application_name = 'IDManagement'
      AND visit__application_details__referrer_app__application_name = 'consumer_portal'
      AND message__event_case_id = 'IdentityManagement_Manual_Login_Start'
      THEN message__event_case_id
  END) AS ResiIDMLoginStart
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND visit__application_details__referrer_app__application_name = 'IDManagement'
      AND message__event_case_id = 'SPECTRUM_loginStop_centralizedAuth_success'
      THEN message__event_case_id
  END) AS ResiLoginSuccess
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND visit__application_details__referrer_app__application_name = 'IDManagement'
      AND message__event_case_id = 'SPECTRUM_loginStart_centralizedAuth'
      THEN message__event_case_id
  END) AS ResiLoginStart
,COUNT(CASE WHEN visit__application_details__application_name = 'IDManagement'
      AND visit__application_details__referrer_app__application_name = 'commercial_portal'
      AND message__event_case_id = 'IdentityManagement_Manual_Login_Success'
      THEN message__event_case_id
  END) AS SMBIDMLoginSuccess
,COUNT(CASE WHEN visit__application_details__application_name = 'IDManagement'
      AND visit__application_details__referrer_app__application_name = 'commercial_portal'
      AND message__event_case_id = 'IdentityManagement_Manual_Login_Start'
      THEN message__event_case_id
  END) AS SMBIDMLoginStart
,COUNT(CASE WHEN visit__application_details__application_name = 'SMB'
      AND visit__application_details__referrer_app__application_name = 'IDManagement'
      AND message__event_case_id = 'SPECTRUM_loginStop_centralizedAuth_success'
      THEN message__event_case_id
  END) AS SMBLoginSuccess
,COUNT(CASE WHEN visit__application_details__application_name = 'SMB'
      AND visit__application_details__referrer_app__application_name = 'IDManagement'
      AND message__event_case_id = 'SPECTRUM_loginStart_centralizedAuth'
      THEN message__event_case_id
  END) AS SMBLoginStart
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND message__event_case_id = 'SPECTRUM_pageView_settings_yourInfo'
      AND visit__application_details__application_type = 'Web'
      THEN message__event_case_id
  END) AS SettingsPageViews
,COUNT(CASE WHEN visit__application_details__application_name = 'SpecNet'
      AND message__event_case_id = 'SPECTRUM_selectAction_header_utilityNav'
      AND state__view__current_page__elements__standardized_name = 'utilityNav-settings'
      AND visit__application_details__application_type = 'Web'
      THEN message__event_case_id
  END) AS SettingsButtonClicks
,partition_date_utc
FROM ${env:TMP_db}.cte_primary_${env:CLUSTER}
GROUP BY partition_date_utc
;

--------------------------------------------------------------------------------
----------------------- ***** Dropping temp tables ***** -----------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.cte_primary_${env:CLUSTER} PURGE;
