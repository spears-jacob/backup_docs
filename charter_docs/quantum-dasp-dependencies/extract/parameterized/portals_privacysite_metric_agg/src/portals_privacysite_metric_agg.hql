USE ${env:ENVIRONMENT};

set hive.vectorized.execution.enabled = false;

DROP TABLE IF EXISTS prod_tmp.ps_metric_agg PURGE;
CREATE TEMPORARY TABLE prod_tmp.ps_metric_agg AS

-- Dimensions go here
select  state__view__current_page__app_section as app_section,
        visit__user__role as user_role,
        visit__device__enc_uuid as device_id,
        visit__visit_id as visit_id,

-------------------------------------------------
--- Begin Rosetta-Generated Metric Defintions ---
------------ Privacy Site Metric Agg ------------
-------------------------------------------------
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPGE-1000' , 1, 0)) AS CPGE_1000_privacy_api_generic_api_failure_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPIT-0000' , 1, 0)) AS CPIT_0000_it_privacy_api_issue_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPIT-1000' , 1, 0)) AS CPIT_1000_it_request_ticket_submission_user_failed_verification_2x12_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPIT-1001' , 1, 0)) AS CPIT_1001_it_request_ticket_submission_server_5xx_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPKB-0000' , 1, 0)) AS CPKB_0000_general_error_in_orchestrator_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPKB-1000' , 1, 0)) AS CPKB_1000_kba_user_failed_verification_generic_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPKB-1001' , 1, 0)) AS CPKB_1001_kba_user_failed_verification_timeout_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPKB-1002' , 1, 0)) AS CPKB_1002_kba_user_request_cannot_be_processed_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPKB-1003' , 1, 0)) AS CPKB_1003_kba_user_request_cannot_be_processed_cache_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPLN-0000' , 1, 0)) AS CPLN_0000_lexisnexis_general_transaction_status_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPLN-1000' , 1, 0)) AS CPLN_1000_lexisnexis_server_error_5xx_before_kba_verification_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPLN-1001' , 1, 0)) AS CPLN_1001_lexisnexis_server_error_5xx_during_kba_verification_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPLN-1002' , 1, 0)) AS CPLN_1002_lexisnexis_server_not_found_before_kba_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPLN-1003' , 1, 0)) AS CPLN_1003_lexisnexis_server_not_found_during_kba_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPLN-1004' , 1, 0)) AS CPLN_1004_lexisnexis_user_not_found_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPLN-1005' , 1, 0)) AS CPLN_1005_lexisnexis_unexpected_data_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPLN-1006' , 1, 0)) AS CPLN_1006_lexisnexis_user_blocked_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_type = 'ccpaVerification'  AND application__error__error_code = 'CPLN-1007' , 1, 0)) AS CPLN_1007_lexisnexis_24hr_cooldown_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'selectAction'  AND state__view__current_page__elements__standardized_name = 'agreeAndContinue'  AND state__view__current_page__page_name = 'ccpaChoiceForm' , 1, 0)) AS ccpa_agree_and_continue_clicks,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaChoiceForm' , 1, 0)) AS ccpa_choice_form_direct_pageviews,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaSuccess' , 1, 0)) AS ccpa_flow_complete_successes_pageviews,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaKbaVerificationQuestion' , 1, 0)) AS ccpa_kba_identity_question_pageviews,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'selectAction'  AND state__view__current_page__elements__standardized_name = 'continue'  AND message__event_case_id = 'privacyMicrosite_selectAction_kbaIntro_continue' , 1, 0)) AS ccpa_kba_speedbump_warning_continue_clicks,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaKbaIntro' , 1, 0)) AS ccpa_kba_speedbump_warning_pageviews,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND ((message__name = 'pageView' AND state__view__current_page__page_name = 'ccpaSuccess') OR (message__name = 'error' AND application__error__error_type = 'ccpaVerification' AND application__error__error_code = 'CPIT-1000')) , 1, 0)) AS ccpa_kba_successfully_authenticated_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'selectAction'  AND state__view__current_page__elements__standardized_name = 'continue'  AND state__view__current_page__page_name = 'ccpaResIdForm' , 1, 0)) AS ccpa_resident_id_continue_clicks,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaResIdForm' , 1, 0)) AS ccpa_resident_id_form_pageviews,
-------------------------------------------------
---- End Rosetta-Generated Metric Defintions ----
------------ Privacy Site Metric Agg ------------
-------------------------------------------------
'אליהו' AS placeholder_for_additional_strings,
prod.epoch_converter(received__timestamp, 'America/Denver') as denver_date
FROM core_quantum_events_portals_v
WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
  AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
  and visit__application_details__application_name ='PrivacyMicrosite'
  and message__name IN('pageView', 'selectAction', 'error')
group by prod.epoch_converter(received__timestamp, 'America/Denver'),
         state__view__current_page__app_section,
         visit__user__role,
         visit__device__enc_uuid,
         visit__visit_id
;

INSERT OVERWRITE TABLE asp_privacysite_metric_agg PARTITION (denver_date)
select * from prod_tmp.ps_metric_agg;
