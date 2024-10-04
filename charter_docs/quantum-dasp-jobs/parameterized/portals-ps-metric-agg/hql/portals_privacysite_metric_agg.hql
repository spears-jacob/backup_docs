USE ${env:DASP_db};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = false;
set orc.force.positional.evolution=true;

DROP TABLE IF EXISTS ${env:TMP_db}.ps_metric_agg_${env:CLUSTER} PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.ps_metric_agg_${env:CLUSTER} AS

select  state__view__current_page__app_section as app_section,
        visit__user__role as user_role,
        message__context as message_context,
        visit__device__enc_uuid as device_id,
        visit__visit_id as visit_id,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPCA-0000' , 1, 0)) AS CPCA_0000_captcha_validation_unspecified_captcha_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPCA-1000' , 1, 0)) AS CPCA_1000_captcha_validation_failed_captcha_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPCX-0000' , 1, 0)) AS CPCX_0000_citrix_unspecified_citrix_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPCX-1000' , 1, 0)) AS CPCX_1000_citrix_invalid_file_type_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPCX-1001' , 1, 0)) AS CPCX_1001_citrix_upload_failed_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPCX-1002' , 1, 0)) AS CPCX_1002_citrix_file_delete_failed_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPCX-1003' , 1, 0)) AS CPCX_1003_citrix_file_size_too_large_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPCX-1004' , 1, 0)) AS CPCX_1004_citrix_file_update_failed_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPGE-1000' , 1, 0)) AS CPGE_1000_privacy_api_generic_api_failure_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPGE-1001' , 1, 0)) AS CPGE_1001_generic_timeout_attempt_post_requisition_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPGE-1002' , 1, 0)) AS CPGE_1002_generic_timeout_attempt_put_requisition_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPGE-1003' , 1, 0)) AS CPGE_1003_generic_timeout_attempt_post_document_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPGE-1004' , 1, 0)) AS CPGE_1004_4xx5xx_server_error_attempt_post_requisition_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND  application__error__error_code = 'CPGE-1005' , 1, 0)) AS CPGE_1005_4xx5xx_server_error_attempt_put_requisition_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPGE-1006' , 1, 0)) AS CPGE_1006_4xx5xx_server_error_attempt_post_document_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPGE-1007' , 1, 0)) AS CPGE_1007_malformed_response_from_server_with_200_status_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPIT-0000' , 1, 0)) AS CPIT_0000_it_privacy_api_issue_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPIT-1000' , 1, 0)) AS CPIT_1000_it_request_ticket_submission_user_failed_verification_2x12_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPIT-1001' , 1, 0)) AS CPIT_1001_it_request_ticket_submission_server_5xx_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPIT-1002' , 1, 0)) AS CPIT_1002_it_request_ticket_request_guid_not_received_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPIT-1003' , 1, 0)) AS CPIT_1003_it_request_ticket_unexpected_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPIT-1004' , 1, 0)) AS CPIT_1004_it_request_ticket_duplicate_requests_by_same_user_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPKB-0000' , 1, 0)) AS CPKB_0000_general_error_in_orchestrator_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPKB-1000' , 1, 0)) AS CPKB_1000_kba_user_failed_verification_generic_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPKB-1001' , 1, 0)) AS CPKB_1001_kba_user_failed_verification_timeout_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPKB-1002' , 1, 0)) AS CPKB_1002_kba_user_request_cannot_be_processed_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPKB-1003' , 1, 0)) AS CPKB_1003_kba_user_request_cannot_be_processed_cache_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPLN-0000' , 1, 0)) AS CPLN_0000_lexisnexis_general_transaction_status_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPLN-1000' , 1, 0)) AS CPLN_1000_lexisnexis_server_error_5xx_before_kba_verification_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPLN-1001' , 1, 0)) AS CPLN_1001_lexisnexis_server_error_5xx_during_kba_verification_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPLN-1002' , 1, 0)) AS CPLN_1002_lexisnexis_server_not_found_before_kba_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPLN-1003' , 1, 0)) AS CPLN_1003_lexisnexis_server_not_found_during_kba_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPLN-1004' , 1, 0)) AS CPLN_1004_lexisnexis_user_not_found_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPLN-1005' , 1, 0)) AS CPLN_1005_lexisnexis_unexpected_data_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPLN-1006' , 1, 0)) AS CPLN_1006_lexisnexis_user_blocked_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'error'  AND application__error__error_code = 'CPLN-1007' , 1, 0)) AS CPLN_1007_lexisnexis_24hr_cooldown_error_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'selectAction'  AND state__view__current_page__elements__standardized_name = 'agreeAndContinue'  AND state__view__current_page__page_name = 'ccpaChoiceForm' , 1, 0)) AS ccpa_agree_and_continue_clicks,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaChoiceForm' , 1, 0)) AS ccpa_choice_form_direct_pageviews,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'selectAction'  AND state__view__current_page__elements__standardized_name = 'submit' , 1, 0)) AS ccpa_doc_submit_clicks,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'selectAction'  AND state__view__current_page__elements__standardized_name = 'upload' , 1, 0)) AS ccpa_doc_upload_clicks,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaDocumentUpload' , 1, 0)) AS ccpa_doc_upload_pageviews,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaSuccess' , 1, 0)) AS ccpa_flow_complete_successes_pageviews,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaKbaVerificationQuestion' , 1, 0)) AS ccpa_kba_identity_question_pageviews,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'selectAction'  AND state__view__current_page__elements__standardized_name = 'continue'  AND message__event_case_id = 'privacyMicrosite_selectAction_kbaIntro_continue' , 1, 0)) AS ccpa_kba_speedbump_warning_continue_clicks,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaKbaIntro' , 1, 0)) AS ccpa_kba_speedbump_warning_pageviews,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND ((message__name = 'pageView' AND state__view__current_page__page_name = 'ccpaSuccess') OR (message__name = 'error' AND application__error__error_code = 'CPIT-1000')) , 1, 0)) AS ccpa_kba_successfully_authenticated_counts,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'selectAction'  AND state__view__current_page__elements__standardized_name = 'continue'  AND state__view__current_page__page_name = 'ccpaRepIdForm' , 1, 0)) AS ccpa_representative_id_continue_clicks,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaResIdForm' , 1, 0)) AS ccpa_representative_id_form_pageviews,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'selectAction'  AND state__view__current_page__elements__standardized_name = 'continue' , 1, 0)) AS ccpa_resident_id_continue_clicks,
        SUM(IF(visit__application_details__application_name ='PrivacyMicrosite' AND message__name = 'pageView'  AND state__view__current_page__page_name = 'ccpaResIdForm' , 1, 0)) AS ccpa_resident_id_form_pageviews,

'test' AS placeholder_for_additional_strings,
partition_date_utc
FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
WHERE partition_date_utc = '${hiveconf:START_DATE}'
  and visit__application_details__application_name ='PrivacyMicrosite'
  and message__name IN('pageView', 'selectAction', 'error')
group by partition_date_utc,
         state__view__current_page__app_section,
         visit__user__role,
         message__context,
         visit__device__enc_uuid,
         visit__visit_id
;

INSERT OVERWRITE TABLE asp_privacysite_metric_agg PARTITION (partition_date_utc)
select * from ${env:TMP_db}.ps_metric_agg_${env:CLUSTER};

DROP TABLE IF EXISTS ${env:TMP_db}.ps_metric_agg_${env:CLUSTER} PURGE;
