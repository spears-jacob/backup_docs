USE ${env:DASP_db};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO ${env:TMP_db}.portals_privacysite_set_agg_stage_instances_${hiveconf:CLUSTER}
SELECT
 app_section,
 user_role,
 message_context,
 'instances' AS unit_type,
 metric_name,
 metric_value,
 partition_date_utc,
 '${hiveconf:grain}' AS grain
FROM
  (
  SELECT
    '${hiveconf:partition_date_utc}' AS partition_date_utc,
    app_section,
    user_role,
    message_context,
      MAP(
     'CPCA_0000_captcha_validation_unspecified_captcha_error_counts', SUM(CPCA_0000_captcha_validation_unspecified_captcha_error_counts),
     'CPCA_1000_captcha_validation_failed_captcha_error_counts', SUM(CPCA_1000_captcha_validation_failed_captcha_error_counts),
     'CPCX_0000_citrix_unspecified_citrix_error_counts', SUM(CPCX_0000_citrix_unspecified_citrix_error_counts),
     'CPCX_1000_citrix_invalid_file_type_error_counts', SUM(CPCX_1000_citrix_invalid_file_type_error_counts),
     'CPCX_1001_citrix_upload_failed_error_counts', SUM(CPCX_1001_citrix_upload_failed_error_counts),
     'CPCX_1002_citrix_file_delete_failed_error_counts', SUM(CPCX_1002_citrix_file_delete_failed_error_counts),
     'CPCX_1003_citrix_file_size_too_large_error_counts', SUM(CPCX_1003_citrix_file_size_too_large_error_counts),
     'CPCX_1004_citrix_file_update_failed_error_counts', SUM(CPCX_1004_citrix_file_update_failed_error_counts),
     'CPGE_1000_privacy_api_generic_api_failure_error_counts', SUM(CPGE_1000_privacy_api_generic_api_failure_error_counts),
     'CPGE_1001_generic_timeout_attempt_post_requisition_error_counts', SUM(CPGE_1001_generic_timeout_attempt_post_requisition_error_counts),
     'CPGE_1002_generic_timeout_attempt_put_requisition_error_counts', SUM(CPGE_1002_generic_timeout_attempt_put_requisition_error_counts),
     'CPGE_1003_generic_timeout_attempt_post_document_error_counts', SUM(CPGE_1003_generic_timeout_attempt_post_document_error_counts),
     'CPGE_1004_4xx5xx_server_error_attempt_post_requisition_error_counts', SUM(CPGE_1004_4xx5xx_server_error_attempt_post_requisition_error_counts),
     'CPGE_1005_4xx5xx_server_error_attempt_put_requisition_error_counts', SUM(CPGE_1005_4xx5xx_server_error_attempt_put_requisition_error_counts),
     'CPGE_1006_4xx5xx_server_error_attempt_post_document_error_counts', SUM(CPGE_1006_4xx5xx_server_error_attempt_post_document_error_counts),
     'CPGE_1007_malformed_response_from_server_with_200_status_error_counts', SUM(CPGE_1007_malformed_response_from_server_with_200_status_error_counts),
     'CPIT_0000_it_privacy_api_issue_error_counts', SUM(CPIT_0000_it_privacy_api_issue_error_counts),
     'CPIT_1000_it_request_ticket_submission_user_failed_verification_2x12_error_counts', SUM(CPIT_1000_it_request_ticket_submission_user_failed_verification_2x12_error_counts),
     'CPIT_1001_it_request_ticket_submission_server_5xx_error_counts', SUM(CPIT_1001_it_request_ticket_submission_server_5xx_error_counts),
     'CPIT_1002_it_request_ticket_request_guid_not_received_error_counts', SUM(CPIT_1002_it_request_ticket_request_guid_not_received_error_counts),
     'CPIT_1003_it_request_ticket_unexpected_error_counts', SUM(CPIT_1003_it_request_ticket_unexpected_error_counts),
     'CPIT_1004_it_request_ticket_duplicate_requests_by_same_user_error_counts', SUM(CPIT_1004_it_request_ticket_duplicate_requests_by_same_user_error_counts),
     'CPKB_0000_general_error_in_orchestrator_error_counts', SUM(CPKB_0000_general_error_in_orchestrator_error_counts),
     'CPKB_1000_kba_user_failed_verification_generic_error_counts', SUM(CPKB_1000_kba_user_failed_verification_generic_error_counts),
     'CPKB_1001_kba_user_failed_verification_timeout_error_counts', SUM(CPKB_1001_kba_user_failed_verification_timeout_error_counts),
     'CPKB_1002_kba_user_request_cannot_be_processed_error_counts', SUM(CPKB_1002_kba_user_request_cannot_be_processed_error_counts),
     'CPKB_1003_kba_user_request_cannot_be_processed_cache_error_counts', SUM(CPKB_1003_kba_user_request_cannot_be_processed_cache_error_counts),
     'CPLN_0000_lexisnexis_general_transaction_status_error_counts', SUM(CPLN_0000_lexisnexis_general_transaction_status_error_counts),
     'CPLN_1000_lexisnexis_server_error_5xx_before_kba_verification_error_counts', SUM(CPLN_1000_lexisnexis_server_error_5xx_before_kba_verification_error_counts),
     'CPLN_1001_lexisnexis_server_error_5xx_during_kba_verification_error_counts', SUM(CPLN_1001_lexisnexis_server_error_5xx_during_kba_verification_error_counts),
     'CPLN_1002_lexisnexis_server_not_found_before_kba_error_counts', SUM(CPLN_1002_lexisnexis_server_not_found_before_kba_error_counts),
     'CPLN_1003_lexisnexis_server_not_found_during_kba_error_counts', SUM(CPLN_1003_lexisnexis_server_not_found_during_kba_error_counts),
     'CPLN_1004_lexisnexis_user_not_found_error_counts', SUM(CPLN_1004_lexisnexis_user_not_found_error_counts),
     'CPLN_1005_lexisnexis_unexpected_data_error_counts', SUM(CPLN_1005_lexisnexis_unexpected_data_error_counts),
     'CPLN_1006_lexisnexis_user_blocked_error_counts', SUM(CPLN_1006_lexisnexis_user_blocked_error_counts),
     'CPLN_1007_lexisnexis_24hr_cooldown_error_counts', SUM(CPLN_1007_lexisnexis_24hr_cooldown_error_counts),
     'ccpa_agree_and_continue_clicks', SUM(ccpa_agree_and_continue_clicks),
     'ccpa_choice_form_direct_pageviews', SUM(ccpa_choice_form_direct_pageviews),
     'ccpa_doc_submit_clicks', SUM(ccpa_doc_submit_clicks),
     'ccpa_doc_upload_clicks', SUM(ccpa_doc_upload_clicks),
     'ccpa_doc_upload_pageviews', SUM(ccpa_doc_upload_pageviews),
     'ccpa_flow_complete_successes_pageviews', SUM(ccpa_flow_complete_successes_pageviews),
     'ccpa_kba_identity_question_pageviews', SUM(ccpa_kba_identity_question_pageviews),
     'ccpa_kba_speedbump_warning_continue_clicks', SUM(ccpa_kba_speedbump_warning_continue_clicks),
     'ccpa_kba_speedbump_warning_pageviews', SUM(ccpa_kba_speedbump_warning_pageviews),
     'ccpa_kba_successfully_authenticated_counts', SUM(ccpa_kba_successfully_authenticated_counts),
     'ccpa_representative_id_continue_clicks', SUM(ccpa_representative_id_continue_clicks),
     'ccpa_representative_id_form_pageviews', SUM(ccpa_representative_id_form_pageviews),
     'ccpa_resident_id_continue_clicks', SUM(ccpa_resident_id_continue_clicks),
     'ccpa_resident_id_form_pageviews', SUM(ccpa_resident_id_form_pageviews)
    ) AS tmp_map
  FROM
    (
    SELECT
      '${hiveconf:partition_date_utc}' AS partition_date_utc,
      app_section,
      user_role,
      message_context,
        SUM(CPCA_0000_captcha_validation_unspecified_captcha_error_counts) AS CPCA_0000_captcha_validation_unspecified_captcha_error_counts,
        SUM(CPCA_1000_captcha_validation_failed_captcha_error_counts) AS CPCA_1000_captcha_validation_failed_captcha_error_counts,
        SUM(CPCX_0000_citrix_unspecified_citrix_error_counts) AS CPCX_0000_citrix_unspecified_citrix_error_counts,
        SUM(CPCX_1000_citrix_invalid_file_type_error_counts) AS CPCX_1000_citrix_invalid_file_type_error_counts,
        SUM(CPCX_1001_citrix_upload_failed_error_counts) AS CPCX_1001_citrix_upload_failed_error_counts,
        SUM(CPCX_1002_citrix_file_delete_failed_error_counts) AS CPCX_1002_citrix_file_delete_failed_error_counts,
        SUM(CPCX_1003_citrix_file_size_too_large_error_counts) AS CPCX_1003_citrix_file_size_too_large_error_counts,
        SUM(CPCX_1004_citrix_file_update_failed_error_counts) AS CPCX_1004_citrix_file_update_failed_error_counts,
        SUM(CPGE_1000_privacy_api_generic_api_failure_error_counts) AS CPGE_1000_privacy_api_generic_api_failure_error_counts,
        SUM(CPGE_1001_generic_timeout_attempt_post_requisition_error_counts) AS CPGE_1001_generic_timeout_attempt_post_requisition_error_counts,
        SUM(CPGE_1002_generic_timeout_attempt_put_requisition_error_counts) AS CPGE_1002_generic_timeout_attempt_put_requisition_error_counts,
        SUM(CPGE_1003_generic_timeout_attempt_post_document_error_counts) AS CPGE_1003_generic_timeout_attempt_post_document_error_counts,
        SUM(CPGE_1004_4xx5xx_server_error_attempt_post_requisition_error_counts) AS CPGE_1004_4xx5xx_server_error_attempt_post_requisition_error_counts,
        SUM(CPGE_1005_4xx5xx_server_error_attempt_put_requisition_error_counts) AS CPGE_1005_4xx5xx_server_error_attempt_put_requisition_error_counts,
        SUM(CPGE_1006_4xx5xx_server_error_attempt_post_document_error_counts) AS CPGE_1006_4xx5xx_server_error_attempt_post_document_error_counts,
        SUM(CPGE_1007_malformed_response_from_server_with_200_status_error_counts) AS CPGE_1007_malformed_response_from_server_with_200_status_error_counts,
        SUM(CPIT_0000_it_privacy_api_issue_error_counts) AS CPIT_0000_it_privacy_api_issue_error_counts,
        SUM(CPIT_1000_it_request_ticket_submission_user_failed_verification_2x12_error_counts) AS CPIT_1000_it_request_ticket_submission_user_failed_verification_2x12_error_counts,
        SUM(CPIT_1001_it_request_ticket_submission_server_5xx_error_counts) AS CPIT_1001_it_request_ticket_submission_server_5xx_error_counts,
        SUM(CPIT_1002_it_request_ticket_request_guid_not_received_error_counts) AS CPIT_1002_it_request_ticket_request_guid_not_received_error_counts,
        SUM(CPIT_1003_it_request_ticket_unexpected_error_counts) AS CPIT_1003_it_request_ticket_unexpected_error_counts,
        SUM(CPIT_1004_it_request_ticket_duplicate_requests_by_same_user_error_counts) AS CPIT_1004_it_request_ticket_duplicate_requests_by_same_user_error_counts,
        SUM(CPKB_0000_general_error_in_orchestrator_error_counts) AS CPKB_0000_general_error_in_orchestrator_error_counts,
        SUM(CPKB_1000_kba_user_failed_verification_generic_error_counts) AS CPKB_1000_kba_user_failed_verification_generic_error_counts,
        SUM(CPKB_1001_kba_user_failed_verification_timeout_error_counts) AS CPKB_1001_kba_user_failed_verification_timeout_error_counts,
        SUM(CPKB_1002_kba_user_request_cannot_be_processed_error_counts) AS CPKB_1002_kba_user_request_cannot_be_processed_error_counts,
        SUM(CPKB_1003_kba_user_request_cannot_be_processed_cache_error_counts) AS CPKB_1003_kba_user_request_cannot_be_processed_cache_error_counts,
        SUM(CPLN_0000_lexisnexis_general_transaction_status_error_counts) AS CPLN_0000_lexisnexis_general_transaction_status_error_counts,
        SUM(CPLN_1000_lexisnexis_server_error_5xx_before_kba_verification_error_counts) AS CPLN_1000_lexisnexis_server_error_5xx_before_kba_verification_error_counts,
        SUM(CPLN_1001_lexisnexis_server_error_5xx_during_kba_verification_error_counts) AS CPLN_1001_lexisnexis_server_error_5xx_during_kba_verification_error_counts,
        SUM(CPLN_1002_lexisnexis_server_not_found_before_kba_error_counts) AS CPLN_1002_lexisnexis_server_not_found_before_kba_error_counts,
        SUM(CPLN_1003_lexisnexis_server_not_found_during_kba_error_counts) AS CPLN_1003_lexisnexis_server_not_found_during_kba_error_counts,
        SUM(CPLN_1004_lexisnexis_user_not_found_error_counts) AS CPLN_1004_lexisnexis_user_not_found_error_counts,
        SUM(CPLN_1005_lexisnexis_unexpected_data_error_counts) AS CPLN_1005_lexisnexis_unexpected_data_error_counts,
        SUM(CPLN_1006_lexisnexis_user_blocked_error_counts) AS CPLN_1006_lexisnexis_user_blocked_error_counts,
        SUM(CPLN_1007_lexisnexis_24hr_cooldown_error_counts) AS CPLN_1007_lexisnexis_24hr_cooldown_error_counts,
        SUM(ccpa_agree_and_continue_clicks) AS ccpa_agree_and_continue_clicks,
        SUM(ccpa_choice_form_direct_pageviews) AS ccpa_choice_form_direct_pageviews,
        SUM(ccpa_doc_submit_clicks) AS ccpa_doc_submit_clicks,
        SUM(ccpa_doc_upload_clicks) AS ccpa_doc_upload_clicks,
        SUM(ccpa_doc_upload_pageviews) AS ccpa_doc_upload_pageviews,
        SUM(ccpa_flow_complete_successes_pageviews) AS ccpa_flow_complete_successes_pageviews,
        SUM(ccpa_kba_identity_question_pageviews) AS ccpa_kba_identity_question_pageviews,
        SUM(ccpa_kba_speedbump_warning_continue_clicks) AS ccpa_kba_speedbump_warning_continue_clicks,
        SUM(ccpa_kba_speedbump_warning_pageviews) AS ccpa_kba_speedbump_warning_pageviews,
        SUM(ccpa_kba_successfully_authenticated_counts) AS ccpa_kba_successfully_authenticated_counts,
        SUM(ccpa_representative_id_continue_clicks) AS ccpa_representative_id_continue_clicks,
        SUM(ccpa_representative_id_form_pageviews) AS ccpa_representative_id_form_pageviews,
        SUM(ccpa_resident_id_continue_clicks) AS ccpa_resident_id_continue_clicks,
        SUM(ccpa_resident_id_form_pageviews) AS ccpa_resident_id_form_pageviews
      FROM asp_privacysite_metric_agg
      WHERE (partition_date_utc >= ("${hiveconf:START_DATE}") AND partition_date_utc < ("${hiveconf:END_DATE}"))
      GROUP BY
        app_section,
        user_role,
        message_context
      ) sumfirst
    GROUP BY
      partition_date_utc,
      app_section,
      user_role,
      message_context
    ) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
