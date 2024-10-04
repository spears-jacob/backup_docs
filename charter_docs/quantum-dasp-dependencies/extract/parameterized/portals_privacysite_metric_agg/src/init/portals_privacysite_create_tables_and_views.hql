USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable_daily} (run_date STRING);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');



USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_privacysite_metric_agg
(
  app_section STRING,
  user_role   STRING,
  device_id   STRING,
  visit_id    STRING,
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
  CPGE_1000_privacy_api_generic_api_failure_error_counts INT,
  CPIT_0000_it_privacy_api_issue_error_counts INT,
  CPIT_1000_it_request_ticket_submission_user_failed_verification_2x12_error_counts INT,
  CPIT_1001_it_request_ticket_submission_server_5xx_error_counts INT,
  CPKB_0000_general_error_in_orchestrator_error_counts INT,
  CPKB_1000_kba_user_failed_verification_generic_error_counts INT,
  CPKB_1001_kba_user_failed_verification_timeout_error_counts INT,
  CPKB_1002_kba_user_request_cannot_be_processed_error_counts INT,
  CPKB_1003_kba_user_request_cannot_be_processed_cache_error_counts INT,
  CPLN_0000_lexisnexis_general_transaction_status_error_counts INT,
  CPLN_1000_lexisnexis_server_error_5xx_before_kba_verification_error_counts INT,
  CPLN_1001_lexisnexis_server_error_5xx_during_kba_verification_error_counts INT,
  CPLN_1002_lexisnexis_server_not_found_before_kba_error_counts INT,
  CPLN_1003_lexisnexis_server_not_found_during_kba_error_counts INT,
  CPLN_1004_lexisnexis_user_not_found_error_counts INT,
  CPLN_1005_lexisnexis_unexpected_data_error_counts INT,
  CPLN_1006_lexisnexis_user_blocked_error_counts INT,
  CPLN_1007_lexisnexis_24hr_cooldown_error_counts INT,
  ccpa_agree_and_continue_clicks INT,
  ccpa_choice_form_direct_pageviews INT,
  ccpa_flow_complete_successes_pageviews INT,
  ccpa_kba_identity_question_pageviews INT,
  ccpa_kba_speedbump_warning_continue_clicks INT,
  ccpa_kba_speedbump_warning_pageviews INT,
  ccpa_kba_successfully_authenticated_counts INT,
  ccpa_resident_id_continue_clicks INT,
  ccpa_resident_id_form_pageviews INT,


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
placeholder_for_additional_strings STRING
)
PARTITIONED BY (denver_date STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - With PII (3 Years)')
;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
