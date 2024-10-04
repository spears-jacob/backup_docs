USE ${env:ENVIRONMENT};
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.tez.container.size=16000;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- {instance} metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

INSERT INTO ${env:TMP_db}.${env:domain}_${env:project}_set_agg_stage_instances_${env:execid}
SELECT
 app_section,
 user_role,
 'instances' AS unit_type,
 metric_name,
 metric_value,
 label_date_denver,
 '${env:grain}' AS grain
FROM
  (
  SELECT
    '${env:label_date_denver}' AS label_date_denver,
    app_section,
    user_role ,
      MAP(

-------------------------------------------------
--- Begin Rosetta-Generated Metric Defintions ---
-------------- Privacy Site Set Agg -------------
-------------------------------------------------
     'CPGE_1000_privacy_api_generic_api_failure_error_counts', SUM(CPGE_1000_privacy_api_generic_api_failure_error_counts),
     'CPIT_0000_it_privacy_api_issue_error_counts', SUM(CPIT_0000_it_privacy_api_issue_error_counts),
     'CPIT_1000_it_request_ticket_submission_user_failed_verification_2x12_error_counts', SUM(CPIT_1000_it_request_ticket_submission_user_failed_verification_2x12_error_counts),
     'CPIT_1001_it_request_ticket_submission_server_5xx_error_counts', SUM(CPIT_1001_it_request_ticket_submission_server_5xx_error_counts),
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
     'ccpa_flow_complete_successes_pageviews', SUM(ccpa_flow_complete_successes_pageviews),
     'ccpa_kba_identity_question_pageviews', SUM(ccpa_kba_identity_question_pageviews),
     'ccpa_kba_speedbump_warning_continue_clicks', SUM(ccpa_kba_speedbump_warning_continue_clicks),
     'ccpa_kba_speedbump_warning_pageviews', SUM(ccpa_kba_speedbump_warning_pageviews),
     'ccpa_kba_successfully_authenticated_counts', SUM(ccpa_kba_successfully_authenticated_counts),
     'ccpa_resident_id_continue_clicks', SUM(ccpa_resident_id_continue_clicks),
     'ccpa_resident_id_form_pageviews', SUM(ccpa_resident_id_form_pageviews)

    --------------------------------------------------------------------------------
    --------------------------------------------------------------------------------
    --------------------------------------------------------------------------------

    ) AS tmp_map
  FROM
    (
    SELECT
      '${env:label_date_denver}' AS label_date_denver,
      app_section,
      user_role ,

      --------------------------------------------------------------------------------
      --------------------------------------------------------------------------------
      --------------------------------------------------------------------------------
        SUM(CPGE_1000_privacy_api_generic_api_failure_error_counts) AS CPGE_1000_privacy_api_generic_api_failure_error_counts,
        SUM(CPIT_0000_it_privacy_api_issue_error_counts) AS CPIT_0000_it_privacy_api_issue_error_counts,
        SUM(CPIT_1000_it_request_ticket_submission_user_failed_verification_2x12_error_counts) AS CPIT_1000_it_request_ticket_submission_user_failed_verification_2x12_error_counts,
        SUM(CPIT_1001_it_request_ticket_submission_server_5xx_error_counts) AS CPIT_1001_it_request_ticket_submission_server_5xx_error_counts,
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
        SUM(ccpa_flow_complete_successes_pageviews) AS ccpa_flow_complete_successes_pageviews,
        SUM(ccpa_kba_identity_question_pageviews) AS ccpa_kba_identity_question_pageviews,
        SUM(ccpa_kba_speedbump_warning_continue_clicks) AS ccpa_kba_speedbump_warning_continue_clicks,
        SUM(ccpa_kba_speedbump_warning_pageviews) AS ccpa_kba_speedbump_warning_pageviews,
        SUM(ccpa_kba_successfully_authenticated_counts) AS ccpa_kba_successfully_authenticated_counts,
        SUM(ccpa_resident_id_continue_clicks) AS ccpa_resident_id_continue_clicks,
        SUM(ccpa_resident_id_form_pageviews) AS ccpa_resident_id_form_pageviews

--------------------------------------------------------------------------------
-----------------------sets_set_agg_instaces_03.hql start-----------------------
--------------------------------------------------------------------------------


      FROM asp_privacysite_metric_agg
      WHERE (denver_date >= ("${env:START_DATE}") AND denver_date < ("${env:END_DATE}"))
      GROUP BY
        app_section,
        user_role
      ) sumfirst
    GROUP BY
      label_date_denver,
      app_section,
      user_role
    ) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

--------------------------------------------------------------------------------
-----------------------sets_set_agg_instaces_03.hql end-----------------------
--------------------------------***** END *****---------------------------------
--------------------------------------------------------------------------------
