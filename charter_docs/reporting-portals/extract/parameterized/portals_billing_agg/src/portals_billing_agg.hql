-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
--  Billing Detail set aggregation HQL
--
--  '${env:grain}' AS grain
--  '${env:label_date_denver}',
--- "${env:START_DATE}" "${env:END_DATE}"
--- "${env:ProcessTimestamp}"  "${env:ProcessUser}"

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
USE ${env:ENVIRONMENT};
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- Instance Metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

INSERT OVERWRITE TABLE asp_billing_set_agg PARTITION (label_date_denver, grain)
SELECT
   CASE WHEN (grouping_id & 2) != 0        THEN application_name
        ELSE 'All Platforms' END             AS application_name,
   CASE WHEN (grouping_id & 4) != 0        THEN mso
        ELSE 'All Companies' END             AS mso,
   CASE WHEN (grouping_id & 8) != 0        THEN autopay_status
        ELSE 'All Autopay Statuses' END      AS autopay_status,
   CASE WHEN (grouping_id & 16) != 0       THEN billing_status
        ELSE 'All Billing Statuses' END      AS billing_status,

   metric_name,
   metric_value,
   grouping_id,

   '${env:ProcessTimestamp}'            AS process_date_time_denver,
   '${env:ProcessUser}'                 AS process_identity,
   label_date_denver,
   '${env:grain}'                       AS grain
FROM
  (
  SELECT
    label_date_denver,
    mso,
    application_name,
    autopay_status,
    billing_status,
    CAST(grouping__id AS INT)           AS grouping_id,

    MAP(
      'portals_autopay_enroll_radio_toggle_instances', SUM(portals_autopay_enroll_radio_toggle),
      'portals_one_time_payment_select_amount_instances', SUM(portals_one_time_payment_select_amount),
      'portals_one_time_payment_select_date_instances', SUM(portals_one_time_payment_select_date),
      'portals_one_time_payment_select_payment_method_instances', SUM(portals_one_time_payment_select_payment_method),
      'portals_one_time_payment_submits_instances', SUM(portals_one_time_payment_submits),
      'portals_bill_pay_clicks_from_local_nav_instances', SUM(portals_bill_pay_clicks_from_local_nav),
      'portals_one_time_payment_failures_instances', SUM(portals_one_time_payment_failures),
      'portals_one_time_payment_starts_instances', SUM(portals_one_time_payment_starts),
      'portals_one_time_payment_successes_instances', SUM(portals_one_time_payment_successes),
      'portals_set_up_auto_payment_failures_instances', SUM(portals_set_up_auto_payment_failures),
      'portals_set_up_auto_payment_starts_instances', SUM(portals_set_up_auto_payment_starts),
      'portals_set_up_auto_payment_submits_instances', SUM(portals_set_up_auto_payment_submits),
      'portals_set_up_auto_payment_successes_instances', SUM(portals_set_up_auto_payment_successes),
      'portals_view_online_statments_instances', SUM(portals_view_online_statments),
      'portals_set_up_auto_payment_successes_with_payment_instances', SUM(portals_set_up_auto_payment_successes_with_payment),
      'portals_set_up_auto_payment_successes_without_payment_instances', SUM(portals_set_up_auto_payment_successes_without_payment),
      'portals_one_time_payment_successes_with_ap_enroll_instances', SUM(portals_one_time_payment_successes_with_ap_enroll),
      'portals_one_time_payment_successes_without_ap_enroll_instances', SUM(portals_one_time_payment_successes_without_ap_enroll),
      'portals_billing_feature_interaction_instances', SUM(portals_billing_feature_interaction),
      'portals_bill_delivery_pref_paperless_submit_instances', SUM(portals_bill_delivery_pref_paperless_submit),
      'portals_bill_delivery_pref_paper_instances', SUM(portals_bill_delivery_pref_paper),
      'portals_bill_delivery_pref_keep_paperless_instances', SUM(portals_bill_delivery_pref_keep_paperless),
      'portals_bill_delivery_pref_exit_instances', SUM(portals_bill_delivery_pref_exit),
      'portals_bill_delivery_pref_manage_paperless_instances', SUM(portals_bill_delivery_pref_manage_paperless),
      'portals_bill_delivery_pref_paperless_enroll_from_homepage_instances', SUM(portals_bill_delivery_pref_paperless_enroll_from_homepage),
      'portals_bill_delivery_pref_paperless_submit_from_billinghome_instances', SUM(portals_bill_delivery_pref_paperless_submit_from_billinghome),
      'portals_bill_delivery_pref_paperless_submit_from_homepage_instances', SUM(portals_bill_delivery_pref_paperless_submit_from_homepage),
      'portals_bill_delivery_pref_paperless_submit_from_paperless_instances', SUM(portals_bill_delivery_pref_paperless_submit_from_paperless),
      'portals_bill_delivery_pref_paperless_success_instances', SUM(portals_bill_delivery_pref_paperless_success),
      'portals_bill_delivery_pref_paper_success_instances', SUM(portals_bill_delivery_pref_paper_success),
      'portals_bill_delivery_pref_paperless_failure_instances', SUM(portals_bill_delivery_pref_paperless_failure),
      'portals_bill_delivery_pref_paper_failure_instances', SUM(portals_bill_delivery_pref_paper_failure),
      'portals_one_time_payment_submission_new_payment_method_instances', SUM(portals_one_time_payment_submission_new_payment_method),
      'portals_one_time_payment_submission_new_payment_method_not_stored_instances', SUM(portals_one_time_payment_submission_new_payment_method_not_stored),
      'portals_one_time_payment_submission_new_payment_method_stored_instances', SUM(portals_one_time_payment_submission_new_payment_method_stored),
      'portals_one_time_payment_submission_previously_stored_payment_method_instances', SUM(portals_one_time_payment_submission_previously_stored_payment_method),
      'portals_one_time_payment_checking_account_instances', SUM(portals_one_time_payment_checking_account),
      'portals_one_time_payment_checking_instances', SUM(portals_one_time_payment_checking),
      'portals_one_time_payment_credit_debit_card_instances', SUM(portals_one_time_payment_credit_debit_card),
      'portals_one_time_payment_credit_instances', SUM(portals_one_time_payment_credit),
      'portals_one_time_payment_savings_account_instances', SUM(portals_one_time_payment_savings_account),
      'portals_one_time_payment_savings_instances', SUM(portals_one_time_payment_savings),
      'portals_account_billing_support_views_instances', SUM(portals_account_billing_support_views)
    ) AS tmp_map

  FROM
    (
    SELECT
      '${env:label_date_denver}'        AS label_date_denver,
      mso,
      application_name,
      autopay_status,
      billing_status,

      SUM(portals_autopay_enroll_radio_toggle) AS portals_autopay_enroll_radio_toggle,
      SUM(portals_one_time_payment_select_amount) AS portals_one_time_payment_select_amount,
      SUM(portals_one_time_payment_select_date) AS portals_one_time_payment_select_date,
      SUM(portals_one_time_payment_select_payment_method) AS portals_one_time_payment_select_payment_method,
      SUM(portals_one_time_payment_submits) AS portals_one_time_payment_submits,
      SUM(portals_bill_pay_clicks_from_local_nav) AS portals_bill_pay_clicks_from_local_nav,
      SUM(portals_one_time_payment_failures) AS portals_one_time_payment_failures,
      SUM(portals_one_time_payment_starts) AS portals_one_time_payment_starts,
      SUM(portals_one_time_payment_successes) AS portals_one_time_payment_successes,
      SUM(portals_set_up_auto_payment_failures) AS portals_set_up_auto_payment_failures,
      SUM(portals_set_up_auto_payment_starts) AS portals_set_up_auto_payment_starts,
      SUM(portals_set_up_auto_payment_submits) AS portals_set_up_auto_payment_submits,
      SUM(portals_set_up_auto_payment_successes) AS portals_set_up_auto_payment_successes,
      SUM(portals_view_online_statments) AS portals_view_online_statments,
      SUM(portals_set_up_auto_payment_successes_with_payment) AS portals_set_up_auto_payment_successes_with_payment,
      SUM(portals_set_up_auto_payment_successes_without_payment) AS portals_set_up_auto_payment_successes_without_payment,
      SUM(portals_one_time_payment_successes_with_ap_enroll) AS portals_one_time_payment_successes_with_ap_enroll,
      SUM(portals_one_time_payment_successes_without_ap_enroll) AS portals_one_time_payment_successes_without_ap_enroll,
      SUM(portals_billing_feature_interaction) AS portals_billing_feature_interaction,
      SUM(portals_bill_delivery_pref_paperless_submit) AS portals_bill_delivery_pref_paperless_submit,
      SUM(portals_bill_delivery_pref_paper) AS portals_bill_delivery_pref_paper,
      SUM(portals_bill_delivery_pref_keep_paperless) AS portals_bill_delivery_pref_keep_paperless,
      SUM(portals_bill_delivery_pref_exit) AS portals_bill_delivery_pref_exit,
      SUM(portals_bill_delivery_pref_manage_paperless) AS portals_bill_delivery_pref_manage_paperless,
      SUM(portals_bill_delivery_pref_paperless_enroll_from_homepage) AS portals_bill_delivery_pref_paperless_enroll_from_homepage,
      SUM(portals_bill_delivery_pref_paperless_submit_from_billinghome) AS portals_bill_delivery_pref_paperless_submit_from_billinghome,
      SUM(portals_bill_delivery_pref_paperless_submit_from_homepage) AS portals_bill_delivery_pref_paperless_submit_from_homepage,
      SUM(portals_bill_delivery_pref_paperless_submit_from_paperless) AS portals_bill_delivery_pref_paperless_submit_from_paperless,
      SUM(portals_bill_delivery_pref_paperless_success) AS portals_bill_delivery_pref_paperless_success,
      SUM(portals_bill_delivery_pref_paper_success) AS portals_bill_delivery_pref_paper_success,
      SUM(portals_bill_delivery_pref_paperless_failure) AS portals_bill_delivery_pref_paperless_failure,
      SUM(portals_bill_delivery_pref_paper_failure) AS portals_bill_delivery_pref_paper_failure,
      SUM(portals_one_time_payment_submission_new_payment_method) AS portals_one_time_payment_submission_new_payment_method,
      SUM(portals_one_time_payment_submission_new_payment_method_not_stored) AS portals_one_time_payment_submission_new_payment_method_not_stored,
      SUM(portals_one_time_payment_submission_new_payment_method_stored) AS portals_one_time_payment_submission_new_payment_method_stored,
      SUM(portals_one_time_payment_submission_previously_stored_payment_method) AS portals_one_time_payment_submission_previously_stored_payment_method,
      SUM(portals_one_time_payment_checking_account) AS portals_one_time_payment_checking_account,
      SUM(portals_one_time_payment_checking) AS portals_one_time_payment_checking,
      SUM(portals_one_time_payment_credit_debit_card) AS portals_one_time_payment_credit_debit_card,
      SUM(portals_one_time_payment_credit) AS portals_one_time_payment_credit,
      SUM(portals_one_time_payment_savings_account) AS portals_one_time_payment_savings_account,
      SUM(portals_one_time_payment_savings) AS portals_one_time_payment_savings,
      SUM(portals_account_billing_support_views) AS portals_account_billing_support_views

    FROM asp_quantum_metric_agg_v
    WHERE (denver_date >= ("${env:START_DATE}") AND denver_date < ("${env:END_DATE}"))
    GROUP BY
      '${env:label_date_denver}',
      application_name,
      mso,
      autopay_status,
      billing_status
    ) sumfirst
  GROUP BY
    label_date_denver,
    application_name,
    mso,
    autopay_status,
    billing_status
  GROUPING SETS (
    (label_date_denver),
    (label_date_denver, application_name),
    (label_date_denver, application_name, mso, autopay_status, billing_status))
  ) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- Visit Metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

UNION ALL
SELECT
   CASE WHEN (grouping_id & 2) != 0        THEN application_name
        ELSE 'All Platforms' END             AS application_name,
   CASE WHEN (grouping_id & 4) != 0        THEN mso
        ELSE 'All Companies' END             AS mso,
   CASE WHEN (grouping_id & 8) != 0        THEN autopay_status
        ELSE 'All Autopay Statuses' END      AS autopay_status,
   CASE WHEN (grouping_id & 16) != 0       THEN billing_status
        ELSE 'All Billing Statuses' END      AS billing_status,

  metric_name,
  metric_value,
  grouping_id,

  '${env:ProcessTimestamp}'             AS process_date_time_denver,
  '${env:ProcessUser}'                  AS process_identity,
  label_date_denver,
  '${env:grain}'                        AS grain
FROM
  (
  SELECT
    '${env:label_date_denver}'          AS label_date_denver,
    mso,
    application_name,
    autopay_status,
    billing_status,
    grouping_id,

    MAP(
      'portals_autopay_enroll_radio_toggle_visits', SUM(portals_autopay_enroll_radio_toggle),
      'portals_one_time_payment_select_amount_visits', SUM(portals_one_time_payment_select_amount),
      'portals_one_time_payment_select_date_visits', SUM(portals_one_time_payment_select_date),
      'portals_one_time_payment_select_payment_method_visits', SUM(portals_one_time_payment_select_payment_method),
      'portals_one_time_payment_submits_visits', SUM(portals_one_time_payment_submits),
      'portals_bill_pay_clicks_from_local_nav_visits', SUM(portals_bill_pay_clicks_from_local_nav),
      'portals_one_time_payment_failures_visits', SUM(portals_one_time_payment_failures),
      'portals_one_time_payment_starts_visits', SUM(portals_one_time_payment_starts),
      'portals_one_time_payment_successes_visits', SUM(portals_one_time_payment_successes),
      'portals_set_up_auto_payment_failures_visits', SUM(portals_set_up_auto_payment_failures),
      'portals_set_up_auto_payment_starts_visits', SUM(portals_set_up_auto_payment_starts),
      'portals_set_up_auto_payment_submits_visits', SUM(portals_set_up_auto_payment_submits),
      'portals_set_up_auto_payment_successes_visits', SUM(portals_set_up_auto_payment_successes),
      'portals_view_online_statments_visits', SUM(portals_view_online_statments),
      'portals_set_up_auto_payment_successes_with_payment_visits', SUM(portals_set_up_auto_payment_successes_with_payment),
      'portals_set_up_auto_payment_successes_without_payment_visits', SUM(portals_set_up_auto_payment_successes_without_payment),
      'portals_one_time_payment_successes_with_ap_enroll_visits', SUM(portals_one_time_payment_successes_with_ap_enroll),
      'portals_one_time_payment_successes_without_ap_enroll_visits', SUM(portals_one_time_payment_successes_without_ap_enroll),
      'portals_billing_feature_interaction_visits', SUM(portals_billing_feature_interaction),
      'portals_bill_delivery_pref_paperless_submit_visits', SUM(portals_bill_delivery_pref_paperless_submit),
      'portals_bill_delivery_pref_paper_visits', SUM(portals_bill_delivery_pref_paper),
      'portals_bill_delivery_pref_keep_paperless_visits', SUM(portals_bill_delivery_pref_keep_paperless),
      'portals_bill_delivery_pref_exit_visits', SUM(portals_bill_delivery_pref_exit),
      'portals_bill_delivery_pref_manage_paperless_visits', SUM(portals_bill_delivery_pref_manage_paperless),
      'portals_bill_delivery_pref_paperless_enroll_from_homepage_visits', SUM(portals_bill_delivery_pref_paperless_enroll_from_homepage),
      'portals_bill_delivery_pref_paperless_submit_from_billinghome_visits', SUM(portals_bill_delivery_pref_paperless_submit_from_billinghome),
      'portals_bill_delivery_pref_paperless_submit_from_homepage_visits', SUM(portals_bill_delivery_pref_paperless_submit_from_homepage),
      'portals_bill_delivery_pref_paperless_submit_from_paperless_visits', SUM(portals_bill_delivery_pref_paperless_submit_from_paperless),
      'portals_bill_delivery_pref_paperless_success_visits', SUM(portals_bill_delivery_pref_paperless_success),
      'portals_bill_delivery_pref_paper_success_visits', SUM(portals_bill_delivery_pref_paper_success),
      'portals_bill_delivery_pref_paperless_failure_visits', SUM(portals_bill_delivery_pref_paperless_failure),
      'portals_bill_delivery_pref_paper_failure_visits', SUM(portals_bill_delivery_pref_paper_failure),
      'portals_one_time_payment_submission_new_payment_method_visits', SUM(portals_one_time_payment_submission_new_payment_method),
      'portals_one_time_payment_submission_new_payment_method_not_stored_visits', SUM(portals_one_time_payment_submission_new_payment_method_not_stored),
      'portals_one_time_payment_submission_new_payment_method_stored_visits', SUM(portals_one_time_payment_submission_new_payment_method_stored),
      'portals_one_time_payment_submission_previously_stored_payment_method_visits', SUM(portals_one_time_payment_submission_previously_stored_payment_method),
      'portals_one_time_payment_checking_account_visits', SUM(portals_one_time_payment_checking_account),
      'portals_one_time_payment_checking_visits', SUM(portals_one_time_payment_checking),
      'portals_one_time_payment_credit_debit_card_visits', SUM(portals_one_time_payment_credit_debit_card),
      'portals_one_time_payment_credit_visits', SUM(portals_one_time_payment_credit),
      'portals_one_time_payment_savings_account_visits', SUM(portals_one_time_payment_savings_account),
      'portals_one_time_payment_savings_visits', SUM(portals_one_time_payment_savings),
      'portals_account_billing_support_views_visits', SUM(portals_account_billing_support_views)
    ) AS tmp_map

  FROM
    (
    SELECT
      visit_id,
      mso,
      application_name,
      autopay_status,
      billing_status,
      CAST(grouping__id AS INT)         AS grouping_id,

      IF(SUM(portals_autopay_enroll_radio_toggle) > 0, 1, 0) AS portals_autopay_enroll_radio_toggle,
      IF(SUM(portals_one_time_payment_select_amount) > 0, 1, 0) AS portals_one_time_payment_select_amount,
      IF(SUM(portals_one_time_payment_select_date) > 0, 1, 0) AS portals_one_time_payment_select_date,
      IF(SUM(portals_one_time_payment_select_payment_method) > 0, 1, 0) AS portals_one_time_payment_select_payment_method,
      IF(SUM(portals_one_time_payment_submits) > 0, 1, 0) AS portals_one_time_payment_submits,
      IF(SUM(portals_bill_pay_clicks_from_local_nav) > 0, 1, 0) AS portals_bill_pay_clicks_from_local_nav,
      IF(SUM(portals_one_time_payment_failures) > 0, 1, 0) AS portals_one_time_payment_failures,
      IF(SUM(portals_one_time_payment_starts) > 0, 1, 0) AS portals_one_time_payment_starts,
      IF(SUM(portals_one_time_payment_successes) > 0, 1, 0) AS portals_one_time_payment_successes,
      IF(SUM(portals_set_up_auto_payment_failures) > 0, 1, 0) AS portals_set_up_auto_payment_failures,
      IF(SUM(portals_set_up_auto_payment_starts) > 0, 1, 0) AS portals_set_up_auto_payment_starts,
      IF(SUM(portals_set_up_auto_payment_submits) > 0, 1, 0) AS portals_set_up_auto_payment_submits,
      IF(SUM(portals_set_up_auto_payment_successes) > 0, 1, 0) AS portals_set_up_auto_payment_successes,
      IF(SUM(portals_view_online_statments) > 0, 1, 0) AS portals_view_online_statments,
      IF(SUM(portals_set_up_auto_payment_successes_with_payment) > 0, 1, 0) AS portals_set_up_auto_payment_successes_with_payment,
      IF(SUM(portals_set_up_auto_payment_successes_without_payment) > 0, 1, 0) AS portals_set_up_auto_payment_successes_without_payment,
      IF(SUM(portals_one_time_payment_successes_with_ap_enroll) > 0, 1, 0) AS portals_one_time_payment_successes_with_ap_enroll,
      IF(SUM(portals_one_time_payment_successes_without_ap_enroll) > 0, 1, 0) AS portals_one_time_payment_successes_without_ap_enroll,
      IF(SUM(portals_billing_feature_interaction) > 0, 1, 0) AS portals_billing_feature_interaction,
      IF(SUM(portals_bill_delivery_pref_paperless_submit) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit,
      IF(SUM(portals_bill_delivery_pref_paper) > 0, 1, 0) AS portals_bill_delivery_pref_paper,
      IF(SUM(portals_bill_delivery_pref_keep_paperless) > 0, 1, 0) AS portals_bill_delivery_pref_keep_paperless,
      IF(SUM(portals_bill_delivery_pref_exit) > 0, 1, 0) AS portals_bill_delivery_pref_exit,
      IF(SUM(portals_bill_delivery_pref_manage_paperless) > 0, 1, 0) AS portals_bill_delivery_pref_manage_paperless,
      IF(SUM(portals_bill_delivery_pref_paperless_enroll_from_homepage) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_enroll_from_homepage,
      IF(SUM(portals_bill_delivery_pref_paperless_submit_from_billinghome) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit_from_billinghome,
      IF(SUM(portals_bill_delivery_pref_paperless_submit_from_homepage) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit_from_homepage,
      IF(SUM(portals_bill_delivery_pref_paperless_submit_from_paperless) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit_from_paperless,
      IF(SUM(portals_bill_delivery_pref_paperless_success) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_success,
      IF(SUM(portals_bill_delivery_pref_paper_success) > 0, 1, 0) AS portals_bill_delivery_pref_paper_success,
      IF(SUM(portals_bill_delivery_pref_paperless_failure) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_failure,
      IF(SUM(portals_bill_delivery_pref_paper_failure) > 0, 1, 0) AS portals_bill_delivery_pref_paper_failure,
      IF(SUM(portals_one_time_payment_submission_new_payment_method) > 0, 1, 0) AS portals_one_time_payment_submission_new_payment_method,
      IF(SUM(portals_one_time_payment_submission_new_payment_method_not_stored) > 0, 1, 0) AS portals_one_time_payment_submission_new_payment_method_not_stored,
      IF(SUM(portals_one_time_payment_submission_new_payment_method_stored) > 0, 1, 0) AS portals_one_time_payment_submission_new_payment_method_stored,
      IF(SUM(portals_one_time_payment_submission_previously_stored_payment_method) > 0, 1, 0) AS portals_one_time_payment_submission_previously_stored_payment_method,
      IF(SUM(portals_one_time_payment_checking_account) > 0, 1, 0) AS portals_one_time_payment_checking_account,
      IF(SUM(portals_one_time_payment_checking) > 0, 1, 0) AS portals_one_time_payment_checking,
      IF(SUM(portals_one_time_payment_credit_debit_card) > 0, 1, 0) AS portals_one_time_payment_credit_debit_card,
      IF(SUM(portals_one_time_payment_credit) > 0, 1, 0) AS portals_one_time_payment_credit,
      IF(SUM(portals_one_time_payment_savings_account) > 0, 1, 0) AS portals_one_time_payment_savings_account,
      IF(SUM(portals_one_time_payment_savings) > 0, 1, 0) AS portals_one_time_payment_savings,
      IF(SUM(portals_account_billing_support_views) > 0, 1, 0) AS portals_account_billing_support_views

    FROM
      (
      SELECT
        visit_id,
        mso,
        application_name,
        autopay_status,
        billing_status,

        SUM(portals_autopay_enroll_radio_toggle) AS portals_autopay_enroll_radio_toggle,
        SUM(portals_one_time_payment_select_amount) AS portals_one_time_payment_select_amount,
        SUM(portals_one_time_payment_select_date) AS portals_one_time_payment_select_date,
        SUM(portals_one_time_payment_select_payment_method) AS portals_one_time_payment_select_payment_method,
        SUM(portals_one_time_payment_submits) AS portals_one_time_payment_submits,
        SUM(portals_bill_pay_clicks_from_local_nav) AS portals_bill_pay_clicks_from_local_nav,
        SUM(portals_one_time_payment_failures) AS portals_one_time_payment_failures,
        SUM(portals_one_time_payment_starts) AS portals_one_time_payment_starts,
        SUM(portals_one_time_payment_successes) AS portals_one_time_payment_successes,
        SUM(portals_set_up_auto_payment_failures) AS portals_set_up_auto_payment_failures,
        SUM(portals_set_up_auto_payment_starts) AS portals_set_up_auto_payment_starts,
        SUM(portals_set_up_auto_payment_submits) AS portals_set_up_auto_payment_submits,
        SUM(portals_set_up_auto_payment_successes) AS portals_set_up_auto_payment_successes,
        SUM(portals_view_online_statments) AS portals_view_online_statments,
        SUM(portals_set_up_auto_payment_successes_with_payment) AS portals_set_up_auto_payment_successes_with_payment,
        SUM(portals_set_up_auto_payment_successes_without_payment) AS portals_set_up_auto_payment_successes_without_payment,
        SUM(portals_one_time_payment_successes_with_ap_enroll) AS portals_one_time_payment_successes_with_ap_enroll,
        SUM(portals_one_time_payment_successes_without_ap_enroll) AS portals_one_time_payment_successes_without_ap_enroll,
        SUM(portals_billing_feature_interaction) AS portals_billing_feature_interaction,
        SUM(portals_bill_delivery_pref_paperless_submit) AS portals_bill_delivery_pref_paperless_submit,
        SUM(portals_bill_delivery_pref_paper) AS portals_bill_delivery_pref_paper,
        SUM(portals_bill_delivery_pref_keep_paperless) AS portals_bill_delivery_pref_keep_paperless,
        SUM(portals_bill_delivery_pref_exit) AS portals_bill_delivery_pref_exit,
        SUM(portals_bill_delivery_pref_manage_paperless) AS portals_bill_delivery_pref_manage_paperless,
        SUM(portals_bill_delivery_pref_paperless_enroll_from_homepage) AS portals_bill_delivery_pref_paperless_enroll_from_homepage,
        SUM(portals_bill_delivery_pref_paperless_submit_from_billinghome) AS portals_bill_delivery_pref_paperless_submit_from_billinghome,
        SUM(portals_bill_delivery_pref_paperless_submit_from_homepage) AS portals_bill_delivery_pref_paperless_submit_from_homepage,
        SUM(portals_bill_delivery_pref_paperless_submit_from_paperless) AS portals_bill_delivery_pref_paperless_submit_from_paperless,
        SUM(portals_bill_delivery_pref_paperless_success) AS portals_bill_delivery_pref_paperless_success,
        SUM(portals_bill_delivery_pref_paper_success) AS portals_bill_delivery_pref_paper_success,
        SUM(portals_bill_delivery_pref_paperless_failure) AS portals_bill_delivery_pref_paperless_failure,
        SUM(portals_bill_delivery_pref_paper_failure) AS portals_bill_delivery_pref_paper_failure,
        SUM(portals_one_time_payment_submission_new_payment_method) AS portals_one_time_payment_submission_new_payment_method,
        SUM(portals_one_time_payment_submission_new_payment_method_not_stored) AS portals_one_time_payment_submission_new_payment_method_not_stored,
        SUM(portals_one_time_payment_submission_new_payment_method_stored) AS portals_one_time_payment_submission_new_payment_method_stored,
        SUM(portals_one_time_payment_submission_previously_stored_payment_method) AS portals_one_time_payment_submission_previously_stored_payment_method,
        SUM(portals_one_time_payment_checking_account) AS portals_one_time_payment_checking_account,
        SUM(portals_one_time_payment_checking) AS portals_one_time_payment_checking,
        SUM(portals_one_time_payment_credit_debit_card) AS portals_one_time_payment_credit_debit_card,
        SUM(portals_one_time_payment_credit) AS portals_one_time_payment_credit,
        SUM(portals_one_time_payment_savings_account) AS portals_one_time_payment_savings_account,
        SUM(portals_one_time_payment_savings) AS portals_one_time_payment_savings,
        SUM(portals_account_billing_support_views) AS portals_account_billing_support_views

      FROM asp_quantum_metric_agg_v
      WHERE (denver_date >= ("${env:START_DATE}") AND denver_date < ("${env:END_DATE}"))
      GROUP BY
        application_name,
        mso,
        autopay_status,
        billing_status,
        visit_id
      ) sumfirst
    GROUP BY
      visit_id,
      application_name,
      mso,
      autopay_status,
      billing_status
    GROUPING SETS (
      (visit_id),
      (visit_id, application_name),
      (visit_id, application_name, mso, autopay_status, billing_status))
    ) sets
  GROUP BY
    '${env:label_date_denver}',
    application_name,
    mso,
    autopay_status,
    billing_status,
    grouping_id
  ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- Device Metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

UNION ALL
SELECT
   CASE WHEN (grouping_id & 2) != 0        THEN application_name
        ELSE 'All Platforms' END             AS application_name,
   CASE WHEN (grouping_id & 4) != 0        THEN mso
        ELSE 'All Companies' END             AS mso,
   CASE WHEN (grouping_id & 8) != 0        THEN autopay_status
        ELSE 'All Autopay Statuses' END      AS autopay_status,
   CASE WHEN (grouping_id & 16) != 0       THEN billing_status
        ELSE 'All Billing Statuses' END      AS billing_status,

  metric_name,
  metric_value,
  grouping_id,

  '${env:ProcessTimestamp}'             AS process_date_time_denver,
  '${env:ProcessUser}'                  AS process_identity,
  label_date_denver,
  '${env:grain}'                        AS grain
FROM
  (
  SELECT
    '${env:label_date_denver}'          AS label_date_denver,
    mso,
    application_name,
    autopay_status,
    billing_status,
    grouping_id,

    MAP(
      'portals_autopay_enroll_radio_toggle_devices', SUM(portals_autopay_enroll_radio_toggle),
      'portals_one_time_payment_select_amount_devices', SUM(portals_one_time_payment_select_amount),
      'portals_one_time_payment_select_date_devices', SUM(portals_one_time_payment_select_date),
      'portals_one_time_payment_select_payment_method_devices', SUM(portals_one_time_payment_select_payment_method),
      'portals_one_time_payment_submits_devices', SUM(portals_one_time_payment_submits),
      'portals_bill_pay_clicks_from_local_nav_devices', SUM(portals_bill_pay_clicks_from_local_nav),
      'portals_one_time_payment_failures_devices', SUM(portals_one_time_payment_failures),
      'portals_one_time_payment_starts_devices', SUM(portals_one_time_payment_starts),
      'portals_one_time_payment_successes_devices', SUM(portals_one_time_payment_successes),
      'portals_set_up_auto_payment_failures_devices', SUM(portals_set_up_auto_payment_failures),
      'portals_set_up_auto_payment_starts_devices', SUM(portals_set_up_auto_payment_starts),
      'portals_set_up_auto_payment_submits_devices', SUM(portals_set_up_auto_payment_submits),
      'portals_set_up_auto_payment_successes_devices', SUM(portals_set_up_auto_payment_successes),
      'portals_view_online_statments_devices', SUM(portals_view_online_statments),
      'portals_set_up_auto_payment_successes_with_payment_devices', SUM(portals_set_up_auto_payment_successes_with_payment),
      'portals_set_up_auto_payment_successes_without_payment_devices', SUM(portals_set_up_auto_payment_successes_without_payment),
      'portals_one_time_payment_successes_with_ap_enroll_devices', SUM(portals_one_time_payment_successes_with_ap_enroll),
      'portals_one_time_payment_successes_without_ap_enroll_devices', SUM(portals_one_time_payment_successes_without_ap_enroll),
      'portals_billing_feature_interaction_devices', SUM(portals_billing_feature_interaction),
      'portals_bill_delivery_pref_paperless_submit_devices', SUM(portals_bill_delivery_pref_paperless_submit),
      'portals_bill_delivery_pref_paper_devices', SUM(portals_bill_delivery_pref_paper),
      'portals_bill_delivery_pref_keep_paperless_devices', SUM(portals_bill_delivery_pref_keep_paperless),
      'portals_bill_delivery_pref_exit_devices', SUM(portals_bill_delivery_pref_exit),
      'portals_bill_delivery_pref_manage_paperless_devices', SUM(portals_bill_delivery_pref_manage_paperless),
      'portals_bill_delivery_pref_paperless_enroll_from_homepage_devices', SUM(portals_bill_delivery_pref_paperless_enroll_from_homepage),
      'portals_bill_delivery_pref_paperless_submit_from_billinghome_devices', SUM(portals_bill_delivery_pref_paperless_submit_from_billinghome),
      'portals_bill_delivery_pref_paperless_submit_from_homepage_devices', SUM(portals_bill_delivery_pref_paperless_submit_from_homepage),
      'portals_bill_delivery_pref_paperless_submit_from_paperless_devices', SUM(portals_bill_delivery_pref_paperless_submit_from_paperless),
      'portals_bill_delivery_pref_paperless_success_devices', SUM(portals_bill_delivery_pref_paperless_success),
      'portals_bill_delivery_pref_paper_success_devices', SUM(portals_bill_delivery_pref_paper_success),
      'portals_bill_delivery_pref_paperless_failure_devices', SUM(portals_bill_delivery_pref_paperless_failure),
      'portals_bill_delivery_pref_paper_failure_devices', SUM(portals_bill_delivery_pref_paper_failure),
      'portals_one_time_payment_submission_new_payment_method_devices', SUM(portals_one_time_payment_submission_new_payment_method),
      'portals_one_time_payment_submission_new_payment_method_not_stored_devices', SUM(portals_one_time_payment_submission_new_payment_method_not_stored),
      'portals_one_time_payment_submission_new_payment_method_stored_devices', SUM(portals_one_time_payment_submission_new_payment_method_stored),
      'portals_one_time_payment_submission_previously_stored_payment_method_devices', SUM(portals_one_time_payment_submission_previously_stored_payment_method),
      'portals_one_time_payment_checking_account_devices', SUM(portals_one_time_payment_checking_account),
      'portals_one_time_payment_checking_devices', SUM(portals_one_time_payment_checking),
      'portals_one_time_payment_credit_debit_card_devices', SUM(portals_one_time_payment_credit_debit_card),
      'portals_one_time_payment_credit_devices', SUM(portals_one_time_payment_credit),
      'portals_one_time_payment_savings_account_devices', SUM(portals_one_time_payment_savings_account),
      'portals_one_time_payment_savings_devices', SUM(portals_one_time_payment_savings),
      'portals_account_billing_support_views_devices', SUM(portals_account_billing_support_views)
    ) AS tmp_map

  FROM
    (
    SELECT
      device_id,
      mso,
      application_name,
      autopay_status,
      billing_status,
      CAST(grouping__id AS INT)         AS grouping_id,

      IF(SUM(portals_autopay_enroll_radio_toggle) > 0, 1, 0) AS portals_autopay_enroll_radio_toggle,
      IF(SUM(portals_one_time_payment_select_amount) > 0, 1, 0) AS portals_one_time_payment_select_amount,
      IF(SUM(portals_one_time_payment_select_date) > 0, 1, 0) AS portals_one_time_payment_select_date,
      IF(SUM(portals_one_time_payment_select_payment_method) > 0, 1, 0) AS portals_one_time_payment_select_payment_method,
      IF(SUM(portals_one_time_payment_submits) > 0, 1, 0) AS portals_one_time_payment_submits,
      IF(SUM(portals_bill_pay_clicks_from_local_nav) > 0, 1, 0) AS portals_bill_pay_clicks_from_local_nav,
      IF(SUM(portals_one_time_payment_failures) > 0, 1, 0) AS portals_one_time_payment_failures,
      IF(SUM(portals_one_time_payment_starts) > 0, 1, 0) AS portals_one_time_payment_starts,
      IF(SUM(portals_one_time_payment_successes) > 0, 1, 0) AS portals_one_time_payment_successes,
      IF(SUM(portals_set_up_auto_payment_failures) > 0, 1, 0) AS portals_set_up_auto_payment_failures,
      IF(SUM(portals_set_up_auto_payment_starts) > 0, 1, 0) AS portals_set_up_auto_payment_starts,
      IF(SUM(portals_set_up_auto_payment_submits) > 0, 1, 0) AS portals_set_up_auto_payment_submits,
      IF(SUM(portals_set_up_auto_payment_successes) > 0, 1, 0) AS portals_set_up_auto_payment_successes,
      IF(SUM(portals_view_online_statments) > 0, 1, 0) AS portals_view_online_statments,
      IF(SUM(portals_set_up_auto_payment_successes_with_payment) > 0, 1, 0) AS portals_set_up_auto_payment_successes_with_payment,
      IF(SUM(portals_set_up_auto_payment_successes_without_payment) > 0, 1, 0) AS portals_set_up_auto_payment_successes_without_payment,
      IF(SUM(portals_one_time_payment_successes_with_ap_enroll) > 0, 1, 0) AS portals_one_time_payment_successes_with_ap_enroll,
      IF(SUM(portals_one_time_payment_successes_without_ap_enroll) > 0, 1, 0) AS portals_one_time_payment_successes_without_ap_enroll,
      IF(SUM(portals_billing_feature_interaction) > 0, 1, 0) AS portals_billing_feature_interaction,
      IF(SUM(portals_bill_delivery_pref_paperless_submit) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit,
      IF(SUM(portals_bill_delivery_pref_paper) > 0, 1, 0) AS portals_bill_delivery_pref_paper,
      IF(SUM(portals_bill_delivery_pref_keep_paperless) > 0, 1, 0) AS portals_bill_delivery_pref_keep_paperless,
      IF(SUM(portals_bill_delivery_pref_exit) > 0, 1, 0) AS portals_bill_delivery_pref_exit,
      IF(SUM(portals_bill_delivery_pref_manage_paperless) > 0, 1, 0) AS portals_bill_delivery_pref_manage_paperless,
      IF(SUM(portals_bill_delivery_pref_paperless_enroll_from_homepage) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_enroll_from_homepage,
      IF(SUM(portals_bill_delivery_pref_paperless_submit_from_billinghome) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit_from_billinghome,
      IF(SUM(portals_bill_delivery_pref_paperless_submit_from_homepage) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit_from_homepage,
      IF(SUM(portals_bill_delivery_pref_paperless_submit_from_paperless) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit_from_paperless,
      IF(SUM(portals_bill_delivery_pref_paperless_success) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_success,
      IF(SUM(portals_bill_delivery_pref_paper_success) > 0, 1, 0) AS portals_bill_delivery_pref_paper_success,
      IF(SUM(portals_bill_delivery_pref_paperless_failure) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_failure,
      IF(SUM(portals_bill_delivery_pref_paper_failure) > 0, 1, 0) AS portals_bill_delivery_pref_paper_failure,
      IF(SUM(portals_one_time_payment_submission_new_payment_method) > 0, 1, 0) AS portals_one_time_payment_submission_new_payment_method,
      IF(SUM(portals_one_time_payment_submission_new_payment_method_not_stored) > 0, 1, 0) AS portals_one_time_payment_submission_new_payment_method_not_stored,
      IF(SUM(portals_one_time_payment_submission_new_payment_method_stored) > 0, 1, 0) AS portals_one_time_payment_submission_new_payment_method_stored,
      IF(SUM(portals_one_time_payment_submission_previously_stored_payment_method) > 0, 1, 0) AS portals_one_time_payment_submission_previously_stored_payment_method,
      IF(SUM(portals_one_time_payment_checking_account) > 0, 1, 0) AS portals_one_time_payment_checking_account,
      IF(SUM(portals_one_time_payment_checking) > 0, 1, 0) AS portals_one_time_payment_checking,
      IF(SUM(portals_one_time_payment_credit_debit_card) > 0, 1, 0) AS portals_one_time_payment_credit_debit_card,
      IF(SUM(portals_one_time_payment_credit) > 0, 1, 0) AS portals_one_time_payment_credit,
      IF(SUM(portals_one_time_payment_savings_account) > 0, 1, 0) AS portals_one_time_payment_savings_account,
      IF(SUM(portals_one_time_payment_savings) > 0, 1, 0) AS portals_one_time_payment_savings,
      IF(SUM(portals_account_billing_support_views) > 0, 1, 0) AS portals_account_billing_support_views

    FROM
      (
      SELECT
        mso,
        application_name,
        autopay_status,
        billing_status,
        device_id,

        SUM(portals_autopay_enroll_radio_toggle) AS portals_autopay_enroll_radio_toggle,
        SUM(portals_one_time_payment_select_amount) AS portals_one_time_payment_select_amount,
        SUM(portals_one_time_payment_select_date) AS portals_one_time_payment_select_date,
        SUM(portals_one_time_payment_select_payment_method) AS portals_one_time_payment_select_payment_method,
        SUM(portals_one_time_payment_submits) AS portals_one_time_payment_submits,
        SUM(portals_bill_pay_clicks_from_local_nav) AS portals_bill_pay_clicks_from_local_nav,
        SUM(portals_one_time_payment_failures) AS portals_one_time_payment_failures,
        SUM(portals_one_time_payment_starts) AS portals_one_time_payment_starts,
        SUM(portals_one_time_payment_successes) AS portals_one_time_payment_successes,
        SUM(portals_set_up_auto_payment_failures) AS portals_set_up_auto_payment_failures,
        SUM(portals_set_up_auto_payment_starts) AS portals_set_up_auto_payment_starts,
        SUM(portals_set_up_auto_payment_submits) AS portals_set_up_auto_payment_submits,
        SUM(portals_set_up_auto_payment_successes) AS portals_set_up_auto_payment_successes,
        SUM(portals_view_online_statments) AS portals_view_online_statments,
        SUM(portals_set_up_auto_payment_successes_with_payment) AS portals_set_up_auto_payment_successes_with_payment,
        SUM(portals_set_up_auto_payment_successes_without_payment) AS portals_set_up_auto_payment_successes_without_payment,
        SUM(portals_one_time_payment_successes_with_ap_enroll) AS portals_one_time_payment_successes_with_ap_enroll,
        SUM(portals_one_time_payment_successes_without_ap_enroll) AS portals_one_time_payment_successes_without_ap_enroll,
        SUM(portals_billing_feature_interaction) AS portals_billing_feature_interaction,
        SUM(portals_bill_delivery_pref_paperless_submit) AS portals_bill_delivery_pref_paperless_submit,
        SUM(portals_bill_delivery_pref_paper) AS portals_bill_delivery_pref_paper,
        SUM(portals_bill_delivery_pref_keep_paperless) AS portals_bill_delivery_pref_keep_paperless,
        SUM(portals_bill_delivery_pref_exit) AS portals_bill_delivery_pref_exit,
        SUM(portals_bill_delivery_pref_manage_paperless) AS portals_bill_delivery_pref_manage_paperless,
        SUM(portals_bill_delivery_pref_paperless_enroll_from_homepage) AS portals_bill_delivery_pref_paperless_enroll_from_homepage,
        SUM(portals_bill_delivery_pref_paperless_submit_from_billinghome) AS portals_bill_delivery_pref_paperless_submit_from_billinghome,
        SUM(portals_bill_delivery_pref_paperless_submit_from_homepage) AS portals_bill_delivery_pref_paperless_submit_from_homepage,
        SUM(portals_bill_delivery_pref_paperless_submit_from_paperless) AS portals_bill_delivery_pref_paperless_submit_from_paperless,
        SUM(portals_bill_delivery_pref_paperless_success) AS portals_bill_delivery_pref_paperless_success,
        SUM(portals_bill_delivery_pref_paper_success) AS portals_bill_delivery_pref_paper_success,
        SUM(portals_bill_delivery_pref_paperless_failure) AS portals_bill_delivery_pref_paperless_failure,
        SUM(portals_bill_delivery_pref_paper_failure) AS portals_bill_delivery_pref_paper_failure,
        SUM(portals_one_time_payment_submission_new_payment_method) AS portals_one_time_payment_submission_new_payment_method,
        SUM(portals_one_time_payment_submission_new_payment_method_not_stored) AS portals_one_time_payment_submission_new_payment_method_not_stored,
        SUM(portals_one_time_payment_submission_new_payment_method_stored) AS portals_one_time_payment_submission_new_payment_method_stored,
        SUM(portals_one_time_payment_submission_previously_stored_payment_method) AS portals_one_time_payment_submission_previously_stored_payment_method,
        SUM(portals_one_time_payment_checking_account) AS portals_one_time_payment_checking_account,
        SUM(portals_one_time_payment_checking) AS portals_one_time_payment_checking,
        SUM(portals_one_time_payment_credit_debit_card) AS portals_one_time_payment_credit_debit_card,
        SUM(portals_one_time_payment_credit) AS portals_one_time_payment_credit,
        SUM(portals_one_time_payment_savings_account) AS portals_one_time_payment_savings_account,
        SUM(portals_one_time_payment_savings) AS portals_one_time_payment_savings,
        SUM(portals_account_billing_support_views) AS portals_account_billing_support_views

      FROM asp_quantum_metric_agg_v
      WHERE (denver_date >= ("${env:START_DATE}") AND denver_date < ("${env:END_DATE}"))
      GROUP BY
        application_name,
        mso,
        autopay_status,
        billing_status,
        device_id
      ) sumfirst
    GROUP BY
      device_id,
      application_name,
      mso,
      autopay_status,
      billing_status
    GROUPING SETS (
      (device_id),
      (device_id, application_name),
      (device_id, application_name, mso, autopay_status, billing_status))
    ) sets
  GROUP BY
    '${env:label_date_denver}',
    application_name,
    mso,
    autopay_status,
    billing_status,
    grouping_id
  ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- Household Metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

UNION ALL
SELECT
   CASE WHEN (grouping_id & 2) != 0        THEN application_name
        ELSE 'All Platforms' END             AS application_name,
   CASE WHEN (grouping_id & 4) != 0        THEN mso
        ELSE 'All Companies' END             AS mso,
   CASE WHEN (grouping_id & 8) != 0        THEN autopay_status
        ELSE 'All Autopay Statuses' END      AS autopay_status,
   CASE WHEN (grouping_id & 16) != 0       THEN billing_status
        ELSE 'All Billing Statuses' END      AS billing_status,

  metric_name,
  metric_value,
  grouping_id,

  '${env:ProcessTimestamp}'             AS process_date_time_denver,
  '${env:ProcessUser}'                  AS process_identity,
  label_date_denver,
  '${env:grain}'                        AS grain

FROM
  (
  SELECT
    '${env:label_date_denver}'          AS label_date_denver,
    mso,
    application_name,
    autopay_status,
    billing_status,
    grouping_id,

    MAP(
      'portals_autopay_enroll_radio_toggle_hh', SUM(portals_autopay_enroll_radio_toggle),
      'portals_one_time_payment_select_amount_hh', SUM(portals_one_time_payment_select_amount),
      'portals_one_time_payment_select_date_hh', SUM(portals_one_time_payment_select_date),
      'portals_one_time_payment_select_payment_method_hh', SUM(portals_one_time_payment_select_payment_method),
      'portals_one_time_payment_submits_hh', SUM(portals_one_time_payment_submits),
      'portals_bill_pay_clicks_from_local_nav_hh', SUM(portals_bill_pay_clicks_from_local_nav),
      'portals_one_time_payment_failures_hh', SUM(portals_one_time_payment_failures),
      'portals_one_time_payment_starts_hh', SUM(portals_one_time_payment_starts),
      'portals_one_time_payment_successes_hh', SUM(portals_one_time_payment_successes),
      'portals_set_up_auto_payment_failures_hh', SUM(portals_set_up_auto_payment_failures),
      'portals_set_up_auto_payment_starts_hh', SUM(portals_set_up_auto_payment_starts),
      'portals_set_up_auto_payment_submits_hh', SUM(portals_set_up_auto_payment_submits),
      'portals_set_up_auto_payment_successes_hh', SUM(portals_set_up_auto_payment_successes),
      'portals_view_online_statments_hh', SUM(portals_view_online_statments),
      'portals_set_up_auto_payment_successes_with_payment_hh', SUM(portals_set_up_auto_payment_successes_with_payment),
      'portals_set_up_auto_payment_successes_without_payment_hh', SUM(portals_set_up_auto_payment_successes_without_payment),
      'portals_one_time_payment_successes_with_ap_enroll_hh', SUM(portals_one_time_payment_successes_with_ap_enroll),
      'portals_one_time_payment_successes_without_ap_enroll_hh', SUM(portals_one_time_payment_successes_without_ap_enroll),
      'portals_billing_feature_interaction_hh', SUM(portals_billing_feature_interaction),
      'portals_bill_delivery_pref_paperless_submit_hh', SUM(portals_bill_delivery_pref_paperless_submit),
      'portals_bill_delivery_pref_paper_hh', SUM(portals_bill_delivery_pref_paper),
      'portals_bill_delivery_pref_keep_paperless_hh', SUM(portals_bill_delivery_pref_keep_paperless),
      'portals_bill_delivery_pref_exit_hh', SUM(portals_bill_delivery_pref_exit),
      'portals_bill_delivery_pref_manage_paperless_hh', SUM(portals_bill_delivery_pref_manage_paperless),
      'portals_bill_delivery_pref_paperless_enroll_from_homepage_hh', SUM(portals_bill_delivery_pref_paperless_enroll_from_homepage),
      'portals_bill_delivery_pref_paperless_submit_from_billinghome_hh', SUM(portals_bill_delivery_pref_paperless_submit_from_billinghome),
      'portals_bill_delivery_pref_paperless_submit_from_homepage_hh', SUM(portals_bill_delivery_pref_paperless_submit_from_homepage),
      'portals_bill_delivery_pref_paperless_submit_from_paperless_hh', SUM(portals_bill_delivery_pref_paperless_submit_from_paperless),
      'portals_bill_delivery_pref_paperless_success_hh', SUM(portals_bill_delivery_pref_paperless_success),
      'portals_bill_delivery_pref_paper_success_hh', SUM(portals_bill_delivery_pref_paper_success),
      'portals_bill_delivery_pref_paperless_failure_hh', SUM(portals_bill_delivery_pref_paperless_failure),
      'portals_bill_delivery_pref_paper_failure_hh', SUM(portals_bill_delivery_pref_paper_failure),
      'portals_one_time_payment_submission_new_payment_method_hh', SUM(portals_one_time_payment_submission_new_payment_method),
      'portals_one_time_payment_submission_new_payment_method_not_stored_hh', SUM(portals_one_time_payment_submission_new_payment_method_not_stored),
      'portals_one_time_payment_submission_new_payment_method_stored_hh', SUM(portals_one_time_payment_submission_new_payment_method_stored),
      'portals_one_time_payment_submission_previously_stored_payment_method_hh', SUM(portals_one_time_payment_submission_previously_stored_payment_method),
      'portals_one_time_payment_checking_account_hh', SUM(portals_one_time_payment_checking_account),
      'portals_one_time_payment_checking_hh', SUM(portals_one_time_payment_checking),
      'portals_one_time_payment_credit_debit_card_hh', SUM(portals_one_time_payment_credit_debit_card),
      'portals_one_time_payment_credit_hh', SUM(portals_one_time_payment_credit),
      'portals_one_time_payment_savings_account_hh', SUM(portals_one_time_payment_savings_account),
      'portals_one_time_payment_savings_hh', SUM(portals_one_time_payment_savings),
      'portals_account_billing_support_views_hh', SUM(portals_account_billing_support_views)
    ) AS tmp_map

  FROM
    (
    SELECT
      portals_unique_acct_key,
      mso,
      application_name,
      autopay_status,
      billing_status,
      CAST(grouping__id AS INT)         AS grouping_id,

      IF(SUM(portals_autopay_enroll_radio_toggle) > 0, 1, 0) AS portals_autopay_enroll_radio_toggle,
      IF(SUM(portals_one_time_payment_select_amount) > 0, 1, 0) AS portals_one_time_payment_select_amount,
      IF(SUM(portals_one_time_payment_select_date) > 0, 1, 0) AS portals_one_time_payment_select_date,
      IF(SUM(portals_one_time_payment_select_payment_method) > 0, 1, 0) AS portals_one_time_payment_select_payment_method,
      IF(SUM(portals_one_time_payment_submits) > 0, 1, 0) AS portals_one_time_payment_submits,
      IF(SUM(portals_bill_pay_clicks_from_local_nav) > 0, 1, 0) AS portals_bill_pay_clicks_from_local_nav,
      IF(SUM(portals_one_time_payment_failures) > 0, 1, 0) AS portals_one_time_payment_failures,
      IF(SUM(portals_one_time_payment_starts) > 0, 1, 0) AS portals_one_time_payment_starts,
      IF(SUM(portals_one_time_payment_successes) > 0, 1, 0) AS portals_one_time_payment_successes,
      IF(SUM(portals_set_up_auto_payment_failures) > 0, 1, 0) AS portals_set_up_auto_payment_failures,
      IF(SUM(portals_set_up_auto_payment_starts) > 0, 1, 0) AS portals_set_up_auto_payment_starts,
      IF(SUM(portals_set_up_auto_payment_submits) > 0, 1, 0) AS portals_set_up_auto_payment_submits,
      IF(SUM(portals_set_up_auto_payment_successes) > 0, 1, 0) AS portals_set_up_auto_payment_successes,
      IF(SUM(portals_view_online_statments) > 0, 1, 0) AS portals_view_online_statments,
      IF(SUM(portals_set_up_auto_payment_successes_with_payment) > 0, 1, 0) AS portals_set_up_auto_payment_successes_with_payment,
      IF(SUM(portals_set_up_auto_payment_successes_without_payment) > 0, 1, 0) AS portals_set_up_auto_payment_successes_without_payment,
      IF(SUM(portals_one_time_payment_successes_with_ap_enroll) > 0, 1, 0) AS portals_one_time_payment_successes_with_ap_enroll,
      IF(SUM(portals_one_time_payment_successes_without_ap_enroll) > 0, 1, 0) AS portals_one_time_payment_successes_without_ap_enroll,
      IF(SUM(portals_billing_feature_interaction) > 0, 1, 0) AS portals_billing_feature_interaction,
      IF(SUM(portals_bill_delivery_pref_paperless_submit) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit,
      IF(SUM(portals_bill_delivery_pref_paper) > 0, 1, 0) AS portals_bill_delivery_pref_paper,
      IF(SUM(portals_bill_delivery_pref_keep_paperless) > 0, 1, 0) AS portals_bill_delivery_pref_keep_paperless,
      IF(SUM(portals_bill_delivery_pref_exit) > 0, 1, 0) AS portals_bill_delivery_pref_exit,
      IF(SUM(portals_bill_delivery_pref_manage_paperless) > 0, 1, 0) AS portals_bill_delivery_pref_manage_paperless,
      IF(SUM(portals_bill_delivery_pref_paperless_enroll_from_homepage) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_enroll_from_homepage,
      IF(SUM(portals_bill_delivery_pref_paperless_submit_from_billinghome) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit_from_billinghome,
      IF(SUM(portals_bill_delivery_pref_paperless_submit_from_homepage) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit_from_homepage,
      IF(SUM(portals_bill_delivery_pref_paperless_submit_from_paperless) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_submit_from_paperless,
      IF(SUM(portals_bill_delivery_pref_paperless_success) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_success,
      IF(SUM(portals_bill_delivery_pref_paper_success) > 0, 1, 0) AS portals_bill_delivery_pref_paper_success,
      IF(SUM(portals_bill_delivery_pref_paperless_failure) > 0, 1, 0) AS portals_bill_delivery_pref_paperless_failure,
      IF(SUM(portals_bill_delivery_pref_paper_failure) > 0, 1, 0) AS portals_bill_delivery_pref_paper_failure,
      IF(SUM(portals_one_time_payment_submission_new_payment_method) > 0, 1, 0) AS portals_one_time_payment_submission_new_payment_method,
      IF(SUM(portals_one_time_payment_submission_new_payment_method_not_stored) > 0, 1, 0) AS portals_one_time_payment_submission_new_payment_method_not_stored,
      IF(SUM(portals_one_time_payment_submission_new_payment_method_stored) > 0, 1, 0) AS portals_one_time_payment_submission_new_payment_method_stored,
      IF(SUM(portals_one_time_payment_submission_previously_stored_payment_method) > 0, 1, 0) AS portals_one_time_payment_submission_previously_stored_payment_method,
      IF(SUM(portals_one_time_payment_checking_account) > 0, 1, 0) AS portals_one_time_payment_checking_account,
      IF(SUM(portals_one_time_payment_checking) > 0, 1, 0) AS portals_one_time_payment_checking,
      IF(SUM(portals_one_time_payment_credit_debit_card) > 0, 1, 0) AS portals_one_time_payment_credit_debit_card,
      IF(SUM(portals_one_time_payment_credit) > 0, 1, 0) AS portals_one_time_payment_credit,
      IF(SUM(portals_one_time_payment_savings_account) > 0, 1, 0) AS portals_one_time_payment_savings_account,
      IF(SUM(portals_one_time_payment_savings) > 0, 1, 0) AS portals_one_time_payment_savings,
      IF(SUM(portals_account_billing_support_views) > 0, 1, 0) AS portals_account_billing_support_views

    FROM
      (
      SELECT
        mso,
        application_name,
        autopay_status,
        billing_status,
        portals_unique_acct_key,

        SUM(portals_autopay_enroll_radio_toggle) AS portals_autopay_enroll_radio_toggle,
        SUM(portals_one_time_payment_select_amount) AS portals_one_time_payment_select_amount,
        SUM(portals_one_time_payment_select_date) AS portals_one_time_payment_select_date,
        SUM(portals_one_time_payment_select_payment_method) AS portals_one_time_payment_select_payment_method,
        SUM(portals_one_time_payment_submits) AS portals_one_time_payment_submits,
        SUM(portals_bill_pay_clicks_from_local_nav) AS portals_bill_pay_clicks_from_local_nav,
        SUM(portals_one_time_payment_failures) AS portals_one_time_payment_failures,
        SUM(portals_one_time_payment_starts) AS portals_one_time_payment_starts,
        SUM(portals_one_time_payment_successes) AS portals_one_time_payment_successes,
        SUM(portals_set_up_auto_payment_failures) AS portals_set_up_auto_payment_failures,
        SUM(portals_set_up_auto_payment_starts) AS portals_set_up_auto_payment_starts,
        SUM(portals_set_up_auto_payment_submits) AS portals_set_up_auto_payment_submits,
        SUM(portals_set_up_auto_payment_successes) AS portals_set_up_auto_payment_successes,
        SUM(portals_view_online_statments) AS portals_view_online_statments,
        SUM(portals_set_up_auto_payment_successes_with_payment) AS portals_set_up_auto_payment_successes_with_payment,
        SUM(portals_set_up_auto_payment_successes_without_payment) AS portals_set_up_auto_payment_successes_without_payment,
        SUM(portals_one_time_payment_successes_with_ap_enroll) AS portals_one_time_payment_successes_with_ap_enroll,
        SUM(portals_one_time_payment_successes_without_ap_enroll) AS portals_one_time_payment_successes_without_ap_enroll,
        SUM(portals_billing_feature_interaction) AS portals_billing_feature_interaction,
        SUM(portals_bill_delivery_pref_paperless_submit) AS portals_bill_delivery_pref_paperless_submit,
        SUM(portals_bill_delivery_pref_paper) AS portals_bill_delivery_pref_paper,
        SUM(portals_bill_delivery_pref_keep_paperless) AS portals_bill_delivery_pref_keep_paperless,
        SUM(portals_bill_delivery_pref_exit) AS portals_bill_delivery_pref_exit,
        SUM(portals_bill_delivery_pref_manage_paperless) AS portals_bill_delivery_pref_manage_paperless,
        SUM(portals_bill_delivery_pref_paperless_enroll_from_homepage) AS portals_bill_delivery_pref_paperless_enroll_from_homepage,
        SUM(portals_bill_delivery_pref_paperless_submit_from_billinghome) AS portals_bill_delivery_pref_paperless_submit_from_billinghome,
        SUM(portals_bill_delivery_pref_paperless_submit_from_homepage) AS portals_bill_delivery_pref_paperless_submit_from_homepage,
        SUM(portals_bill_delivery_pref_paperless_submit_from_paperless) AS portals_bill_delivery_pref_paperless_submit_from_paperless,
        SUM(portals_bill_delivery_pref_paperless_success) AS portals_bill_delivery_pref_paperless_success,
        SUM(portals_bill_delivery_pref_paper_success) AS portals_bill_delivery_pref_paper_success,
        SUM(portals_bill_delivery_pref_paperless_failure) AS portals_bill_delivery_pref_paperless_failure,
        SUM(portals_bill_delivery_pref_paper_failure) AS portals_bill_delivery_pref_paper_failure,
        SUM(portals_one_time_payment_submission_new_payment_method) AS portals_one_time_payment_submission_new_payment_method,
        SUM(portals_one_time_payment_submission_new_payment_method_not_stored) AS portals_one_time_payment_submission_new_payment_method_not_stored,
        SUM(portals_one_time_payment_submission_new_payment_method_stored) AS portals_one_time_payment_submission_new_payment_method_stored,
        SUM(portals_one_time_payment_submission_previously_stored_payment_method) AS portals_one_time_payment_submission_previously_stored_payment_method,
        SUM(portals_one_time_payment_checking_account) AS portals_one_time_payment_checking_account,
        SUM(portals_one_time_payment_checking) AS portals_one_time_payment_checking,
        SUM(portals_one_time_payment_credit_debit_card) AS portals_one_time_payment_credit_debit_card,
        SUM(portals_one_time_payment_credit) AS portals_one_time_payment_credit,
        SUM(portals_one_time_payment_savings_account) AS portals_one_time_payment_savings_account,
        SUM(portals_one_time_payment_savings) AS portals_one_time_payment_savings,
        SUM(portals_account_billing_support_views) AS portals_account_billing_support_views

      FROM asp_quantum_metric_agg_v
      WHERE (denver_date >= ("${env:START_DATE}") AND denver_date < ("${env:END_DATE}"))
      GROUP BY
        application_name,
        mso,
        autopay_status,
        billing_status,
        portals_unique_acct_key
      ) sumfirst
    GROUP BY
      portals_unique_acct_key,
      application_name,
      mso,
      autopay_status,
      billing_status
    GROUPING SETS (
      (portals_unique_acct_key),
      (portals_unique_acct_key, application_name),
      (portals_unique_acct_key, application_name, mso, autopay_status, billing_status))
    ) sets
  GROUP BY
    '${env:label_date_denver}',
    application_name,
    mso,
    autopay_status,
    billing_status,
    grouping_id
  ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;
