USE ${env:DASP_db};

set hive.auto.convert.join=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
set hive.support.concurrency=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- accounts metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
INSERT INTO ${env:TMP_db}.m2dot0_set_agg_portals_stage_accounts_${hiveconf:execid}_${hiveconf:stepid}
SELECT
CASE WHEN (grouping_id & 1024) = 0 THEN visit__application_details__application_name ELSE 'All apps' END AS visit__application_details__application_name,
CASE WHEN (grouping_id & 512) = 0 THEN visit__application_details__application_type ELSE 'All app types' END AS visit__application_details__application_type,
CASE WHEN (grouping_id & 256) = 0 THEN visit__application_details__app_version ELSE 'All app versions' END AS visit__application_details__app_version,
CASE WHEN (grouping_id & 128) = 0 THEN CUSTOM_visit__application_details__app_version ELSE 'All app versions' END AS CUSTOM_visit__application_details__app_version,
CASE WHEN (grouping_id & 64) = 0 THEN agg_custom_visit__account__details__service_subscriptions ELSE 'All services' END AS agg_custom_visit__account__details__service_subscriptions,
CASE WHEN (grouping_id & 32) = 0 THEN agg_custom_visit__account__details__service_subscriptions_other ELSE 'All services' END AS agg_custom_visit__account__details__service_subscriptions_other,
CASE WHEN (grouping_id & 16) = 0 THEN agg_custom_visit__account__details__service_subscriptions_core ELSE 'All services' END AS agg_custom_visit__account__details__service_subscriptions_core,
CASE WHEN (grouping_id & 8) = 0 THEN agg_custom_visit__account__details__service_subscriptions_mobile ELSE 'All services' END AS agg_custom_visit__account__details__service_subscriptions_mobile,
CASE WHEN (grouping_id & 4) = 0 THEN agg_custom_customer_group else 'All Customer Groups' end as agg_custom_customer_group,
CASE WHEN (grouping_id & 2) = 0 THEN agg_custom_visit__account__configuration_factors ELSE 'All migration status' END AS agg_custom_visit__account__configuration_factors,
CASE WHEN (grouping_id & 1) = 0 THEN agg_visit__account__configuration_factors ELSE 'All migration status' END AS agg_visit__account__configuration_factors,
  grouping_id,
  metric_name,
  metric_value,
  '${hiveconf:ProcessTimestamp}' as process_date_time_denver,
  '${hiveconf:ProcessUser}' AS process_identity,
  unit_type,
  IF('accounts'='visits',cast(metric_calls as string),cast(NULL as string)) as call_count_24h,
  partition_date_utc,
  '${hiveconf:grain}' AS grain
FROM
  (
  SELECT
    partition_date_utc,
    visit__application_details__application_name, -- 1024
    visit__application_details__application_type, -- 512
    visit__application_details__app_version, -- 256
    CUSTOM_visit__application_details__app_version, --128
    agg_custom_visit__account__details__service_subscriptions, -- 64
    agg_custom_visit__account__details__service_subscriptions_other, -- 32
    agg_custom_visit__account__details__service_subscriptions_core, -- 16
    agg_custom_visit__account__details__service_subscriptions_mobile, -- 8
    agg_custom_customer_group, -- 4
    agg_custom_visit__account__configuration_factors, -- 2
    agg_visit__account__configuration_factors, -- 1
    unit_type,
    grouping_id,
      MAP(
        --------------------------------------------------------------------------------
        --------------------------The Rates---------------------------
        --------------------------------------------------------------------------------
        'Activation Success Rate', sum(mobile_activation_activation_complete)/sum(mobile_activation_activation_in_progress + mobile_activation_transfer_in_progress),
        'AutoPay Enroll Flow Completion Rate (Core)', SUM(AP_enroll_success_core)/sum(AP_enroll_start_core),
        'AutoPay Enroll Attempt Success Rate (Core)', sum(ap_enroll_success_core)/SUM(ap_enroll_success_core + AP_enroll_failure_core),
        'Device Payment Flow Completion Rate', SUM(device_make_payment_success)/SUM(device_make_payment_start),
        'Device Payment Submit Success Rate', SUM(device_make_payment_success)/SUM(device_make_payment_submit),
        'Device Payment Attempt Success Rate', SUM(device_make_payment_success)/SUM(device_make_payment_success + device_make_payment_failure),
        'Device Upgrade Starts', SUM(mobile_device_upgrade),
        'Login Success Rate', sum(login_success)/sum(login_attempt),
        'OTP Flow Completion Rate (Core)', SUM(OTP_success_core + OTP_success_core_full + OTP_success_core_quickPay + OTP_success_legacy)/sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
        'OTP Flow Completion Rate (Mobile)', sum(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile)/sum(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
        'OTP Submit Success Rate (Core)', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy)/SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
        'OTP Submit Success Rate (Mobile)', sum(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile)/SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
        'OTP Attempt Success Rate (Core)', SUM(OTP_success_core + OTP_success_core_full + OTP_success_core_quickPay + OTP_success_legacy)/
                                          SUM(OTP_success_core + OTP_success_core_full + OTP_success_core_quickPay + OTP_success_legacy + OTP_failure_core + OTP_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
        'OTP Attempt Success Rate (Mobile)', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay)/
                                            SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),
        'Page Load Time (Home)', sum(page_load_home - page_load_home_hot_3000)/sum(page_load_home),
        'Page Load Time (Billing)', SUM(page_load_billing - page_load_billing_hot_3000)/sum(page_load_billing),
        'CPNI Flow Completion Rate', SUM(CPNI_success)/SUM(CPNI_start + CPNI_start_modalView + CPNI_start_pageView),
        'CPNI Submit Success Rate', sum(CPNI_success)/SUM(CPNI_submit),
        'CPNI Attempt Success Rate', SUM(CPNI_success)/sum(CPNI_success + CPNI_failure),
        'Mobile Change Plan Flow Completion Rate', sum(mobile_tier_upgrade_success + mobile_success_change_plan)/SUM(mobile_tier_upgrade_start + mobile_change_plan),
        'Mobile Change Plan Submit Success Rate', sum(mobile_tier_upgrade_success + mobile_success_change_plan)/SUM(mobile_confirm_change_plan + mobile_tier_upgrade_submit),
        -- 'CPNI Submit Success Rate (IMEI)', sum(CPNI_success_imei)/sum(CPNI_submit_imei),
        -- 'CPNI Attempt Success Rate (IMEI)', SUM(CPNI_success_imei)/sum(CPNI_success_imei + CPNI_failure_IMEI),
        -- 'CPNI Submit Success Rate (MAC Address)', sum(CPNI_success_mac_address)/sum(CPNI_submit_mac_address),
        -- 'CPNI Attempt Success Rate (Mac Address)', SUM(CPNI_success_mac_address)/SUM(CPNI_success_mac_address + CPNI_failure_mac_address),
        -- 'CPNI Submit Success Rate (Email)', sum(CPNI_success_email)/sum(CPNI_submit_email),
        -- 'CPNI Attempt Success Rate (Email)', SUM(CPNI_success_email)/sum(CPNI_success_email + CPNI_failure_email),
        -- 'CPNI Submit Success Rate (Security Code)', sum(CPNI_success_security_code)/sum(CPNI_submit_security_code),
        -- 'CPNI Attempt Success Rate (Security Code)', sum(CPNI_success_security_code)/sum(CPNI_success_security_code + CPNI_failure_security_code),

        --------------------------------------------------------------------------------
        -------------------------- The Components of Reporting Metrics-------------------
        --------------------------------------------------------------------------------
        'CPNI Flow Completion - Starts', sum(cpni_start + CPNI_start_pageView + CPNI_start_modalView),
        'CPNI Flow Completion - Submits', sum(CPNI_submit),
        'CPNI Flow Completion - Successes', sum(CPNI_success),
        'CPNI Flow Completion - Failures', sum(CPNI_failure),
        'CPNI Flow Completion - Attempts', sum(CPNI_success + CPNI_failure),
        'CPNI Submit Success - Starts', sum(cpni_start + CPNI_start_pageView + CPNI_start_modalView),
        'CPNI Submit Success - Submits', sum(CPNI_submit),
        'CPNI Submit Success - Successes', sum(CPNI_success),
        'CPNI Submit Success - Failures', sum(CPNI_failure),
        'CPNI Submit Success - Attempts', sum(CPNI_success + CPNI_failure),
        'CPNI Attempt Success - Starts', sum(cpni_start + CPNI_start_pageView + CPNI_start_modalView),
        'CPNI Attempt Success - Submits', sum(CPNI_submit),
        'CPNI Attempt Success - Successes', sum(CPNI_success),
        'CPNI Attempt Success - Failures', sum(CPNI_failure),
        'CPNI Attempt Success - Attempts', sum(CPNI_success + CPNI_failure),
        'Activation Successes', sum(mobile_activation_activation_complete),
        'Activation Starts', SUm(mobile_activation_activation_in_progress),
        'Activation Failures', SUM(mobile_activation_unable_to_activate),
        'AutoPay Enroll Flow (Core) - Starts', SUM(ap_enroll_start_core),
        'AutoPay Enroll Flow (Core) - Successes', SUM(AP_enroll_success_core),
        'AutoPay Enroll Flow (Core) - Failures', SUM(ap_enroll_failure_core),
        'AutoPay Enroll Flow (Core) - Attempts', SUM(ap_enroll_success_core + ap_enroll_failure_core),
        'AutoPay Enroll Attempt (Core) - Starts', sum(ap_enroll_start_core),
        'AutoPay Enroll Attempt (Core) - Successes', sum(AP_enroll_success_core),
        'AutoPay Enroll Attempt (Core) - Failures', sum(ap_enroll_failure_core),
        'AutoPay Enroll Attempt (Core) - Attempts', SUM(ap_enroll_failure_core + AP_enroll_success_core),
        'Device Payment Flow Completion - Successes', sum(device_make_payment_success),
        'Device Payment Flow Completion - Starts', sum(device_make_payment_start),
        'Device Payment Flow Completion - Failures', SUM(device_make_payment_failure),
        'Device Payment Flow Completion - Attempts', sum(device_make_payment_failure + device_make_payment_success),
        'Device Payment Flow Completion - Submit', sum(device_make_payment_submit),
        'Device Payment Submit Success - Successes', sum(device_make_payment_success),
        'Device Payment Submit Success - Starts', sum(device_make_payment_start),
        'Device Payment Submit Success - Failures', SUM(device_make_payment_failure),
        'Device Payment Submit Success - Attempts', sum(device_make_payment_failure + device_make_payment_success),
        'Device Payment Submit Success - Submits', sum(device_make_payment_submit),
        'Device Payment Attempt Success - Successes', sum(device_make_payment_success),
        'Device Payment Attempt Success - Starts', sum(device_make_payment_start),
        'Device Payment Attempt Success - Failures', SUM(device_make_payment_failure),
        'Device Payment Attempt Success - Attempts', sum(device_make_payment_failure + device_make_payment_success),
        'Device Payment Attempt Success - Submit', sum(device_make_payment_submit),
        'Login Successes', sum(login_success),
        'Login Failures', sum(login_failure),
        'Login Attempts', sum(login_success + login_failure),
        'Mobile Change Plan Flow Completion - Successes', sum(mobile_tier_upgrade_success + mobile_success_change_plan),
        'Mobile Change Plan Flow Completion - Submits', SUM(mobile_confirm_change_plan + mobile_tier_upgrade_submit),
        'Mobile Change Plan Flow Completion - Starts', sum(mobile_tier_upgrade_start + mobile_change_plan),
        'Mobile Change Plan Submit Success Rate - Successes', sum(mobile_tier_upgrade_success + mobile_success_change_plan),
        'Mobile Change Plan Submit Success Rate - Submits', SUM(mobile_confirm_change_plan + mobile_tier_upgrade_submit),
        'Mobile Change Plan Submit Success Rate - Starts', sum(mobile_tier_upgrade_start + mobile_change_plan),

        'OTP Flow Completion (Core) - Successes', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy),
        'OTP Flow Completion (Core) - Starts', sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
        'OTP Flow Completion (Core) - Failures', SUM(otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
        'OTP Flow Completion (Core) - Submits', SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
        'OTP Flow Completion (Core) - Attempts', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy + otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),

        'OTP Attempt Success Rate (Core) - Successes',  sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy),
        'OTP Attempt Success Rate (Core) - Starts', sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
        'OTP Attempt Success Rate (Core) - Failures', SUM(otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
        'OTP Attempt Success Rate (Core) - Submits', SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
        'OTP Attempt Success Rate (Core) - Attempts', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy + otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),

        'OTP Submit Success Rate (Core) - Successes', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy),
        'OTP Submit Success Rate (Core) - Starts', sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
        'OTP Submit Success Rate (Core) - Failures', SUM(otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
        'OTP Submit Success Rate (Core) - Submits', SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
        'OTP Submit Success Rate (Core) - Attempts', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy + otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay),

        'OTP Flow Completion (Mobile) - Successes', SUM(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile),
        'OTP Flow Completion (Mobile) - Starts', SUM(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
        'OTP Flow Completion (Mobile) - Failures', SUM(otp_failure_mobile + otp_failure_mobile_full + OTP_failure_mobile_quickPay),
        'OTP Flow Completion (Mobile) - Submits', SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
        'OTP Flow Completion (Mobile) - Attempts', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),

        'OTP Attempt Success Rate (Mobile) - Successes', SUM(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile),
        'OTP Attempt Success Rate (Mobile) - Starts', SUM(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
        'OTP Attempt Success Rate (Mobile) - Failures', SUM(otp_failure_mobile + otp_failure_mobile_full + OTP_failure_mobile_quickPay),
        'OTP Attempt Success Rate (Mobile) - Submits', SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
        'OTP Attempt Success Rate (Mobile) - Attempts', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),

        'OTP Submit Success Rate (Mobile) - Successes', SUM(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile),
        'OTP Submit Success Rate (Mobile) - Starts', SUM(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
        'OTP Submit Success Rate (Mobile) - Failures', SUM(otp_failure_mobile + otp_failure_mobile_full + OTP_failure_mobile_quickPay),
        'OTP Submit Success Rate (Mobile) - Submits', SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
        'OTP Submit Success Rate (Mobile) - Attempts', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),

        'Page Views < 3 secs (Home)', sum(page_load_home - page_load_home_hot_3000),
        'Page Views >= 3 secs (Home)', sum(page_load_home_hot_3000),
        'Page Views (Home)', sum(page_load_home),
        'Page Views < 3 secs (Billing)', sum(page_load_billing - page_load_billing_hot_3000),
        'Page Views >= 3 secs (Billing)', sum(page_load_billing_hot_3000),
        'Page Views (Billing)', sum(page_load_billing),
        --------------------------------------------------------------------------------
        --------------------------------------------------------------------------------
        --------------------------------------------------------------------------------
     'AP_enroll_abandon_core', SUM(AP_enroll_abandon_core),
     'AP_enroll_failure_core', SUM(AP_enroll_failure_core),
     'AP_enroll_start_core', SUM(AP_enroll_start_core),
     'AP_enroll_submit_core', SUM(AP_enroll_submit_core),
     'AP_enroll_success_core', SUM(AP_enroll_success_core),
     'AP_manage_failure', SUM(AP_manage_failure),
     'AP_manage_start', SUM(AP_manage_start),
     'AP_manage_success', SUM(AP_manage_success),
     'CPNI_cancel', SUM(CPNI_cancel),
     'CPNI_failure', SUM(CPNI_failure),
     'CPNI_failure_IMEI', SUM(CPNI_failure_IMEI),
     'CPNI_failure_email', SUM(CPNI_failure_email),
     'CPNI_failure_mac_address', SUM(CPNI_failure_mac_address),
     'CPNI_failure_security_code', SUM(CPNI_failure_security_code),
     'CPNI_start', SUM(CPNI_start),
     'CPNI_start_modalView', SUM(CPNI_start_modalView),
     'CPNI_start_pageView', SUM(CPNI_start_pageView),
     'CPNI_submit', SUM(CPNI_submit),
     'CPNI_submit_IMEI', SUM(CPNI_submit_IMEI),
     'CPNI_submit_email', SUM(CPNI_submit_email),
     'CPNI_submit_mac_address', SUM(CPNI_submit_mac_address),
     'CPNI_submit_security_code', SUM(CPNI_submit_security_code),
     'CPNI_success', SUM(CPNI_success),
     'CPNI_success_IMEI', SUM(CPNI_success_IMEI),
     'CPNI_success_email', SUM(CPNI_success_email),
     'CPNI_success_mac_address', SUM(CPNI_success_mac_address),
     'CPNI_success_security_code', SUM(CPNI_success_security_code),
     'OTP_failure_core', SUM(OTP_failure_core),
     'OTP_failure_core_full', SUM(OTP_failure_core_full),
     'OTP_failure_core_quickPay', SUM(OTP_failure_core_quickPay),
     'OTP_failure_legacy', SUM(OTP_failure_legacy),
     'OTP_failure_mobile', SUM(OTP_failure_mobile),
     'OTP_failure_mobile_full', SUM(OTP_failure_mobile_full),
     'OTP_failure_mobile_quickPay', SUM(OTP_failure_mobile_quickPay),
     'OTP_start_core', SUM(OTP_start_core),
     'OTP_start_core_full', SUM(OTP_start_core_full),
     'OTP_start_core_quickPay', SUM(OTP_start_core_quickPay),
     'OTP_start_legacy', SUM(OTP_start_legacy),
     'OTP_start_mobile', SUM(OTP_start_mobile),
     'OTP_start_mobile_full', SUM(OTP_start_mobile_full),
     'OTP_start_mobile_quickPay', SUM(OTP_start_mobile_quickPay),
     'OTP_start_modal', SUM(OTP_start_modal),
     'OTP_submit_core', SUM(OTP_submit_core),
     'OTP_submit_core_full', SUM(OTP_submit_core_full),
     'OTP_submit_core_quickPay', SUM(OTP_submit_core_quickPay),
     'OTP_submit_legacy', SUM(OTP_submit_legacy),
     'OTP_submit_mobile', SUM(OTP_submit_mobile),
     'OTP_submit_mobile_full', SUM(OTP_submit_mobile_full),
     'OTP_submit_mobile_quickPay', SUM(OTP_submit_mobile_quickPay),
     'OTP_success_core', SUM(OTP_success_core),
     'OTP_success_core_full', SUM(OTP_success_core_full),
     'OTP_success_core_quickPay', SUM(OTP_success_core_quickPay),
     'OTP_success_legacy', SUM(OTP_success_legacy),
     'OTP_success_mobile', SUM(OTP_success_mobile),
     'OTP_success_mobile_full', SUM(OTP_success_mobile_full),
     'OTP_success_mobile_quickPay', SUM(OTP_success_mobile_quickPay),
     'device_make_payment_failure', SUM(device_make_payment_failure),
     'device_make_payment_start', SUM(device_make_payment_start),
     'device_make_payment_submit', SUM(device_make_payment_submit),
     'device_make_payment_success', SUM(device_make_payment_success),
     'login_attempt', SUM(login_attempt),
     'login_failure', SUM(login_failure),
     'login_success', SUM(login_success),
     'mobile_activation_activation_complete', SUM(mobile_activation_activation_complete),
     'mobile_activation_activation_in_progress', SUM(mobile_activation_activation_in_progress),
     'mobile_activation_transfer_complete', SUM(mobile_activation_transfer_complete),
     'mobile_activation_transfer_in_progress', SUM(mobile_activation_transfer_in_progress),
     'mobile_activation_unable_to_activate', SUM(mobile_activation_unable_to_activate),
     'mobile_activation_unable_to_transfer', SUM(mobile_activation_unable_to_transfer),
     'mobile_change_plan', SUM(mobile_change_plan),
     'mobile_confirm_change_plan', SUM(mobile_confirm_change_plan),
     'mobile_device_upgrade', SUM(mobile_device_upgrade),
     'mobile_success_change_plan', SUM(mobile_success_change_plan),
     'mobile_tier_upgrade_review', SUM(mobile_tier_upgrade_review),
     'mobile_tier_upgrade_start', SUM(mobile_tier_upgrade_start),
     'mobile_tier_upgrade_submit', SUM(mobile_tier_upgrade_submit),
     'mobile_tier_upgrade_success', SUM(mobile_tier_upgrade_success),
     'page_load_billing', SUM(page_load_billing),
     'page_load_billing_hot_2000', SUM(page_load_billing_hot_2000),
     'page_load_billing_hot_3000', SUM(page_load_billing_hot_3000),
     'page_load_home', SUM(page_load_home),
     'page_load_home_hot_2000', SUM(page_load_home_hot_2000),
     'page_load_home_hot_3000', SUM(page_load_home_hot_3000),
     'payment_method_add_failure_mobile', SUM(payment_method_add_failure_mobile),
     'payment_method_add_success_mobile', SUM(payment_method_add_success_mobile),
     'payment_method_delete_failure_mobile', SUM(payment_method_delete_failure_mobile),
     'payment_method_delete_success_mobile', SUM(payment_method_delete_success_mobile),
     'payment_method_edit_failure_mobile', SUM(payment_method_edit_failure_mobile),
     'payment_method_edit_success_mobile', SUM(payment_method_edit_success_mobile),
     'portals_csat_submit_rating_1_very_dissatisfied', SUM(portals_csat_submit_rating_1_very_dissatisfied),
     'portals_csat_submit_rating_2_dissatisfied', SUM(portals_csat_submit_rating_2_dissatisfied),
     'portals_csat_submit_rating_3_neutral', SUM(portals_csat_submit_rating_3_neutral),
     'portals_csat_submit_rating_4_satisfied', SUM(portals_csat_submit_rating_4_satisfied),
     'portals_csat_submit_rating_5_very_satisfied', SUM(portals_csat_submit_rating_5_very_satisfied)
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

) AS tmp_map,

MAP(
  --------------------------------------------------------------------------------
  --------------------------The Rates---------------------------
  --------------------------------------------------------------------------------
  'Activation Success Rate', sum(mobile_activation_activation_complete)/sum(mobile_activation_activation_in_progress + mobile_activation_transfer_in_progress),
  'AutoPay Enroll Flow Completion Rate (Core)', SUM(AP_enroll_success_core)/sum(AP_enroll_start_core),
  'AutoPay Enroll Attempt Success Rate (Core)', sum(ap_enroll_success_core)/SUM(ap_enroll_success_core + AP_enroll_failure_core),
  'Device Payment Flow Completion Rate', SUM(device_make_payment_success)/SUM(device_make_payment_start),
  'Device Payment Submit Success Rate', SUM(device_make_payment_success)/SUM(device_make_payment_submit),
  'Device Payment Attempt Success Rate', SUM(device_make_payment_success)/SUM(device_make_payment_success + device_make_payment_failure),
  'Device Upgrade Starts', SUM(mobile_device_upgrade),
  'Login Success Rate', sum(login_success)/sum(login_attempt),
  'OTP Flow Completion Rate (Core)', SUM(OTP_success_core + OTP_success_core_full + OTP_success_core_quickPay + OTP_success_legacy)/sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
  'OTP Flow Completion Rate (Mobile)', sum(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile)/sum(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
  'OTP Submit Success Rate (Core)', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy)/SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
  'OTP Submit Success Rate (Mobile)', sum(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile)/SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
  'OTP Attempt Success Rate (Core)', SUM(OTP_success_core + OTP_success_core_full + OTP_success_core_quickPay + OTP_success_legacy)/
                                    SUM(OTP_success_core + OTP_success_core_full + OTP_success_core_quickPay + OTP_success_legacy + OTP_failure_core + OTP_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
  'OTP Attempt Success Rate (Mobile)', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay)/
                                      SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),
  'Page Load Time (Home)', sum(page_load_home - page_load_home_hot_3000)/sum(page_load_home),
  'Page Load Time (Billing)', SUM(page_load_billing - page_load_billing_hot_3000)/sum(page_load_billing),
  'CPNI Flow Completion Rate', SUM(CPNI_success)/SUM(CPNI_start + CPNI_start_modalView + CPNI_start_pageView),
  'CPNI Submit Success Rate', sum(CPNI_success)/SUM(CPNI_submit),
  'CPNI Attempt Success Rate', SUM(CPNI_success)/sum(CPNI_success + CPNI_failure),
  'Mobile Change Plan Flow Completion Rate', sum(mobile_tier_upgrade_success + mobile_success_change_plan)/SUM(mobile_tier_upgrade_start + mobile_change_plan),
  'Mobile Change Plan Submit Success Rate', sum(mobile_tier_upgrade_success + mobile_success_change_plan)/SUM(mobile_confirm_change_plan + mobile_tier_upgrade_submit),
  -- 'CPNI Submit Success Rate (IMEI)', sum(CPNI_success_imei)/sum(CPNI_submit_imei),
  -- 'CPNI Attempt Success Rate (IMEI)', SUM(CPNI_success_imei)/sum(CPNI_success_imei + CPNI_failure_IMEI),
  -- 'CPNI Submit Success Rate (MAC Address)', sum(CPNI_success_mac_address)/sum(CPNI_submit_mac_address),
  -- 'CPNI Attempt Success Rate (Mac Address)', SUM(CPNI_success_mac_address)/SUM(CPNI_success_mac_address + CPNI_failure_mac_address),
  -- 'CPNI Submit Success Rate (Email)', sum(CPNI_success_email)/sum(CPNI_submit_email),
  -- 'CPNI Attempt Success Rate (Email)', SUM(CPNI_success_email)/sum(CPNI_success_email + CPNI_failure_email),
  -- 'CPNI Submit Success Rate (Security Code)', sum(CPNI_success_security_code)/sum(CPNI_submit_security_code),
  -- 'CPNI Attempt Success Rate (Security Code)', sum(CPNI_success_security_code)/sum(CPNI_success_security_code + CPNI_failure_security_code),

  --------------------------------------------------------------------------------
  -------------------------- The Components of Reporting Metrics-------------------
  --------------------------------------------------------------------------------
  'CPNI Flow Completion - Starts', sum(cpni_start + CPNI_start_pageView + CPNI_start_modalView),
  'CPNI Flow Completion - Submits', sum(CPNI_submit),
  'CPNI Flow Completion - Successes', sum(CPNI_success),
  'CPNI Flow Completion - Failures', sum(CPNI_failure),
  'CPNI Flow Completion - Attempts', sum(CPNI_success + CPNI_failure),
  'CPNI Submit Success - Starts', sum(cpni_start + CPNI_start_pageView + CPNI_start_modalView),
  'CPNI Submit Success - Submits', sum(CPNI_submit),
  'CPNI Submit Success - Successes', sum(CPNI_success),
  'CPNI Submit Success - Failures', sum(CPNI_failure),
  'CPNI Submit Success - Attempts', sum(CPNI_success + CPNI_failure),
  'CPNI Attempt Success - Starts', sum(cpni_start + CPNI_start_pageView + CPNI_start_modalView),
  'CPNI Attempt Success - Submits', sum(CPNI_submit),
  'CPNI Attempt Success - Successes', sum(CPNI_success),
  'CPNI Attempt Success - Failures', sum(CPNI_failure),
  'CPNI Attempt Success - Attempts', sum(CPNI_success + CPNI_failure),
  'Activation Successes', sum(mobile_activation_activation_complete),
  'Activation Starts', SUm(mobile_activation_activation_in_progress),
  'Activation Failures', SUM(mobile_activation_unable_to_activate),
  'AutoPay Enroll Flow (Core) - Starts', SUM(ap_enroll_start_core),
  'AutoPay Enroll Flow (Core) - Successes', SUM(AP_enroll_success_core),
  'AutoPay Enroll Flow (Core) - Failures', SUM(ap_enroll_failure_core),
  'AutoPay Enroll Flow (Core) - Attempts', SUM(ap_enroll_success_core + ap_enroll_failure_core),
  'AutoPay Enroll Attempt (Core) - Starts', sum(ap_enroll_start_core),
  'AutoPay Enroll Attempt (Core) - Successes', sum(AP_enroll_success_core),
  'AutoPay Enroll Attempt (Core) - Failures', sum(ap_enroll_failure_core),
  'AutoPay Enroll Attempt (Core) - Attempts', SUM(ap_enroll_failure_core + AP_enroll_success_core),
  'Device Payment Flow Completion - Successes', sum(device_make_payment_success),
  'Device Payment Flow Completion - Starts', sum(device_make_payment_start),
  'Device Payment Flow Completion - Failures', SUM(device_make_payment_failure),
  'Device Payment Flow Completion - Attempts', sum(device_make_payment_failure + device_make_payment_success),
  'Device Payment Flow Completion - Submit', sum(device_make_payment_submit),
  'Device Payment Submit Success - Successes', sum(device_make_payment_success),
  'Device Payment Submit Success - Starts', sum(device_make_payment_start),
  'Device Payment Submit Success - Failures', SUM(device_make_payment_failure),
  'Device Payment Submit Success - Attempts', sum(device_make_payment_failure + device_make_payment_success),
  'Device Payment Submit Success - Submits', sum(device_make_payment_submit),
  'Device Payment Attempt Success - Successes', sum(device_make_payment_success),
  'Device Payment Attempt Success - Starts', sum(device_make_payment_start),
  'Device Payment Attempt Success - Failures', SUM(device_make_payment_failure),
  'Device Payment Attempt Success - Attempts', sum(device_make_payment_failure + device_make_payment_success),
  'Device Payment Attempt Success - Submit', sum(device_make_payment_submit),
  'Login Successes', sum(login_success),
  'Login Failures', sum(login_failure),
  'Login Attempts', sum(login_success + login_failure),
  'Mobile Change Plan Flow Completion - Successes', sum(mobile_tier_upgrade_success + mobile_success_change_plan),
  'Mobile Change Plan Flow Completion - Submits', SUM(mobile_confirm_change_plan + mobile_tier_upgrade_submit),
  'Mobile Change Plan Flow Completion - Starts', sum(mobile_tier_upgrade_start + mobile_change_plan),
  'Mobile Change Plan Submit Success Rate - Successes', sum(mobile_tier_upgrade_success + mobile_success_change_plan),
  'Mobile Change Plan Submit Success Rate - Submits', SUM(mobile_confirm_change_plan + mobile_tier_upgrade_submit),
  'Mobile Change Plan Submit Success Rate - Starts', sum(mobile_tier_upgrade_start + mobile_change_plan),

  'OTP Flow Completion (Core) - Successes', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy),
  'OTP Flow Completion (Core) - Starts', sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
  'OTP Flow Completion (Core) - Failures', SUM(otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
  'OTP Flow Completion (Core) - Submits', SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
  'OTP Flow Completion (Core) - Attempts', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy + otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),

  'OTP Attempt Success Rate (Core) - Successes',  sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy),
  'OTP Attempt Success Rate (Core) - Starts', sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
  'OTP Attempt Success Rate (Core) - Failures', SUM(otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
  'OTP Attempt Success Rate (Core) - Submits', SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
  'OTP Attempt Success Rate (Core) - Attempts', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy + otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),

  'OTP Submit Success Rate (Core) - Successes', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy),
  'OTP Submit Success Rate (Core) - Starts', sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
  'OTP Submit Success Rate (Core) - Failures', SUM(otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
  'OTP Submit Success Rate (Core) - Submits', SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
  'OTP Submit Success Rate (Core) - Attempts', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy + otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay),

  'OTP Flow Completion (Mobile) - Successes', SUM(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile),
  'OTP Flow Completion (Mobile) - Starts', SUM(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
  'OTP Flow Completion (Mobile) - Failures', SUM(otp_failure_mobile + otp_failure_mobile_full + OTP_failure_mobile_quickPay),
  'OTP Flow Completion (Mobile) - Submits', SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
  'OTP Flow Completion (Mobile) - Attempts', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),

  'OTP Attempt Success Rate (Mobile) - Successes', SUM(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile),
  'OTP Attempt Success Rate (Mobile) - Starts', SUM(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
  'OTP Attempt Success Rate (Mobile) - Failures', SUM(otp_failure_mobile + otp_failure_mobile_full + OTP_failure_mobile_quickPay),
  'OTP Attempt Success Rate (Mobile) - Submits', SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
  'OTP Attempt Success Rate (Mobile) - Attempts', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),

  'OTP Submit Success Rate (Mobile) - Successes', SUM(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile),
  'OTP Submit Success Rate (Mobile) - Starts', SUM(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
  'OTP Submit Success Rate (Mobile) - Failures', SUM(otp_failure_mobile + otp_failure_mobile_full + OTP_failure_mobile_quickPay),
  'OTP Submit Success Rate (Mobile) - Submits', SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
  'OTP Submit Success Rate (Mobile) - Attempts', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),

  'Page Views < 3 secs (Home)', sum(page_load_home - page_load_home_hot_3000),
  'Page Views >= 3 secs (Home)', sum(page_load_home_hot_3000),
  'Page Views (Home)', sum(page_load_home),
  'Page Views < 3 secs (Billing)', sum(page_load_billing - page_load_billing_hot_3000),
  'Page Views >= 3 secs (Billing)', sum(page_load_billing_hot_3000),
  'Page Views (Billing)', sum(page_load_billing),
  --------------------------------------------------------------------------------
  --------------------------------------------------------------------------------
  --------------------------------------------------------------------------------
     'AP_enroll_abandon_core', SUM(AP_enroll_abandon_core_calls),
     'AP_enroll_failure_core', SUM(AP_enroll_failure_core_calls),
     'AP_enroll_start_core', SUM(AP_enroll_start_core_calls),
     'AP_enroll_submit_core', SUM(AP_enroll_submit_core_calls),
     'AP_enroll_success_core', SUM(AP_enroll_success_core_calls),
     'AP_manage_failure', SUM(AP_manage_failure_calls),
     'AP_manage_start', SUM(AP_manage_start_calls),
     'AP_manage_success', SUM(AP_manage_success_calls),
     'CPNI_cancel', SUM(CPNI_cancel_calls),
     'CPNI_failure', SUM(CPNI_failure_calls),
     'CPNI_failure_IMEI', SUM(CPNI_failure_IMEI_calls),
     'CPNI_failure_email', SUM(CPNI_failure_email_calls),
     'CPNI_failure_mac_address', SUM(CPNI_failure_mac_address_calls),
     'CPNI_failure_security_code', SUM(CPNI_failure_security_code_calls),
     'CPNI_start', SUM(CPNI_start_calls),
     'CPNI_start_modalView', SUM(CPNI_start_modalView_calls),
     'CPNI_start_pageView', SUM(CPNI_start_pageView_calls),
     'CPNI_submit', SUM(CPNI_submit_calls),
     'CPNI_submit_IMEI', SUM(CPNI_submit_IMEI_calls),
     'CPNI_submit_email', SUM(CPNI_submit_email_calls),
     'CPNI_submit_mac_address', SUM(CPNI_submit_mac_address_calls),
     'CPNI_submit_security_code', SUM(CPNI_submit_security_code_calls),
     'CPNI_success', SUM(CPNI_success_calls),
     'CPNI_success_IMEI', SUM(CPNI_success_IMEI_calls),
     'CPNI_success_email', SUM(CPNI_success_email_calls),
     'CPNI_success_mac_address', SUM(CPNI_success_mac_address_calls),
     'CPNI_success_security_code', SUM(CPNI_success_security_code_calls),
     'OTP_failure_core', SUM(OTP_failure_core_calls),
     'OTP_failure_core_full', SUM(OTP_failure_core_full_calls),
     'OTP_failure_core_quickPay', SUM(OTP_failure_core_quickPay_calls),
     'OTP_failure_legacy', SUM(OTP_failure_legacy_calls),
     'OTP_failure_mobile', SUM(OTP_failure_mobile_calls),
     'OTP_failure_mobile_full', SUM(OTP_failure_mobile_full_calls),
     'OTP_failure_mobile_quickPay', SUM(OTP_failure_mobile_quickPay_calls),
     'OTP_start_core', SUM(OTP_start_core_calls),
     'OTP_start_core_full', SUM(OTP_start_core_full_calls),
     'OTP_start_core_quickPay', SUM(OTP_start_core_quickPay_calls),
     'OTP_start_legacy', SUM(OTP_start_legacy_calls),
     'OTP_start_mobile', SUM(OTP_start_mobile_calls),
     'OTP_start_mobile_full', SUM(OTP_start_mobile_full_calls),
     'OTP_start_mobile_quickPay', SUM(OTP_start_mobile_quickPay_calls),
     'OTP_start_modal', SUM(OTP_start_modal_calls),
     'OTP_submit_core', SUM(OTP_submit_core_calls),
     'OTP_submit_core_full', SUM(OTP_submit_core_full_calls),
     'OTP_submit_core_quickPay', SUM(OTP_submit_core_quickPay_calls),
     'OTP_submit_legacy', SUM(OTP_submit_legacy_calls),
     'OTP_submit_mobile', SUM(OTP_submit_mobile_calls),
     'OTP_submit_mobile_full', SUM(OTP_submit_mobile_full_calls),
     'OTP_submit_mobile_quickPay', SUM(OTP_submit_mobile_quickPay_calls),
     'OTP_success_core', SUM(OTP_success_core_calls),
     'OTP_success_core_full', SUM(OTP_success_core_full_calls),
     'OTP_success_core_quickPay', SUM(OTP_success_core_quickPay_calls),
     'OTP_success_legacy', SUM(OTP_success_legacy_calls),
     'OTP_success_mobile', SUM(OTP_success_mobile_calls),
     'OTP_success_mobile_full', SUM(OTP_success_mobile_full_calls),
     'OTP_success_mobile_quickPay', SUM(OTP_success_mobile_quickPay_calls),
     'device_make_payment_failure', SUM(device_make_payment_failure_calls),
     'device_make_payment_start', SUM(device_make_payment_start_calls),
     'device_make_payment_submit', SUM(device_make_payment_submit_calls),
     'device_make_payment_success', SUM(device_make_payment_success_calls),
     'login_attempt', SUM(login_attempt_calls),
     'login_failure', SUM(login_failure_calls),
     'login_success', SUM(login_success_calls),
     'mobile_activation_activation_complete', SUM(mobile_activation_activation_complete_calls),
     'mobile_activation_activation_in_progress', SUM(mobile_activation_activation_in_progress_calls),
     'mobile_activation_transfer_complete', SUM(mobile_activation_transfer_complete_calls),
     'mobile_activation_transfer_in_progress', SUM(mobile_activation_transfer_in_progress_calls),
     'mobile_activation_unable_to_activate', SUM(mobile_activation_unable_to_activate_calls),
     'mobile_activation_unable_to_transfer', SUM(mobile_activation_unable_to_transfer_calls),
     'mobile_change_plan', SUM(mobile_change_plan_calls),
     'mobile_confirm_change_plan', SUM(mobile_confirm_change_plan_calls),
     'mobile_device_upgrade', SUM(mobile_device_upgrade_calls),
     'mobile_success_change_plan', SUM(mobile_success_change_plan_calls),
     'mobile_tier_upgrade_review', SUM(mobile_tier_upgrade_review_calls),
     'mobile_tier_upgrade_start', SUM(mobile_tier_upgrade_start_calls),
     'mobile_tier_upgrade_submit', SUM(mobile_tier_upgrade_submit_calls),
     'mobile_tier_upgrade_success', SUM(mobile_tier_upgrade_success_calls),
     'page_load_billing', SUM(page_load_billing_calls),
     'page_load_billing_hot_2000', SUM(page_load_billing_hot_2000_calls),
     'page_load_billing_hot_3000', SUM(page_load_billing_hot_3000_calls),
     'page_load_home', SUM(page_load_home_calls),
     'page_load_home_hot_2000', SUM(page_load_home_hot_2000_calls),
     'page_load_home_hot_3000', SUM(page_load_home_hot_3000_calls),
     'payment_method_add_failure_mobile', SUM(payment_method_add_failure_mobile_calls),
     'payment_method_add_success_mobile', SUM(payment_method_add_success_mobile_calls),
     'payment_method_delete_failure_mobile', SUM(payment_method_delete_failure_mobile_calls),
     'payment_method_delete_success_mobile', SUM(payment_method_delete_success_mobile_calls),
     'payment_method_edit_failure_mobile', SUM(payment_method_edit_failure_mobile_calls),
     'payment_method_edit_success_mobile', SUM(payment_method_edit_success_mobile_calls),
     'portals_csat_submit_rating_1_very_dissatisfied', SUM(portals_csat_submit_rating_1_very_dissatisfied_calls),
     'portals_csat_submit_rating_2_dissatisfied', SUM(portals_csat_submit_rating_2_dissatisfied_calls),
     'portals_csat_submit_rating_3_neutral', SUM(portals_csat_submit_rating_3_neutral_calls),
     'portals_csat_submit_rating_4_satisfied', SUM(portals_csat_submit_rating_4_satisfied_calls),
     'portals_csat_submit_rating_5_very_satisfied', SUM(portals_csat_submit_rating_5_very_satisfied_calls)

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

) AS call_map
FROM
  (
  SELECT
  partition_date_utc,
  unit_identifier,
  visit__application_details__application_name, -- 1024
  visit__application_details__application_type, -- 512
  visit__application_details__app_version, -- 256
  CUSTOM_visit__application_details__app_version, --128
  agg_custom_visit__account__details__service_subscriptions, -- 64
  agg_custom_visit__account__details__service_subscriptions_other, -- 32
  agg_custom_visit__account__details__service_subscriptions_core, -- 16
  agg_custom_visit__account__details__service_subscriptions_mobile, -- 8
  agg_custom_customer_group, -- 4
  agg_custom_visit__account__configuration_factors, -- 2
  agg_visit__account__configuration_factors, -- 1
  unit_type,
    CAST(grouping__id AS INT) AS grouping_id,
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
        IF(SUM(AP_enroll_abandon_core) > 0, 1, 0) AS AP_enroll_abandon_core,
        IF(SUM(AP_enroll_failure_core) > 0, 1, 0) AS AP_enroll_failure_core,
        IF(SUM(AP_enroll_start_core) > 0, 1, 0) AS AP_enroll_start_core,
        IF(SUM(AP_enroll_submit_core) > 0, 1, 0) AS AP_enroll_submit_core,
        IF(SUM(AP_enroll_success_core) > 0, 1, 0) AS AP_enroll_success_core,
        IF(SUM(AP_manage_failure) > 0, 1, 0) AS AP_manage_failure,
        IF(SUM(AP_manage_start) > 0, 1, 0) AS AP_manage_start,
        IF(SUM(AP_manage_success) > 0, 1, 0) AS AP_manage_success,
        IF(SUM(CPNI_cancel) > 0, 1, 0) AS CPNI_cancel,
        IF(SUM(CPNI_failure) > 0, 1, 0) AS CPNI_failure,
        IF(SUM(CPNI_failure_IMEI) > 0, 1, 0) AS CPNI_failure_IMEI,
        IF(SUM(CPNI_failure_email) > 0, 1, 0) AS CPNI_failure_email,
        IF(SUM(CPNI_failure_mac_address) > 0, 1, 0) AS CPNI_failure_mac_address,
        IF(SUM(CPNI_failure_security_code) > 0, 1, 0) AS CPNI_failure_security_code,
        IF(SUM(CPNI_start) > 0, 1, 0) AS CPNI_start,
        IF(SUM(CPNI_start_modalView) > 0, 1, 0) AS CPNI_start_modalView,
        IF(SUM(CPNI_start_pageView) > 0, 1, 0) AS CPNI_start_pageView,
        IF(SUM(CPNI_submit) > 0, 1, 0) AS CPNI_submit,
        IF(SUM(CPNI_submit_IMEI) > 0, 1, 0) AS CPNI_submit_IMEI,
        IF(SUM(CPNI_submit_email) > 0, 1, 0) AS CPNI_submit_email,
        IF(SUM(CPNI_submit_mac_address) > 0, 1, 0) AS CPNI_submit_mac_address,
        IF(SUM(CPNI_submit_security_code) > 0, 1, 0) AS CPNI_submit_security_code,
        IF(SUM(CPNI_success) > 0, 1, 0) AS CPNI_success,
        IF(SUM(CPNI_success_IMEI) > 0, 1, 0) AS CPNI_success_IMEI,
        IF(SUM(CPNI_success_email) > 0, 1, 0) AS CPNI_success_email,
        IF(SUM(CPNI_success_mac_address) > 0, 1, 0) AS CPNI_success_mac_address,
        IF(SUM(CPNI_success_security_code) > 0, 1, 0) AS CPNI_success_security_code,
        IF(SUM(OTP_failure_core) > 0, 1, 0) AS OTP_failure_core,
        IF(SUM(OTP_failure_core_full) > 0, 1, 0) AS OTP_failure_core_full,
        IF(SUM(OTP_failure_core_quickPay) > 0, 1, 0) AS OTP_failure_core_quickPay,
        IF(SUM(OTP_failure_legacy) > 0, 1, 0) AS OTP_failure_legacy,
        IF(SUM(OTP_failure_mobile) > 0, 1, 0) AS OTP_failure_mobile,
        IF(SUM(OTP_failure_mobile_full) > 0, 1, 0) AS OTP_failure_mobile_full,
        IF(SUM(OTP_failure_mobile_quickPay) > 0, 1, 0) AS OTP_failure_mobile_quickPay,
        IF(SUM(OTP_start_core) > 0, 1, 0) AS OTP_start_core,
        IF(SUM(OTP_start_core_full) > 0, 1, 0) AS OTP_start_core_full,
        IF(SUM(OTP_start_core_quickPay) > 0, 1, 0) AS OTP_start_core_quickPay,
        IF(SUM(OTP_start_legacy) > 0, 1, 0) AS OTP_start_legacy,
        IF(SUM(OTP_start_mobile) > 0, 1, 0) AS OTP_start_mobile,
        IF(SUM(OTP_start_mobile_full) > 0, 1, 0) AS OTP_start_mobile_full,
        IF(SUM(OTP_start_mobile_quickPay) > 0, 1, 0) AS OTP_start_mobile_quickPay,
        IF(SUM(OTP_start_modal) > 0, 1, 0) AS OTP_start_modal,
        IF(SUM(OTP_submit_core) > 0, 1, 0) AS OTP_submit_core,
        IF(SUM(OTP_submit_core_full) > 0, 1, 0) AS OTP_submit_core_full,
        IF(SUM(OTP_submit_core_quickPay) > 0, 1, 0) AS OTP_submit_core_quickPay,
        IF(SUM(OTP_submit_legacy) > 0, 1, 0) AS OTP_submit_legacy,
        IF(SUM(OTP_submit_mobile) > 0, 1, 0) AS OTP_submit_mobile,
        IF(SUM(OTP_submit_mobile_full) > 0, 1, 0) AS OTP_submit_mobile_full,
        IF(SUM(OTP_submit_mobile_quickPay) > 0, 1, 0) AS OTP_submit_mobile_quickPay,
        IF(SUM(OTP_success_core) > 0, 1, 0) AS OTP_success_core,
        IF(SUM(OTP_success_core_full) > 0, 1, 0) AS OTP_success_core_full,
        IF(SUM(OTP_success_core_quickPay) > 0, 1, 0) AS OTP_success_core_quickPay,
        IF(SUM(OTP_success_legacy) > 0, 1, 0) AS OTP_success_legacy,
        IF(SUM(OTP_success_mobile) > 0, 1, 0) AS OTP_success_mobile,
        IF(SUM(OTP_success_mobile_full) > 0, 1, 0) AS OTP_success_mobile_full,
        IF(SUM(OTP_success_mobile_quickPay) > 0, 1, 0) AS OTP_success_mobile_quickPay,
        IF(SUM(device_make_payment_failure) > 0, 1, 0) AS device_make_payment_failure,
        IF(SUM(device_make_payment_start) > 0, 1, 0) AS device_make_payment_start,
        IF(SUM(device_make_payment_submit) > 0, 1, 0) AS device_make_payment_submit,
        IF(SUM(device_make_payment_success) > 0, 1, 0) AS device_make_payment_success,
        IF(SUM(login_attempt) > 0, 1, 0) AS login_attempt,
        IF(SUM(login_failure) > 0, 1, 0) AS login_failure,
        IF(SUM(login_success) > 0, 1, 0) AS login_success,
        IF(SUM(mobile_activation_activation_complete) > 0, 1, 0) AS mobile_activation_activation_complete,
        IF(SUM(mobile_activation_activation_in_progress) > 0, 1, 0) AS mobile_activation_activation_in_progress,
        IF(SUM(mobile_activation_transfer_complete) > 0, 1, 0) AS mobile_activation_transfer_complete,
        IF(SUM(mobile_activation_transfer_in_progress) > 0, 1, 0) AS mobile_activation_transfer_in_progress,
        IF(SUM(mobile_activation_unable_to_activate) > 0, 1, 0) AS mobile_activation_unable_to_activate,
        IF(SUM(mobile_activation_unable_to_transfer) > 0, 1, 0) AS mobile_activation_unable_to_transfer,
        IF(SUM(mobile_change_plan) > 0, 1, 0) AS mobile_change_plan,
        IF(SUM(mobile_confirm_change_plan) > 0, 1, 0) AS mobile_confirm_change_plan,
        IF(SUM(mobile_device_upgrade) > 0, 1, 0) AS mobile_device_upgrade,
        IF(SUM(mobile_success_change_plan) > 0, 1, 0) AS mobile_success_change_plan,
        IF(SUM(mobile_tier_upgrade_review) > 0, 1, 0) AS mobile_tier_upgrade_review,
        IF(SUM(mobile_tier_upgrade_start) > 0, 1, 0) AS mobile_tier_upgrade_start,
        IF(SUM(mobile_tier_upgrade_submit) > 0, 1, 0) AS mobile_tier_upgrade_submit,
        IF(SUM(mobile_tier_upgrade_success) > 0, 1, 0) AS mobile_tier_upgrade_success,
        IF(SUM(page_load_billing) > 0, 1, 0) AS page_load_billing,
        IF(SUM(page_load_billing_hot_2000) > 0, 1, 0) AS page_load_billing_hot_2000,
        IF(SUM(page_load_billing_hot_3000) > 0, 1, 0) AS page_load_billing_hot_3000,
        IF(SUM(page_load_home) > 0, 1, 0) AS page_load_home,
        IF(SUM(page_load_home_hot_2000) > 0, 1, 0) AS page_load_home_hot_2000,
        IF(SUM(page_load_home_hot_3000) > 0, 1, 0) AS page_load_home_hot_3000,
        IF(SUM(payment_method_add_failure_mobile) > 0, 1, 0) AS payment_method_add_failure_mobile,
        IF(SUM(payment_method_add_success_mobile) > 0, 1, 0) AS payment_method_add_success_mobile,
        IF(SUM(payment_method_delete_failure_mobile) > 0, 1, 0) AS payment_method_delete_failure_mobile,
        IF(SUM(payment_method_delete_success_mobile) > 0, 1, 0) AS payment_method_delete_success_mobile,
        IF(SUM(payment_method_edit_failure_mobile) > 0, 1, 0) AS payment_method_edit_failure_mobile,
        IF(SUM(payment_method_edit_success_mobile) > 0, 1, 0) AS payment_method_edit_success_mobile,
        IF(SUM(portals_csat_submit_rating_1_very_dissatisfied) > 0, 1, 0) AS portals_csat_submit_rating_1_very_dissatisfied,
        IF(SUM(portals_csat_submit_rating_2_dissatisfied) > 0, 1, 0) AS portals_csat_submit_rating_2_dissatisfied,
        IF(SUM(portals_csat_submit_rating_3_neutral) > 0, 1, 0) AS portals_csat_submit_rating_3_neutral,
        IF(SUM(portals_csat_submit_rating_4_satisfied) > 0, 1, 0) AS portals_csat_submit_rating_4_satisfied,
        IF(SUM(portals_csat_submit_rating_5_very_satisfied) > 0, 1, 0) AS portals_csat_submit_rating_5_very_satisfied,

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
        IF(SUM(AP_enroll_abandon_core) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS AP_enroll_abandon_core_calls,
        IF(SUM(AP_enroll_failure_core) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS AP_enroll_failure_core_calls,
        IF(SUM(AP_enroll_start_core) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS AP_enroll_start_core_calls,
        IF(SUM(AP_enroll_submit_core) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS AP_enroll_submit_core_calls,
        IF(SUM(AP_enroll_success_core) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS AP_enroll_success_core_calls,
        IF(SUM(AP_manage_failure) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS AP_manage_failure_calls,
        IF(SUM(AP_manage_start) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS AP_manage_start_calls,
        IF(SUM(AP_manage_success) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS AP_manage_success_calls,
        IF(SUM(CPNI_cancel) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_cancel_calls,
        IF(SUM(CPNI_failure) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_failure_calls,
        IF(SUM(CPNI_failure_IMEI) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_failure_IMEI_calls,
        IF(SUM(CPNI_failure_email) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_failure_email_calls,
        IF(SUM(CPNI_failure_mac_address) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_failure_mac_address_calls,
        IF(SUM(CPNI_failure_security_code) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_failure_security_code_calls,
        IF(SUM(CPNI_start) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_start_calls,
        IF(SUM(CPNI_start_modalView) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_start_modalView_calls,
        IF(SUM(CPNI_start_pageView) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_start_pageView_calls,
        IF(SUM(CPNI_submit) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_submit_calls,
        IF(SUM(CPNI_submit_IMEI) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_submit_IMEI_calls,
        IF(SUM(CPNI_submit_email) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_submit_email_calls,
        IF(SUM(CPNI_submit_mac_address) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_submit_mac_address_calls,
        IF(SUM(CPNI_submit_security_code) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_submit_security_code_calls,
        IF(SUM(CPNI_success) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_success_calls,
        IF(SUM(CPNI_success_IMEI) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_success_IMEI_calls,
        IF(SUM(CPNI_success_email) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_success_email_calls,
        IF(SUM(CPNI_success_mac_address) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_success_mac_address_calls,
        IF(SUM(CPNI_success_security_code) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS CPNI_success_security_code_calls,
        IF(SUM(OTP_failure_core) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_failure_core_calls,
        IF(SUM(OTP_failure_core_full) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_failure_core_full_calls,
        IF(SUM(OTP_failure_core_quickPay) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_failure_core_quickPay_calls,
        IF(SUM(OTP_failure_legacy) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_failure_legacy_calls,
        IF(SUM(OTP_failure_mobile) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_failure_mobile_calls,
        IF(SUM(OTP_failure_mobile_full) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_failure_mobile_full_calls,
        IF(SUM(OTP_failure_mobile_quickPay) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_failure_mobile_quickPay_calls,
        IF(SUM(OTP_start_core) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_start_core_calls,
        IF(SUM(OTP_start_core_full) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_start_core_full_calls,
        IF(SUM(OTP_start_core_quickPay) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_start_core_quickPay_calls,
        IF(SUM(OTP_start_legacy) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_start_legacy_calls,
        IF(SUM(OTP_start_mobile) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_start_mobile_calls,
        IF(SUM(OTP_start_mobile_full) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_start_mobile_full_calls,
        IF(SUM(OTP_start_mobile_quickPay) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_start_mobile_quickPay_calls,
        IF(SUM(OTP_start_modal) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_start_modal_calls,
        IF(SUM(OTP_submit_core) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_submit_core_calls,
        IF(SUM(OTP_submit_core_full) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_submit_core_full_calls,
        IF(SUM(OTP_submit_core_quickPay) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_submit_core_quickPay_calls,
        IF(SUM(OTP_submit_legacy) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_submit_legacy_calls,
        IF(SUM(OTP_submit_mobile) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_submit_mobile_calls,
        IF(SUM(OTP_submit_mobile_full) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_submit_mobile_full_calls,
        IF(SUM(OTP_submit_mobile_quickPay) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_submit_mobile_quickPay_calls,
        IF(SUM(OTP_success_core) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_success_core_calls,
        IF(SUM(OTP_success_core_full) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_success_core_full_calls,
        IF(SUM(OTP_success_core_quickPay) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_success_core_quickPay_calls,
        IF(SUM(OTP_success_legacy) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_success_legacy_calls,
        IF(SUM(OTP_success_mobile) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_success_mobile_calls,
        IF(SUM(OTP_success_mobile_full) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_success_mobile_full_calls,
        IF(SUM(OTP_success_mobile_quickPay) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS OTP_success_mobile_quickPay_calls,
        IF(SUM(device_make_payment_failure) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS device_make_payment_failure_calls,
        IF(SUM(device_make_payment_start) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS device_make_payment_start_calls,
        IF(SUM(device_make_payment_submit) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS device_make_payment_submit_calls,
        IF(SUM(device_make_payment_success) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS device_make_payment_success_calls,
        IF(SUM(login_attempt) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS login_attempt_calls,
        IF(SUM(login_failure) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS login_failure_calls,
        IF(SUM(login_success) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS login_success_calls,
        IF(SUM(mobile_activation_activation_complete) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_activation_activation_complete_calls,
        IF(SUM(mobile_activation_activation_in_progress) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_activation_activation_in_progress_calls,
        IF(SUM(mobile_activation_transfer_complete) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_activation_transfer_complete_calls,
        IF(SUM(mobile_activation_transfer_in_progress) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_activation_transfer_in_progress_calls,
        IF(SUM(mobile_activation_unable_to_activate) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_activation_unable_to_activate_calls,
        IF(SUM(mobile_activation_unable_to_transfer) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_activation_unable_to_transfer_calls,
        IF(SUM(mobile_change_plan) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_change_plan_calls,
        IF(SUM(mobile_confirm_change_plan) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_confirm_change_plan_calls,
        IF(SUM(mobile_device_upgrade) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_device_upgrade_calls,
        IF(SUM(mobile_success_change_plan) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_success_change_plan_calls,
        IF(SUM(mobile_tier_upgrade_review) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_tier_upgrade_review_calls,
        IF(SUM(mobile_tier_upgrade_start) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_tier_upgrade_start_calls,
        IF(SUM(mobile_tier_upgrade_submit) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_tier_upgrade_submit_calls,
        IF(SUM(mobile_tier_upgrade_success) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS mobile_tier_upgrade_success_calls,
        IF(SUM(page_load_billing) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS page_load_billing_calls,
        IF(SUM(page_load_billing_hot_2000) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS page_load_billing_hot_2000_calls,
        IF(SUM(page_load_billing_hot_3000) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS page_load_billing_hot_3000_calls,
        IF(SUM(page_load_home) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS page_load_home_calls,
        IF(SUM(page_load_home_hot_2000) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS page_load_home_hot_2000_calls,
        IF(SUM(page_load_home_hot_3000) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS page_load_home_hot_3000_calls,
        IF(SUM(payment_method_add_failure_mobile) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS payment_method_add_failure_mobile_calls,
        IF(SUM(payment_method_add_success_mobile) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS payment_method_add_success_mobile_calls,
        IF(SUM(payment_method_delete_failure_mobile) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS payment_method_delete_failure_mobile_calls,
        IF(SUM(payment_method_delete_success_mobile) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS payment_method_delete_success_mobile_calls,
        IF(SUM(payment_method_edit_failure_mobile) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS payment_method_edit_failure_mobile_calls,
        IF(SUM(payment_method_edit_success_mobile) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS payment_method_edit_success_mobile_calls,
        IF(SUM(portals_csat_submit_rating_1_very_dissatisfied) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS portals_csat_submit_rating_1_very_dissatisfied_calls,
        IF(SUM(portals_csat_submit_rating_2_dissatisfied) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS portals_csat_submit_rating_2_dissatisfied_calls,
        IF(SUM(portals_csat_submit_rating_3_neutral) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS portals_csat_submit_rating_3_neutral_calls,
        IF(SUM(portals_csat_submit_rating_4_satisfied) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS portals_csat_submit_rating_4_satisfied_calls,
        IF(SUM(portals_csat_submit_rating_5_very_satisfied) > 0  AND SUM(calls_within_24_hrs) > 0, 1, 0) AS portals_csat_submit_rating_5_very_satisfied_calls

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

    FROM
      (
      SELECT
      partition_date_utc, -- 2048
      visit__application_details__application_name, -- 1024
      visit__application_details__application_type, -- 512
      visit__application_details__app_version, -- 256
      CUSTOM_visit__application_details__app_version, --128
      agg_custom_visit__account__details__service_subscriptions, -- 64
      agg_custom_visit__account__details__service_subscriptions_other, -- 32
      agg_custom_visit__account__details__service_subscriptions_core, -- 16
      agg_custom_visit__account__details__service_subscriptions_mobile, -- 8
      agg_custom_customer_group, -- 4
      agg_custom_visit__account__configuration_factors, -- 2
      agg_visit__account__configuration_factors, -- 1
        'accounts' AS unit_type,
        portals_unique_acct_key AS unit_identifier,

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
        SUM(AP_enroll_abandon_core) AS AP_enroll_abandon_core,
        SUM(AP_enroll_failure_core) AS AP_enroll_failure_core,
        SUM(AP_enroll_start_core) AS AP_enroll_start_core,
        SUM(AP_enroll_submit_core) AS AP_enroll_submit_core,
        SUM(AP_enroll_success_core) AS AP_enroll_success_core,
        SUM(AP_manage_failure) AS AP_manage_failure,
        SUM(AP_manage_start) AS AP_manage_start,
        SUM(AP_manage_success) AS AP_manage_success,
        SUM(CPNI_cancel) AS CPNI_cancel,
        SUM(CPNI_failure) AS CPNI_failure,
        SUM(CPNI_failure_IMEI) AS CPNI_failure_IMEI,
        SUM(CPNI_failure_email) AS CPNI_failure_email,
        SUM(CPNI_failure_mac_address) AS CPNI_failure_mac_address,
        SUM(CPNI_failure_security_code) AS CPNI_failure_security_code,
        SUM(CPNI_start) AS CPNI_start,
        SUM(CPNI_start_modalView) AS CPNI_start_modalView,
        SUM(CPNI_start_pageView) AS CPNI_start_pageView,
        SUM(CPNI_submit) AS CPNI_submit,
        SUM(CPNI_submit_IMEI) AS CPNI_submit_IMEI,
        SUM(CPNI_submit_email) AS CPNI_submit_email,
        SUM(CPNI_submit_mac_address) AS CPNI_submit_mac_address,
        SUM(CPNI_submit_security_code) AS CPNI_submit_security_code,
        SUM(CPNI_success) AS CPNI_success,
        SUM(CPNI_success_IMEI) AS CPNI_success_IMEI,
        SUM(CPNI_success_email) AS CPNI_success_email,
        SUM(CPNI_success_mac_address) AS CPNI_success_mac_address,
        SUM(CPNI_success_security_code) AS CPNI_success_security_code,
        SUM(OTP_failure_core) AS OTP_failure_core,
        SUM(OTP_failure_core_full) AS OTP_failure_core_full,
        SUM(OTP_failure_core_quickPay) AS OTP_failure_core_quickPay,
        SUM(OTP_failure_legacy) AS OTP_failure_legacy,
        SUM(OTP_failure_mobile) AS OTP_failure_mobile,
        SUM(OTP_failure_mobile_full) AS OTP_failure_mobile_full,
        SUM(OTP_failure_mobile_quickPay) AS OTP_failure_mobile_quickPay,
        SUM(OTP_start_core) AS OTP_start_core,
        SUM(OTP_start_core_full) AS OTP_start_core_full,
        SUM(OTP_start_core_quickPay) AS OTP_start_core_quickPay,
        SUM(OTP_start_legacy) AS OTP_start_legacy,
        SUM(OTP_start_mobile) AS OTP_start_mobile,
        SUM(OTP_start_mobile_full) AS OTP_start_mobile_full,
        SUM(OTP_start_mobile_quickPay) AS OTP_start_mobile_quickPay,
        SUM(OTP_start_modal) AS OTP_start_modal,
        SUM(OTP_submit_core) AS OTP_submit_core,
        SUM(OTP_submit_core_full) AS OTP_submit_core_full,
        SUM(OTP_submit_core_quickPay) AS OTP_submit_core_quickPay,
        SUM(OTP_submit_legacy) AS OTP_submit_legacy,
        SUM(OTP_submit_mobile) AS OTP_submit_mobile,
        SUM(OTP_submit_mobile_full) AS OTP_submit_mobile_full,
        SUM(OTP_submit_mobile_quickPay) AS OTP_submit_mobile_quickPay,
        SUM(OTP_success_core) AS OTP_success_core,
        SUM(OTP_success_core_full) AS OTP_success_core_full,
        SUM(OTP_success_core_quickPay) AS OTP_success_core_quickPay,
        SUM(OTP_success_legacy) AS OTP_success_legacy,
        SUM(OTP_success_mobile) AS OTP_success_mobile,
        SUM(OTP_success_mobile_full) AS OTP_success_mobile_full,
        SUM(OTP_success_mobile_quickPay) AS OTP_success_mobile_quickPay,
        SUM(device_make_payment_failure) AS device_make_payment_failure,
        SUM(device_make_payment_start) AS device_make_payment_start,
        SUM(device_make_payment_submit) AS device_make_payment_submit,
        SUM(device_make_payment_success) AS device_make_payment_success,
        SUM(login_attempt) AS login_attempt,
        SUM(login_failure) AS login_failure,
        SUM(login_success) AS login_success,
        SUM(mobile_activation_activation_complete) AS mobile_activation_activation_complete,
        SUM(mobile_activation_activation_in_progress) AS mobile_activation_activation_in_progress,
        SUM(mobile_activation_transfer_complete) AS mobile_activation_transfer_complete,
        SUM(mobile_activation_transfer_in_progress) AS mobile_activation_transfer_in_progress,
        SUM(mobile_activation_unable_to_activate) AS mobile_activation_unable_to_activate,
        SUM(mobile_activation_unable_to_transfer) AS mobile_activation_unable_to_transfer,
        SUM(mobile_change_plan) AS mobile_change_plan,
        SUM(mobile_confirm_change_plan) AS mobile_confirm_change_plan,
        SUM(mobile_device_upgrade) AS mobile_device_upgrade,
        SUM(mobile_success_change_plan) AS mobile_success_change_plan,
        SUM(mobile_tier_upgrade_review) AS mobile_tier_upgrade_review,
        SUM(mobile_tier_upgrade_start) AS mobile_tier_upgrade_start,
        SUM(mobile_tier_upgrade_submit) AS mobile_tier_upgrade_submit,
        SUM(mobile_tier_upgrade_success) AS mobile_tier_upgrade_success,
        SUM(page_load_billing) AS page_load_billing,
        SUM(page_load_billing_hot_2000) AS page_load_billing_hot_2000,
        SUM(page_load_billing_hot_3000) AS page_load_billing_hot_3000,
        SUM(page_load_home) AS page_load_home,
        SUM(page_load_home_hot_2000) AS page_load_home_hot_2000,
        SUM(page_load_home_hot_3000) AS page_load_home_hot_3000,
        SUM(payment_method_add_failure_mobile) AS payment_method_add_failure_mobile,
        SUM(payment_method_add_success_mobile) AS payment_method_add_success_mobile,
        SUM(payment_method_delete_failure_mobile) AS payment_method_delete_failure_mobile,
        SUM(payment_method_delete_success_mobile) AS payment_method_delete_success_mobile,
        SUM(payment_method_edit_failure_mobile) AS payment_method_edit_failure_mobile,
        SUM(payment_method_edit_success_mobile) AS payment_method_edit_success_mobile,
        SUM(portals_csat_submit_rating_1_very_dissatisfied) AS portals_csat_submit_rating_1_very_dissatisfied,
        SUM(portals_csat_submit_rating_2_dissatisfied) AS portals_csat_submit_rating_2_dissatisfied,
        SUM(portals_csat_submit_rating_3_neutral) AS portals_csat_submit_rating_3_neutral,
        SUM(portals_csat_submit_rating_4_satisfied) AS portals_csat_submit_rating_4_satisfied,
        SUM(portals_csat_submit_rating_5_very_satisfied) AS portals_csat_submit_rating_5_very_satisfied,
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

      SUM(calls_within_24_hrs) as calls_within_24_hrs
      FROM asp_m2dot0_metric_agg
      WHERE (partition_date_utc >= ("${hiveconf:START_DATE}") AND partition_date_utc < ("${hiveconf:END_DATE}"))
      GROUP BY
      partition_date_utc, -- 2048
      visit__application_details__application_name, -- 1024
      visit__application_details__application_type, -- 512
      visit__application_details__app_version, -- 256
      CUSTOM_visit__application_details__app_version, --128
      agg_custom_visit__account__details__service_subscriptions, -- 64
      agg_custom_visit__account__details__service_subscriptions_other, -- 32
      agg_custom_visit__account__details__service_subscriptions_core, -- 16
      agg_custom_visit__account__details__service_subscriptions_mobile, -- 8
      agg_custom_customer_group, -- 4
      agg_custom_visit__account__configuration_factors, -- 2
      agg_visit__account__configuration_factors, -- 1
        portals_unique_acct_key,
        'accounts'
      ) sumfirst
    GROUP BY
      unit_type, -- 4096
      unit_identifier, -- 2048
      partition_date_utc, -- 2048
      visit__application_details__application_name, -- 1024
      visit__application_details__application_type, -- 512
      visit__application_details__app_version, -- 256
      CUSTOM_visit__application_details__app_version, --128
      agg_custom_visit__account__details__service_subscriptions, -- 64
      agg_custom_visit__account__details__service_subscriptions_other, -- 32
      agg_custom_visit__account__details__service_subscriptions_core, -- 16
      agg_custom_visit__account__details__service_subscriptions_mobile, -- 8
      agg_custom_customer_group, -- 4
      agg_custom_visit__account__configuration_factors, -- 2
      agg_visit__account__configuration_factors -- 1
      GROUPING SETS (
        (partition_date_utc, unit_identifier),
        (partition_date_utc, unit_identifier, visit__application_details__application_name, agg_custom_customer_group)
        --(partition_date_utc, unit_identifier, visit__application_details__application_name, CUSTOM_visit__application_details__app_version, agg_custom_customer_group),
        -- (partition_date_utc, unit_identifier, visit__application_details__application_name, CUSTOM_visit__application_details__app_version, agg_CUSTOM_visit__account__details__service_subscriptions_core, agg_CUSTOM_visit__account__details__service_subscriptions_mobile),
        -- (partition_date_utc, unit_identifier, visit__application_details__application_name, CUSTOM_visit__application_details__app_version, agg_custom_visit__account__configuration_factors, agg_custom_visit__account__details__service_subscriptions_mobile)
      )
        ) sets
  GROUP BY
    partition_date_utc,
    visit__application_details__application_name, -- 1024
    visit__application_details__application_type, -- 512
    visit__application_details__app_version, -- 256
    CUSTOM_visit__application_details__app_version, --128
    agg_custom_visit__account__details__service_subscriptions, -- 64
    agg_custom_visit__account__details__service_subscriptions_other, -- 32
    agg_custom_visit__account__details__service_subscriptions_core, -- 16
    agg_custom_visit__account__details__service_subscriptions_mobile, -- 8
    agg_custom_customer_group, -- 4
    agg_custom_visit__account__configuration_factors, -- 2
    agg_visit__account__configuration_factors, -- 1
    grouping_id,
    unit_type,
    CAST(NULL as INT)
  ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
LATERAL VIEW EXPLODE(call_map) explode_table AS metric_name_calls, metric_calls
WHERE metric_name=metric_name_calls;

--------------------------------------------------------------------------------
--------------------------------***** END *****---------------------------------
--------------------------------------------------------------------------------
