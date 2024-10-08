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
-- {unit_type} metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
INSERT INTO ${env:TMP_db}.m2dot0_set_agg_portals_stage_{unit_type}_${hiveconf:execid}_${hiveconf:stepid}
SELECT
CASE WHEN (grouping_id & 32) = 0 THEN visit__application_details__application_name ELSE 'All apps' END AS visit__application_details__application_name,
CASE WHEN (grouping_id & 16) = 0 THEN visit__application_details__application_type ELSE 'All app types' END AS visit__application_details__application_type,
CASE WHEN (grouping_id & 8) = 0 THEN visit__application_details__app_version ELSE 'All app versions' END AS visit__application_details__app_version,
CASE WHEN (grouping_id & 4) = 0 THEN agg_custom_visit__account__details__service_subscriptions ELSE 'All services' END AS agg_custom_visit__account__details__service_subscriptions,
CASE WHEN (grouping_id & 2) = 0 THEN agg_custom_customer_group else 'All Customer Groups' end as agg_custom_customer_group,
CASE WHEN (grouping_id & 1) = 0 THEN agg_visit__account__configuration_factors ELSE 'All migration status' END AS agg_visit__account__configuration_factors,
  grouping_id,
  metric_name,
  metric_value,
  '${hiveconf:ProcessTimestamp}' as process_date_time_denver,
  '${hiveconf:ProcessUser}' AS process_identity,
  unit_type,
  IF('{unit_type}'='visits',cast(metric_calls as string),cast(NULL as string)) as call_count_24h,
  partition_date_utc,
  '${hiveconf:grain}' AS grain
FROM
  (
  SELECT
    partition_date_utc, -- 64
    visit__application_details__application_name, -- 32
    visit__application_details__application_type, -- 16
    visit__application_details__app_version, -- 8
    agg_custom_visit__account__details__service_subscriptions, -- 4
    agg_custom_customer_group, -- 2
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
        'Mobile Change Plan Flow Completion Rate', sum(mobile_tier_upgrade_success + mobile_success_change_plan)/SUM(mobile_tier_upgrade_start + mobile_change_plan_start),
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
        'Activation Starts', SUm(mobile_activation_activation_in_progress + mobile_activation_transfer_in_progress),
        'Activation Failures', SUM(mobile_activation_unable_to_activate),
        'AutoPay Enroll Flow (Core) - Starts', SUM(ap_enroll_start_core),
        'AutoPay Enroll Flow (Core) - Successes', SUM(AP_enroll_success_core),
        'AutoPay Enroll Flow (Core) - Failures', SUM(ap_enroll_failure_core),
        'AutoPay Enroll Flow (Core) - Attempts', SUM(ap_enroll_success_core + ap_enroll_failure_core),
        --'AutoPay Enroll Flow (Core) - Abandons', sum(ap_enroll_abandon_core),
        'AutoPay Enroll Attempt (Core) - Starts', sum(ap_enroll_start_core),
        'AutoPay Enroll Attempt (Core) - Successes', sum(AP_enroll_success_core),
        'AutoPay Enroll Attempt (Core) - Failures', sum(ap_enroll_failure_core),
        'AutoPay Enroll Attempt (Core) - Attempts', SUM(ap_enroll_failure_core + AP_enroll_success_core),
        --'AutoPay Enroll Attempt (Core) - Abandons', sum(ap_enroll_abandon_core),
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
        'Mobile Change Plan Flow Completion - Starts', sum(mobile_tier_upgrade_start + mobile_change_plan_start),
        'Mobile Change Plan Submit Success Rate - Successes', sum(mobile_tier_upgrade_success + mobile_success_change_plan),
        'Mobile Change Plan Submit Success Rate - Submits', SUM(mobile_confirm_change_plan + mobile_tier_upgrade_submit),
        'Mobile Change Plan Submit Success Rate - Starts', sum(mobile_tier_upgrade_start + mobile_change_plan_start),

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
