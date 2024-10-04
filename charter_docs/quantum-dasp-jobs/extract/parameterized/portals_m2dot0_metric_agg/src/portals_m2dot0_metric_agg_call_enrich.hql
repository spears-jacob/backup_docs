USE ${env:DASP_db};

set hive.exec.dynamic.partition.mode=nonstrict;

--updates table with data from previous seven days (relative to RUN_DATE)

 MSCK REPAIR TABLE cs_calls_with_prior_visits;
 MSCK REPAIR TABLE quantum_m2dot0_metric_agg;

insert overwrite table m2dot0_metric_agg
partition (partition_date_utc)
USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

ADD JAR ${env:ARTIFACTS_PATH}/jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

INSERT OVERWRITE TABLE asp_m2dot0_metric_agg PARTITION (partition_date_utc)

select
ma.visit__application_details__application_name as visit__application_details__application_name,
ma.visit__application_details__application_type as visit__application_details__application_type,
ma.visit__application_details__app_version as visit__application_details__app_version,
ma.CUSTOM_visit__application_details__app_version as CUSTOM_visit__application_details__app_version,
ma.agg_CUSTOM_visit__account__details__service_subscriptions as agg_CUSTOM_visit__account__details__service_subscriptions,
ma.agg_CUSTOM_visit__account__details__service_subscriptions_other as agg_CUSTOM_visit__account__details__service_subscriptions_other,
ma.agg_CUSTOM_visit__account__details__service_subscriptions_core as agg_CUSTOM_visit__account__details__service_subscriptions_core,
ma.agg_CUSTOM_visit__account__details__service_subscriptions_mobile as agg_CUSTOM_visit__account__details__service_subscriptions_mobile,
ma.agg_CUSTOM_visit__account__configuration_factors as agg_CUSTOM_visit__account__configuration_factors,
ma.agg_visit__account__configuration_factors as agg_visit__account__configuration_factors,
ma.agg_custom_customer_group as agg_custom_customer_group,
ma.agg_visit__account__details__mso as agg_visit__account__details__mso,
ma.portals_unique_acct_key as portals_unique_acct_key,
ma.visit__account__enc_mobile_account_number as visit__account__enc_mobile_account_number,
ma.visit_id as visit_id,
ma.visit__visit_start_timestamp as visit__visit_start_timestamp,
ma.device_id as device_id,

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
ma.AP_enroll_abandon_core AS   AP_enroll_abandon_core,
ma.AP_enroll_failure_core AS   AP_enroll_failure_core,
ma.AP_enroll_start_core AS   AP_enroll_start_core,
ma.AP_enroll_submit_core AS   AP_enroll_submit_core,
ma.AP_enroll_success_core AS   AP_enroll_success_core,
ma.AP_manage_failure AS   AP_manage_failure,
ma.AP_manage_start AS   AP_manage_start,
ma.AP_manage_success AS   AP_manage_success,
ma.CPNI_cancel AS   CPNI_cancel,
ma.CPNI_failure AS   CPNI_failure,
ma.CPNI_failure_IMEI AS   CPNI_failure_IMEI,
ma.CPNI_failure_email AS   CPNI_failure_email,
ma.CPNI_failure_mac_address AS   CPNI_failure_mac_address,
ma.CPNI_failure_security_code AS   CPNI_failure_security_code,
ma.CPNI_start AS   CPNI_start,
ma.CPNI_start_modalView AS   CPNI_start_modalView,
ma.CPNI_start_pageView AS   CPNI_start_pageView,
ma.CPNI_submit AS   CPNI_submit,
ma.CPNI_submit_IMEI AS   CPNI_submit_IMEI,
ma.CPNI_submit_email AS   CPNI_submit_email,
ma.CPNI_submit_mac_address AS   CPNI_submit_mac_address,
ma.CPNI_submit_security_code AS   CPNI_submit_security_code,
ma.CPNI_success AS   CPNI_success,
ma.CPNI_success_IMEI AS   CPNI_success_IMEI,
ma.CPNI_success_email AS   CPNI_success_email,
ma.CPNI_success_mac_address AS   CPNI_success_mac_address,
ma.CPNI_success_security_code AS   CPNI_success_security_code,
ma.OTP_failure_core AS   OTP_failure_core,
ma.OTP_failure_core_full AS   OTP_failure_core_full,
ma.OTP_failure_core_quickPay AS   OTP_failure_core_quickPay,
ma.OTP_failure_legacy AS   OTP_failure_legacy,
ma.OTP_failure_mobile AS   OTP_failure_mobile,
ma.OTP_failure_mobile_full AS   OTP_failure_mobile_full,
ma.OTP_failure_mobile_quickPay AS   OTP_failure_mobile_quickPay,
ma.OTP_start_core AS   OTP_start_core,
ma.OTP_start_core_full AS   OTP_start_core_full,
ma.OTP_start_core_quickPay AS   OTP_start_core_quickPay,
ma.OTP_start_legacy AS   OTP_start_legacy,
ma.OTP_start_mobile AS   OTP_start_mobile,
ma.OTP_start_mobile_full AS   OTP_start_mobile_full,
ma.OTP_start_mobile_quickPay AS   OTP_start_mobile_quickPay,
ma.OTP_start_modal AS   OTP_start_modal,
ma.OTP_submit_core AS   OTP_submit_core,
ma.OTP_submit_core_full AS   OTP_submit_core_full,
ma.OTP_submit_core_quickPay AS   OTP_submit_core_quickPay,
ma.OTP_submit_legacy AS   OTP_submit_legacy,
ma.OTP_submit_mobile AS   OTP_submit_mobile,
ma.OTP_submit_mobile_full AS   OTP_submit_mobile_full,
ma.OTP_submit_mobile_quickPay AS   OTP_submit_mobile_quickPay,
ma.OTP_success_core AS   OTP_success_core,
ma.OTP_success_core_full AS   OTP_success_core_full,
ma.OTP_success_core_quickPay AS   OTP_success_core_quickPay,
ma.OTP_success_legacy AS   OTP_success_legacy,
ma.OTP_success_mobile AS   OTP_success_mobile,
ma.OTP_success_mobile_full AS   OTP_success_mobile_full,
ma.OTP_success_mobile_quickPay AS   OTP_success_mobile_quickPay,
ma.device_make_payment_failure AS   device_make_payment_failure,
ma.device_make_payment_start AS   device_make_payment_start,
ma.device_make_payment_submit AS   device_make_payment_submit,
ma.device_make_payment_success AS   device_make_payment_success,
ma.login_attempt AS   login_attempt,
ma.login_failure AS   login_failure,
ma.login_success AS   login_success,
ma.mobile_activation_activation_complete AS   mobile_activation_activation_complete,
ma.mobile_activation_activation_in_progress AS   mobile_activation_activation_in_progress,
ma.mobile_activation_transfer_complete AS   mobile_activation_transfer_complete,
ma.mobile_activation_transfer_in_progress AS   mobile_activation_transfer_in_progress,
ma.mobile_activation_unable_to_activate AS   mobile_activation_unable_to_activate,
ma.mobile_activation_unable_to_transfer AS   mobile_activation_unable_to_transfer,
ma.mobile_change_plan_start AS   mobile_change_plan_start,
ma.mobile_confirm_change_plan AS   mobile_confirm_change_plan,
ma.mobile_device_upgrade AS   mobile_device_upgrade,
ma.mobile_success_change_plan AS   mobile_success_change_plan,
ma.mobile_tier_upgrade_review AS   mobile_tier_upgrade_review,
ma.mobile_tier_upgrade_start AS   mobile_tier_upgrade_start,
ma.mobile_tier_upgrade_submit AS   mobile_tier_upgrade_submit,
ma.mobile_tier_upgrade_success AS   mobile_tier_upgrade_success,
ma.page_load_billing AS   page_load_billing,
ma.page_load_billing_hot_2000 AS   page_load_billing_hot_2000,
ma.page_load_billing_hot_3000 AS   page_load_billing_hot_3000,
ma.page_load_home AS   page_load_home,
ma.page_load_home_hot_2000 AS   page_load_home_hot_2000,
ma.page_load_home_hot_3000 AS   page_load_home_hot_3000,
ma.payment_method_add_failure_mobile AS   payment_method_add_failure_mobile,
ma.payment_method_add_success_mobile AS   payment_method_add_success_mobile,
ma.payment_method_delete_failure_mobile AS   payment_method_delete_failure_mobile,
ma.payment_method_delete_success_mobile AS   payment_method_delete_success_mobile,
ma.payment_method_edit_failure_mobile AS   payment_method_edit_failure_mobile,
ma.payment_method_edit_success_mobile AS   payment_method_edit_success_mobile,
ma.portals_csat_submit_rating_1_very_dissatisfied AS   portals_csat_submit_rating_1_very_dissatisfied,
ma.portals_csat_submit_rating_2_dissatisfied AS   portals_csat_submit_rating_2_dissatisfied,
ma.portals_csat_submit_rating_3_neutral AS   portals_csat_submit_rating_3_neutral,
ma.portals_csat_submit_rating_4_satisfied AS   portals_csat_submit_rating_4_satisfied,
ma.portals_csat_submit_rating_5_very_satisfied AS   portals_csat_submit_rating_5_very_satisfied,

COALESCE(cwpv.calls_within_24_hrs,0) as calls_within_24_hrs,
partition_date_utc
FROM
asp_m2dot0_metric_agg ma
LEFT
JOIN (
  SELECT visit_id, count(DISTINCT call_inbound_key) as calls_within_24_hrs
  FROM cs_calls_with_prior_visits
  WHERE (call_date >= ('${hiveconf:call_start_date}') AND call_date < ('${hiveconf:end_date}'))
  GROUP BY visit_id) cwpv
  on cwpv.visit_id=ma.visit_id
AND (partition_date_utc >= ('${hiveconf:visit_start_date}') AND partition_date_utc < ('${hiveconf:end_date}'))
;

SELECT partition_date_utc, count(partition_date_utc)
FROM m2dot0_metric_agg
WHERE partition_date_utc >='${hiveconf:call_start_date}'
AND calls_within_24_hrs > 0
GROUP BY partition_date_utc
ORDER BY partition_date_utc desc
LIMIT 10;
