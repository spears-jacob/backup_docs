USE ${env:DASP_db};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join = true;

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.cbo.enable=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.vectorized.execution.reduce.groupby.enabled = true;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16;
set hive.tez.auto.reducer.parallelism=true;
set tez.grouping.max-size=78643200;
set tez.grouping.min-size=52428800;
set hive.exec.reducers.bytes.per.reducer=26214400;
set orc.force.positional.evolution=true;

SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;

--Sumary
SELECT "\n\nFor 1a: si_summary_agg_master (Dummy records)\n\n";

--Dummy record with zeroes/nulls for days that don’t have data
DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_dummy_monthly PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_dummy_monthly AS
SELECT '${hiveconf:START_DATE}' AS denver_date, '${hiveconf:grain}' AS date_level, 'MySpectrum' AS application_group, 'Android' AS application_type, 'Activation' AS metric_type
UNION SELECT '${hiveconf:START_DATE}' AS denver_date, '${hiveconf:grain}' AS date_level, 'MySpectrum' AS application_group, 'iOS' AS application_type, 'Activation' AS metric_type
UNION SELECT '${hiveconf:START_DATE}' AS denver_date, '${hiveconf:grain}' AS date_level, 'MySpectrum' AS application_group, 'Android' AS application_type, 'Program' AS metric_type
UNION SELECT '${hiveconf:START_DATE}' AS denver_date, '${hiveconf:grain}' AS date_level, 'MySpectrum' AS application_group, 'iOS' AS application_type, 'Program' AS metric_type
UNION SELECT '${hiveconf:START_DATE}' AS denver_date, '${hiveconf:grain}' AS date_level, 'SelfInstall' AS application_group, 'Web' AS application_type, 'Activation' AS metric_type
UNION SELECT '${hiveconf:START_DATE}' AS denver_date, '${hiveconf:grain}' AS date_level, 'SelfInstall' AS application_group, 'Web' AS application_type, 'Program' AS metric_type
;

INSERT INTO TABLE si_summary_agg_master PARTITION (denver_date)
SELECT
	NULL AS visit_id,
	NULL AS device_id,
	NULL AS acct_id,
	dummy.application_group AS application_group,
	dummy.application_type AS application_type,
	NULL AS mso,
	NULL AS user_package,
	0 AS initiated_self_installs,
	0 AS initiated_self_installs_new,
	0 AS initiated_self_installs_existing,
	0 AS account_status_past_due,
	0 AS account_status_current,
	0 AS terms_conditions_accept,
	0 AS terms_conditions_disagree,
	0 AS inactivity,
	0 AS abandonment_exiting_via_overflow_menu,
	0 AS activation_no_touch,
	0 AS activation_user_call,
	0 AS activation_self_install_call,
	0 AS activation_truck_roll_call,
	0 AS activation_success_full,
	0 AS activation_success_partial,
	0 AS activation_failure,
	0 AS activation_failure_full,
	0 AS activation_failure_partial,
	0 AS activation_failure_calls,
	0 AS activation_failure_truck_rolls,
	0 AS re_entry,
	0 AS retry,
	0 AS errors_connection_issue,
	0 AS errors_equipment_setup,
	0 AS customer_satisfaction,
	0 AS customer_satisfaction_submitted,
	NULL AS customer_satisfaction_submission_score,
	0 AS customer_satisfaction_completed_survey,
	0 AS home_page_view,
	0 AS equipment_setup_page_view,
	0 AS terms_conditions_page_view,
	0 AS service_terms_conditions_page_view,
	0 AS equipment_checklist_page_view,
	0 AS modem_checklist_page_view,
	0 AS modem_activation_page_view,
	0 AS router_checklist_page_view,
	0 AS network_setup_page_view,
	0 AS router_activation_page_view,
	0 AS rescue_no_touch,
	0 AS rescue_call,
	0 AS rescue_truck,
	'${hiveconf:grain}' AS date_level,
	dummy.metric_type AS metric_type,
	NULL AS acct_division,
	NULL AS acct_division_id,
	0 AS errors_authentication_setup,
	0 AS errors_device_signal_setup,
	0 AS errors_other_setup,
	0 AS tv_activation_page_view,
	0 AS voice_activation_page_view,
	0 AS download_MSA,	
	0 AS avg_transaction_duration,
	0 AS avg_api_response_time,
	0 AS abandonment_via_exit_setup,
	0 AS coam_device,
	0 AS whitelisted_coam_device,
	0 AS blacklisted_coam_device,
	0 AS minimally_qualified_coam_device,
	0 AS equipment_type_scp,
	0 AS equipment_type_monolithic,
	0 AS visit_eligible,
	0 AS repair_call,
	0 AS repair_truck_roll_call,
	0 AS repair_rescue_no_touch,
	0 AS repair_rescue_call,
	0 AS repair_rescue_truck,
	0 AS job_class_category_code_t,
	0 AS job_class_category_code_all,
	0 AS repair_no_touch,
	NULL AS encrypted_account_key_256,
	NULL AS work_order_date,
	0 AS modem_activating,
	0 AS modem_connecting,
	0 AS router_activating,
	0 AS router_connecting,
	0 AS full_screen_takeover_page_view,
	0 AS activation_complete,
	0 AS modem_errors_preActivation,
	0 AS modem_errors_connection,
	0 AS modem_errors_activation,
	0 AS router_errors_preActivation,
	0 AS router_errors_connection,
	0 AS router_errors_activation,
	0 AS tv_errors_preActivation,
	0 AS tv_errors_connection,
	0 AS tv_errors_activation,
	0 AS errors_authentication,
	0 AS errors_preActivation,
	0 AS errors_activation,
	0 AS other_errors,
	0 AS interaction,
	0 AS modem_setup_success,
	0 AS router_setup_success,
	0 AS tv_setup_success,
	0 AS voice_setup_success,
	0 AS total_devices,
	0 AS tv_checklist_page_view,
	0 AS tv_activating,
	0 AS tv_connecting,
	0 AS pending_modem_count,
	0 AS pending_router_count,
	0 AS pending_tv_count,
	0 AS pending_voice_count,
	0 AS modem_errors_postActivation,
	0 AS router_errors_postActivation,
	0 AS tv_errors_postActivation,
	0 AS voice_checklist_page_view,
	0 AS voice_activating,
	'${hiveconf:START_DATE}' AS denver_date
FROM ${env:TMP_db}.si_summary_dummy_monthly dummy
LEFT JOIN si_summary_agg_master master 
	ON master.denver_date = '${hiveconf:START_DATE}'
	AND master.date_level = dummy.date_level 
	AND master.metric_type = dummy.metric_type
	AND master.application_group = dummy.application_group
	AND master.application_type = dummy.application_type
WHERE master.denver_date IS NULL
;

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_dummy_monthly PURGE;
