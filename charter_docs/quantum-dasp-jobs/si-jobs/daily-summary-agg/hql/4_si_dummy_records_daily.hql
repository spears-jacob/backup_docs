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
DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_dummy_daily PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_dummy_daily AS
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
FROM ${env:TMP_db}.si_summary_dummy_daily dummy
LEFT JOIN si_summary_agg_master master
	ON master.denver_date = '${hiveconf:START_DATE}'
	AND master.date_level = dummy.date_level
	AND master.metric_type = dummy.metric_type
	AND master.application_group = dummy.application_group
	AND master.application_type = dummy.application_type
WHERE master.denver_date IS NULL
;


--Aggregate call data
SELECT "\n\nFor 1b: si_summary_call_agg_master (Program metrics)\n\n";

--Dummy record with zeroes/nulls for days that don’t have data
DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_call_dummy_daily PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_call_dummy_daily AS
SELECT '${hiveconf:START_DATE}' AS denver_date, '${hiveconf:grain}' AS date_level, 'MySpectrum' AS application_group, 'Android' AS application_type
UNION SELECT '${hiveconf:START_DATE}' AS denver_date, '${hiveconf:grain}' AS date_level, 'MySpectrum' AS application_group, 'iOS' AS application_type
UNION SELECT '${hiveconf:START_DATE}' AS denver_date, '${hiveconf:grain}' AS date_level, 'SelfInstall' AS application_group, 'Web' AS application_type
;

INSERT INTO TABLE si_summary_call_agg_master PARTITION (denver_date)
SELECT
	NULL AS visit_id,
	NULL AS device_id,
	NULL AS acct_id,
	dummy.application_group AS application_group,
	dummy.application_type AS application_type,
	NULL AS mso,
	NULL AS issue_description,
	NULL AS cause_description,
	NULL AS resolution_description,
	0 AS call_count,
	dummy.date_level AS date_level,
	dummy.denver_date AS denver_date
FROM ${env:TMP_db}.si_summary_call_dummy_daily dummy
LEFT JOIN si_summary_call_agg_master calls
	ON calls.denver_date = dummy.denver_date
	AND calls.date_level = dummy.date_level
	AND calls.application_group = dummy.application_group
	AND calls.application_type = dummy.application_type
WHERE calls.denver_date IS NULL
;


SELECT "\n\nFor 1c: si_cross_platform (Dummy records)\n\n";

--Dummy record with zeroes/nulls for days that don’t have data
DROP TABLE IF EXISTS ${env:TMP_db}.si_cross_platform_dummy PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_cross_platform_dummy AS
SELECT '${hiveconf:START_DATE}' AS denver_date
;

INSERT INTO TABLE si_cross_platform PARTITION (denver_date)
SELECT
	NULL AS acct_id,
	NULL AS mso,
	NULL AS user_package,
	NULL AS msa_first_initiated_self_installs_visit_id,
	0 AS msa_first_initiated_self_installs_timestamp,
	NULL AS msa_complete_activation_self_installs_visit_id,
	0 AS msa_complete_activation_self_installs_timestamp,
	NULL AS web_first_initiated_self_installs_visit_id,
	0 AS web_first_initiated_self_installs_timestamp,
	NULL AS web_complete_activation_self_installs_visit_id,
	0 AS web_complete_activation_self_installs_timestamp,
	0 AS msa_initiated_self_install,
	0 AS web_initiated_self_install,
	0 AS msa_complete_self_install,
	0 AS web_complete_self_install,
	0 AS device_activated,
	'${hiveconf:START_DATE}' AS denver_date
FROM ${env:TMP_db}.si_cross_platform_dummy dummy
LEFT JOIN si_cross_platform master
	ON master.denver_date = dummy.denver_date
	AND master.denver_date = '${hiveconf:START_DATE}'
WHERE master.denver_date IS NULL
;


--Composite Score
SELECT "\n\nFor 1d: si_composite_score_kpi_agg_daily (Dummy records)\n\n";

--Dummy record with zeroes/nulls for days that don’t have data
DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_dummy_daily PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_composite_score_dummy_daily AS
SELECT '${hiveconf:grain}' AS date_level, '${hiveconf:START_DATE}' AS denver_date, 'myspectrum' AS application_group, 1 AS application_version, 1 AS user_package
UNION SELECT '${hiveconf:grain}' AS date_level, '${hiveconf:START_DATE}' AS denver_date, 'selfinstall' AS application_group, 1 AS application_version, 1 AS user_package
;

INSERT INTO TABLE si_composite_score_kpi_agg_daily PARTITION (denver_date)
SELECT
	CAST(CONCAT(
		CASE WHEN grouping(dummy.user_package) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(dummy.application_version) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(dummy.application_group) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(dummy.denver_date) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(dummy.date_level) = 0 THEN 1 ELSE 0 END
    ) AS INT) AS grouping_id,
	'total' AS timeframe,
	dummy.application_group AS application_group,
	NULL AS application_version,
	NULL AS user_package,
	'self_install_activation_composite' AS metric_name,
	NULL AS metric_value,
	NULL AS transaction_duration_filtered_ratio,
	NULL AS api_response_time_filtered_ratio,
	NULL AS transaction_duration_bucket,
	NULL AS api_response_time_bucket,
	NULL AS activation_both_derived,
	NULL AS activation_both,
	NULL AS activation_success,
	NULL AS activation_failure_not,
	0 AS number_uniq_visit,
	'${hiveconf:grain}' AS date_level,
	NULL AS avg_transaction_duration,
	NULL AS avg_api_response_time,
	'${hiveconf:START_DATE}' AS denver_date
FROM ${env:TMP_db}.si_composite_score_dummy_daily dummy
LEFT JOIN si_composite_score_kpi_agg_daily kpi
	ON kpi.denver_date = '${hiveconf:START_DATE}'
	AND kpi.date_level = dummy.date_level
	AND kpi.application_group = dummy.application_group
	AND kpi.grouping_id > 0
WHERE kpi.grouping_id IS NULL
GROUP BY
	dummy.date_level,
	dummy.denver_date,
	dummy.application_group,
	dummy.application_version,
	dummy.user_package
GROUPING SETS(
	(dummy.date_level, dummy.denver_date),                                                        --11
	(dummy.date_level, dummy.denver_date, dummy.application_group),                               --111
	(dummy.date_level, dummy.denver_date, dummy.application_group, dummy.application_version),    --1111
	(dummy.date_level, dummy.denver_date, dummy.user_package),                                    --10011
	(dummy.date_level, dummy.denver_date, dummy.application_group, dummy.user_package)            --10111
)
;


SELECT "\n\nFor 1e: si_composite_score_kpi_agg_visit_daily (Dummy records)\n\n";


--Dummy record with zeroes/nulls for days that don’t have data
DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_visit_dummy_daily PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_composite_score_visit_dummy_daily AS
SELECT '${hiveconf:grain}' AS date_level, '${hiveconf:START_DATE}' AS denver_date, 'myspectrum' AS application_group, '' AS mso, '' AS acct_id, '' AS visit_id, 1 AS application_version, 1 AS user_package
UNION SELECT '${hiveconf:grain}' AS date_level, '${hiveconf:START_DATE}' AS denver_date, 'selfinstall' AS application_group, '' AS mso, '' AS acct_id, '' AS visit_id, 1 AS application_version, 1 AS user_package
;

INSERT INTO TABLE si_composite_score_kpi_agg_visit_daily PARTITION (denver_date)
SELECT
	CAST(CONCAT(
		CASE WHEN grouping(dummy.user_package) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(dummy.application_version) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(dummy.visit_id) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(dummy.acct_id) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(dummy.mso) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(dummy.application_group) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(dummy.denver_date) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(dummy.date_level) = 0 THEN 1 ELSE 0 END
    ) AS INT) AS grouping_id,
	'total' AS timeframe,
	dummy.application_group AS application_group,
	NULL AS mso,
	NULL AS acct_id,
	NULL AS visit_id,
	NULL AS application_version,
	NULL AS user_package,
	'self_install_activation_composite' AS metric_name,
	NULL AS metric_value,
	NULL AS transaction_duration_filtered_ratio,
	NULL AS api_response_time_filtered_ratio,
	NULL AS transaction_duration_bucket,
	NULL AS api_response_time_bucket,
	NULL AS activation_both_derived,
	NULL AS activation_both,
	NULL AS activation_success,
	NULL AS activation_failure_not,
	0 AS number_uniq_visit,
	'${hiveconf:grain}' AS date_level,
	NULL AS avg_transaction_duration,
	NULL AS avg_api_response_time,
	'${hiveconf:START_DATE}' AS denver_date
FROM ${env:TMP_db}.si_composite_score_visit_dummy_daily dummy
LEFT JOIN si_composite_score_kpi_agg_visit_daily kpi
	ON kpi.denver_date = '${hiveconf:START_DATE}'
	AND kpi.date_level = dummy.date_level
	AND kpi.application_group = dummy.application_group
	AND kpi.grouping_id > 0
WHERE kpi.grouping_id IS NULL
GROUP BY
	dummy.date_level,
	dummy.denver_date,
	dummy.application_group,
	dummy.mso,
	dummy.acct_id,
	dummy.visit_id,
	dummy.application_version,
	dummy.user_package
GROUPING SETS(
	(dummy.date_level, dummy.denver_date, dummy.application_group, dummy.mso, dummy.acct_id, dummy.visit_id, dummy.application_version, dummy.user_package),  --11111111
	(dummy.date_level, dummy.denver_date)                                                                                                                     --11
)
;

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_dummy_daily PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_call_dummy_daily PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_dummy_daily PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_cross_platform_dummy PURGE;
