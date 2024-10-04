USE ${env:DASP_db};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join = true;

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.cbo.enable=true;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled = false;
set hive.vectorized.execution.reduce.groupby.enabled = false;
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

--Activation metrics
--Aggregate event data at visit level
SELECT "\n\nFor 1: ${env:TMP_db}.si_summary_agg_master_original\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_agg_master_original PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_agg_master_original AS
SELECT * 
FROM si_summary_agg_master 
WHERE denver_date = '${hiveconf:START_DATE}'
;


SELECT "\n\nFor 1a: si_summary_agg_master (Activation metrics)\n\n";

INSERT OVERWRITE TABLE si_summary_agg_master PARTITION (denver_date)
SELECT
	page_base.visit_id,
	page_base.device_id,
	page_base.acct_id,
	page_base.application_group,
	page_base.application_type,
	page_base.mso,
	page_base.user_package,
	MAX(page_base.initiated_self_installs) AS initiated_self_installs,
	MAX(page_base.initiated_self_installs_new) AS initiated_self_installs_new,
	MAX(page_base.initiated_self_installs_existing) AS initiated_self_installs_existing,
	MAX(page_base.account_status_past_due) AS account_status_past_due,
	MAX(page_base.account_status_current) AS account_status_current,
	MAX(page_base.terms_conditions_accept) AS terms_conditions_accept,
	MAX(page_base.terms_conditions_disagree) AS terms_conditions_disagree,
	CAST(SUM(page_base.inactivity) AS INT) AS inactivity,
	CAST(SUM(page_base.abandonment_exiting_via_overflow_menu) AS INT) AS abandonment_exiting_via_overflow_menu,
	MAX(page_base.activation_no_touch) AS activation_no_touch,
	MAX(page_base.activation_user_call) AS activation_user_call,
	MAX(page_base.activation_self_install_call) AS activation_self_install_call,
	MAX(page_base.activation_truck_roll_call) AS activation_truck_roll_call,
	MAX(page_base.activation_success_full) AS activation_success_full,
	MAX(page_base.activation_success_partial) AS activation_success_partial,
	MAX(page_base.activation_failure) AS activation_failure,
	MAX(page_base.activation_failure_full) AS activation_failure_full,
	MAX(page_base.activation_failure_partial) AS activation_failure_partial,
	MAX(page_base.activation_failure_calls) AS activation_failure_calls,
	MAX(page_base.activation_failure_truck_rolls) AS activation_failure_truck_rolls,
	CAST(SUM(page_base.re_entry) AS INT) AS re_entry,
	CAST(SUM(page_base.retry) AS INT) AS retry,
	MAX(page_base.errors_connection_issue) AS errors_connection_issue,
	MAX(page_base.errors_equipment_setup) AS errors_equipment_setup,
	MAX(page_base.customer_satisfaction) AS customer_satisfaction,
	MAX(page_base.customer_satisfaction_submitted) AS customer_satisfaction_submitted,
	MAX(page_base.customer_satisfaction_submission_score) AS customer_satisfaction_submission_score,
	MAX(CASE WHEN page_base.customer_satisfaction_submission_score IS NULL THEN 0 
			 WHEN page_base.customer_satisfaction_submission_score > 0 THEN 1 
		ELSE 0 END) AS customer_satisfaction_completed_survey,
	CAST(SUM(page_base.home_page_view) AS INT) AS home_page_view,
	CAST(SUM(page_base.equipment_setup_page_view) AS INT) AS equipment_setup_page_view,
	CAST(SUM(page_base.terms_conditions_page_view) AS INT) AS terms_conditions_page_view,
	CAST(SUM(page_base.service_terms_conditions_page_view) AS INT) AS service_terms_conditions_page_view,
	CAST(SUM(page_base.equipment_checklist_page_view) AS INT) AS equipment_checklist_page_view,
	CAST(SUM(page_base.modem_checklist_page_view) AS INT) AS modem_checklist_page_view,
	CAST(SUM(page_base.modem_activation_page_view) AS INT) AS modem_activation_page_view,
	CAST(SUM(page_base.router_checklist_page_view) AS INT) AS router_checklist_page_view,
	CAST(SUM(page_base.network_setup_page_view) AS INT) AS network_setup_page_view,
	CAST(SUM(page_base.router_activation_page_view) AS INT) AS router_activation_page_view,
	0 AS rescue_no_touch,
	0 AS rescue_call,
	0 AS rescue_truck,
	page_base.date_level,
	'Activation' AS metric_type,
	page_base.acct_division,
	page_base.acct_division_id,
	MAX(page_base.errors_authentication_setup) AS errors_authentication_setup,
	MAX(page_base.errors_device_signal_setup) AS errors_device_signal_setup,
	MAX(page_base.errors_other_setup) AS errors_other_setup,
	CAST(SUM(page_base.tv_activation_page_view) AS INT) AS tv_activation_page_view,
	CAST(SUM(page_base.voice_activation_page_view) AS INT) AS voice_activation_page_view,
	CAST(SUM(page_base.download_MSA) AS INT) AS download_MSA,
	--avg_transaction_duration and avg_api_response_time are populated in Composite Score query
	AVG(cs.avg_transaction_duration) AS avg_transaction_duration,
	AVG(cs.avg_api_response_time) AS avg_api_response_time,
	CAST(SUM(page_base.abandonment_via_exit_setup) AS INT) AS abandonment_via_exit_setup,
	MAX(page_base.coam_device) AS coam_device,
	MAX(page_base.whitelisted_coam_device) AS whitelisted_coam_device,
	MAX(page_base.blacklisted_coam_device) AS blacklisted_coam_device,
	MAX(page_base.minimally_qualified_coam_device) AS minimally_qualified_coam_device,
	MAX(page_base.equipment_type_scp) AS equipment_type_scp,
	MAX(page_base.equipment_type_monolithic) AS equipment_type_monolithic,
	MAX(page_base.visit_eligible) AS visit_eligible,
	MAX(page_base.repair_call) AS repair_call,
	MAX(page_base.repair_truck_roll_call) AS repair_truck_roll_call,
	0 AS repair_rescue_no_touch,
	0 AS repair_rescue_call,
	0 AS repair_rescue_truck,
	
	0 AS job_class_category_code_t,
	0 AS job_class_category_code_all,
	
	MAX(page_base.repair_no_touch) AS repair_no_touch,
	NULL AS encrypted_account_key_256,
	NULL AS work_order_date,
	CAST(SUM(page_base.modem_activating) AS INT) AS modem_activating,
	CAST(SUM(page_base.modem_connecting) AS INT) AS modem_connecting,
	CAST(SUM(page_base.router_activating) AS INT) AS router_activating,
	CAST(SUM(page_base.router_connecting) AS INT) AS router_connecting,
	CAST(SUM(page_base.full_screen_takeover_page_view) AS INT) AS full_screen_takeover_page_view,
	MAX(page_base.activation_complete) AS activation_complete,
	CAST(SUM(page_base.modem_errors_preActivation) AS INT) AS modem_errors_preActivation,
	CAST(SUM(page_base.modem_errors_connection) AS INT) AS modem_errors_connection,
	CAST(SUM(page_base.modem_errors_activation) AS INT) AS modem_errors_activation,
	CAST(SUM(page_base.router_errors_preActivation) AS INT) AS router_errors_preActivation,
	CAST(SUM(page_base.router_errors_connection) AS INT) AS router_errors_connection,
	CAST(SUM(page_base.router_errors_activation) AS INT) AS router_errors_activation,
	CAST(SUM(page_base.tv_errors_preActivation) AS INT) AS tv_errors_preActivation,
	CAST(SUM(page_base.tv_errors_connection) AS INT) AS tv_errors_connection,
	CAST(SUM(page_base.tv_errors_activation) AS INT) AS tv_errors_activation,
	CAST(SUM(page_base.errors_authentication) AS INT) AS errors_authentication,
	CAST(SUM(page_base.errors_preActivation) AS INT) AS errors_preActivation,
	CAST(SUM(page_base.errors_activation) AS INT) AS errors_activation,
	CAST(SUM(page_base.other_errors) AS INT) AS other_errors,
	CAST(SUM(page_base.interaction) AS INT) AS interaction,
	MAX(page_base.modem_setup_success) AS modem_setup_success,
	MAX(page_base.router_setup_success) AS router_setup_success,
	MAX(page_base.tv_setup_success) AS tv_setup_success,
	MAX(page_base.voice_setup_success) AS voice_setup_success,
	MAX(page_base.modem_count) + MAX(page_base.router_count) + MAX(page_base.tv_count) + MAX(page_base.voice_count) AS total_devices,
	SUM(page_base.tv_checklist_page_view) AS tv_checklist_page_view,
	SUM(page_base.tv_activating) AS tv_activating,
	SUM(page_base.tv_connecting) AS tv_connecting,
	MAX(page_base.pending_modem_count) AS pending_modem_count,
	MAX(page_base.pending_router_count) AS pending_router_count,
	MAX(page_base.pending_tv_count) AS pending_tv_count,
	MAX(page_base.pending_voice_count) AS pending_voice_count,
	SUM(page_base.modem_errors_postActivation) AS modem_errors_postActivation,
	SUM(page_base.router_errors_postActivation) AS router_errors_postActivation,
	SUM(page_base.tv_errors_postActivation) AS tv_errors_postActivation,
	SUM(page_base.voice_checklist_page_view) AS voice_checklist_page_view,
	SUM(page_base.voice_activating) AS voice_activating,
	page_base.denver_date
FROM si_summary_page_base_master page_base
LEFT JOIN (SELECT
				denver_date,
				visit_id,
				application_group,
				user_package,
				date_level,
				AVG(avg_transaction_duration) AS avg_transaction_duration,
				AVG(avg_api_response_time) AS avg_api_response_time
			FROM ${env:TMP_db}.si_composite_score_activation_events_daily
			GROUP BY denver_date,
					visit_id,
					application_group,
					user_package,
					date_level
) cs
	ON cs.denver_date = page_base.denver_date
		AND cs.visit_id = page_base.visit_id
		AND cs.application_group = page_base.application_group
		AND cs.user_package = page_base.user_package
		AND cs.date_level = page_base.date_level
WHERE page_base.date_level = '${hiveconf:grain}'
	AND page_base.denver_date = '${hiveconf:START_DATE}'
GROUP BY page_base.visit_id,
	page_base.device_id,
	page_base.acct_id,
	page_base.application_group,
	page_base.application_type,
	page_base.mso,
	page_base.user_package,
	page_base.date_level,
	page_base.denver_date,
	page_base.acct_division,
	page_base.acct_division_id
;

INSERT INTO TABLE si_summary_agg_master PARTITION (denver_date)
SELECT 	
	visit_id,
	device_id,
	acct_id,
	application_group,
	application_type,
	mso,
	user_package,
	initiated_self_installs,
	initiated_self_installs_new,
	initiated_self_installs_existing,
	account_status_past_due,
	account_status_current,
	terms_conditions_accept,
	terms_conditions_disagree,
	inactivity,
	abandonment_exiting_via_overflow_menu,
	activation_no_touch,
	activation_user_call,
	activation_self_install_call,
	activation_truck_roll_call,
	activation_success_full,
	activation_success_partial,
	activation_failure,
	activation_failure_full,
	activation_failure_partial,
	activation_failure_calls,
	activation_failure_truck_rolls,
	re_entry,
	retry,
	errors_connection_issue,
	errors_equipment_setup,
	customer_satisfaction,
	customer_satisfaction_submitted,
	customer_satisfaction_submission_score,
	customer_satisfaction_completed_survey,
	home_page_view,
	equipment_setup_page_view,
	terms_conditions_page_view,
	service_terms_conditions_page_view,
	equipment_checklist_page_view,
	modem_checklist_page_view,
	modem_activation_page_view,
	router_checklist_page_view,
	network_setup_page_view,
	router_activation_page_view,
	rescue_no_touch,
	rescue_call,
	rescue_truck,
	date_level,
	metric_type,
	acct_division,
	acct_division_id,
	errors_authentication_setup,
	errors_device_signal_setup,
	errors_other_setup,
	tv_activation_page_view,
	voice_activation_page_view,
	download_MSA,	
	--avg_transaction_duration and avg_api_response_time are populated in Composite Score query
	avg_transaction_duration,
	avg_api_response_time,
	abandonment_via_exit_setup,
	coam_device,
	whitelisted_coam_device,
	blacklisted_coam_device,
	minimally_qualified_coam_device,
	equipment_type_scp,
	equipment_type_monolithic,
	visit_eligible,
	repair_call,
	repair_truck_roll_call,
	repair_rescue_no_touch,
	repair_rescue_call,
	repair_rescue_truck,
	job_class_category_code_t,
	job_class_category_code_all,
	repair_no_touch,
	encrypted_account_key_256,
	work_order_date,
	modem_activating,
	modem_connecting,
	router_activating,
	router_connecting,
	full_screen_takeover_page_view,
	activation_complete,
	modem_errors_preActivation,
	modem_errors_connection,
	modem_errors_activation,
	router_errors_preActivation,
	router_errors_connection,
	router_errors_activation,
	tv_errors_preActivation,
	tv_errors_connection,
	tv_errors_activation,
	errors_authentication,
	errors_preActivation,
	errors_activation,
	other_errors,
	interaction,
	modem_setup_success,
	router_setup_success,
	tv_setup_success,
	voice_setup_success,
	total_devices,
	tv_checklist_page_view,
	tv_activating,
	tv_connecting,
	pending_modem_count,
	pending_router_count,
	pending_tv_count,
	pending_voice_count,
	modem_errors_postActivation,
	router_errors_postActivation,
	tv_errors_postActivation,
	voice_checklist_page_view,
	voice_activating,
	denver_date
FROM ${env:TMP_db}.si_summary_agg_master_original
WHERE denver_date = '${hiveconf:START_DATE}'
	AND date_level != '${hiveconf:grain}'
;


--Program 
SELECT "\n\nFor 2a: ${env:TMP_db}.si_work_orders (Program metrics)\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_work_orders PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_work_orders AS
WITH wo AS (
	SELECT legacy_company, job_class_category_code, job_status_code, job_entry_date_eastern, service_code, encrypted_account_key_256, partition_date_denver
	FROM ${env:GLOBAL_db}.atom_work_orders
	WHERE partition_date_denver >= '${hiveconf:PRIOR_8DAYS_START_DATE}'
		AND partition_date_denver <= '${hiveconf:PRIOR_8DAYS_END_DATE}'
		AND legacy_company IN('BHN', 'CHR', 'TWC')
)
SELECT orders.legacy_company, orders.job_entry_date_eastern, orders.encrypted_account_key_256, orders.partition_date_denver
	, snapshot.encrypted_padded_account_number_256
	, MAX(CASE WHEN repair_orders.job_class_category_code = 'T' THEN 1 ELSE 0 END) AS job_class_category_code_t
	, MAX(CASE WHEN repair_orders.job_class_category_code IS NOT NULL THEN 1 ELSE 0 END) AS job_class_category_code_all
FROM wo orders
LEFT JOIN wo repair_orders
	ON orders.encrypted_account_key_256 = repair_orders.encrypted_account_key_256
LEFT JOIN (SELECT DISTINCT encrypted_padded_account_number_256, encrypted_account_key_256
			FROM ${env:GLOBAL_db}.atom_accounts_snapshot
			WHERE partition_date_denver >= '${hiveconf:PRIOR_8DAYS_START_DATE}'
				AND partition_date_denver <= '${hiveconf:PRIOR_8DAYS_END_DATE}'
				AND legacy_company IN('BHN', 'CHR', 'TWC')
				AND account_status = 'CONNECTED'
				AND extract_type = 'RESIDENTIAL'
	) snapshot
	ON orders.encrypted_account_key_256 = snapshot.encrypted_account_key_256
WHERE orders.partition_date_denver = '${hiveconf:PRIOR_8DAYS_START_DATE}'
	AND orders.legacy_company IN('BHN', 'CHR', 'TWC')
	AND orders.service_code in('INS21', 'INS22', 'INS27', 'INS30', 'V0107', 'T0125', 'T0126')
	AND orders.job_entry_date_eastern = TRANSLATE(orders.partition_date_denver, '-', '')
GROUP BY orders.legacy_company
	, orders.job_entry_date_eastern
	, orders.encrypted_account_key_256
	, orders.partition_date_denver
	, snapshot.encrypted_padded_account_number_256
;


SELECT "\n\nFor 2b: ${env:TMP_db}.si_summary_call_8days (Program metrics)\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_call_8days PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_call_8days AS
SELECT call_start_date_utc, encrypted_account_key_256, encrypted_padded_account_number_256, --resolution_description, cause_description, issue_description,
	account_agent_mso,
	
	--Self-Install calls
	CASE WHEN call_type = 'Self Install' THEN 1 ELSE 0 END AS self_install_call,
	CASE WHEN call_type = 'Self Install' AND resolution_description = 'Setup Truck Roll' THEN 1 ELSE 0 END AS truck_roll_call,
	
	--Repair calls
	CASE WHEN split_sum_desc = 'REPAIR' THEN 1 ELSE 0 END AS repair_call,
	CASE WHEN split_sum_desc = 'REPAIR' AND truck_roll_flag THEN 1 ELSE 0 END AS repair_truck_roll_call	
FROM ${env:GLOBAL_db}.atom_cs_call_care_data_3
WHERE account_agent_mso IN('BHN', 'CHR', 'TWC')
	AND call_end_date_east >= '${hiveconf:PRIOR_8DAYS_START_DATE}' 
	AND call_end_date_east <= '${hiveconf:PRIOR_8DAYS_END_DATE}'
;


SELECT "\n\nFor 2c: ${env:TMP_db}.si_work_orders_calls (Program metrics)\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_work_orders_calls PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_work_orders_calls AS
SELECT 
	  orders.legacy_company
	, orders.job_entry_date_eastern
	, orders.partition_date_denver
	, orders.encrypted_account_key_256
	, orders.job_class_category_code_t
	, orders.job_class_category_code_all
	, MAX(NVL(calls.encrypted_padded_account_number_256, orders.encrypted_padded_account_number_256)) AS encrypted_padded_account_number_256
	, NVL(SUM(calls.self_install_call), 0) AS self_install_call
	, NVL(SUM(calls.truck_roll_call), 0) AS truck_roll_call
	, NVL(SUM(repair_call), 0) AS repair_call
	, NVL(SUM(repair_truck_roll_call), 0) AS repair_truck_roll_call
	, COUNT(*) AS count_calls
FROM ${env:TMP_db}.si_work_orders orders
LEFT JOIN ${env:TMP_db}.si_summary_call_8days calls
	ON orders.encrypted_account_key_256 = calls.encrypted_account_key_256
GROUP BY orders.legacy_company
	, orders.job_entry_date_eastern
	, orders.partition_date_denver
	, orders.encrypted_account_key_256
	, orders.job_class_category_code_t
	, orders.job_class_category_code_all
;



SELECT "\n\nFor 2d: si_summary_agg_master (Program metrics)\n\n";

INSERT INTO TABLE si_summary_agg_master PARTITION (denver_date)
SELECT 
	NULL AS visit_id,
	NULL AS device_id,
	activation.acct_id,
	activation.application_group,
	activation.application_type,
	activation.mso,
	activation.user_package,
	NVL(MAX(activation.initiated_self_installs), 0) AS initiated_self_installs,
	NVL(MAX(activation.initiated_self_installs_new), 0) AS initiated_self_installs_new,
	NVL(MAX(activation.initiated_self_installs_existing), 0) AS initiated_self_installs_existing,
	NVL(MAX(activation.account_status_past_due), 0) AS account_status_past_due,
	NVL(MAX(activation.account_status_current), 0) AS account_status_current,
	NVL(MAX(activation.terms_conditions_accept), 0) AS terms_conditions_accept,
	NVL(MAX(activation.terms_conditions_disagree), 0) AS terms_conditions_disagree,
	NVL(CAST(SUM(activation.inactivity) AS INT), 0) AS inactivity,
	NVL(CAST(SUM(activation.abandonment_exiting_via_overflow_menu) AS INT), 0) AS abandonment_exiting_via_overflow_menu,
	NVL(CASE WHEN NVL(MAX(activation.initiated_self_installs), 0) > 0 
				AND NVL(MAX(activation.activation_self_install_call), 0) = 0 AND NVL(MAX(activation.activation_truck_roll_call), 0) = 0
				THEN 1
		ELSE 0 END, 0) AS activation_no_touch,
	
	--Self-Install calls within 24 hours (Activation metrics)
	NVL(CAST(SUM(activation.activation_user_call) AS INT), 0) AS activation_user_call,
	NVL(CAST(SUM(activation.activation_self_install_call) AS INT), 0) AS activation_self_install_call,
	NVL(CAST(SUM(activation.activation_truck_roll_call) AS INT), 0) AS activation_truck_roll_call,
	
	NVL(MAX(activation.activation_success_full), 0) AS activation_success_full,
	NVL(MAX(activation.activation_success_partial), 0) AS activation_success_partial,
	NVL(MAX(activation.activation_failure), 0) AS activation_failure,
	NVL(MAX(activation.activation_failure_full), 0) AS activation_failure_full,
	NVL(MAX(activation.activation_failure_partial), 0) AS activation_failure_partial,
	NVL(MAX(activation.activation_failure_calls), 0) AS activation_failure_calls,
	NVL(MAX(activation.activation_failure_truck_rolls), 0) AS activation_failure_truck_rolls,
	NVL(CAST(SUM(activation.re_entry) AS INT), 0) AS re_entry,
	NVL(CAST(SUM(activation.retry) AS INT), 0) AS retry,
	NVL(MAX(activation.errors_connection_issue), 0) AS errors_connection_issue,
	NVL(MAX(activation.errors_equipment_setup), 0) AS errors_equipment_setup,
	NVL(MAX(activation.customer_satisfaction), 0) AS customer_satisfaction,
	NVL(MAX(activation.customer_satisfaction_submitted), 0) AS customer_satisfaction_submitted,
	MAX(activation.customer_satisfaction_submission_score) AS customer_satisfaction_submission_score,
	MAX(CASE WHEN NVL(activation.customer_satisfaction_submission_score, 0) > 0 THEN 1 ELSE 0 END) AS customer_satisfaction_completed_survey,
	NVL(CAST(SUM(activation.home_page_view) AS INT), 0) AS home_page_view,
	NVL(CAST(SUM(activation.equipment_setup_page_view) AS INT), 0) AS equipment_setup_page_view,
	NVL(CAST(SUM(activation.terms_conditions_page_view) AS INT), 0) AS terms_conditions_page_view,
	NVL(CAST(SUM(activation.service_terms_conditions_page_view) AS INT), 0) AS service_terms_conditions_page_view,
	NVL(CAST(SUM(activation.equipment_checklist_page_view) AS INT), 0) AS equipment_checklist_page_view,
	NVL(CAST(SUM(activation.modem_checklist_page_view) AS INT), 0) AS modem_checklist_page_view,
	NVL(CAST(SUM(activation.modem_activation_page_view) AS INT), 0) AS modem_activation_page_view,
	NVL(CAST(SUM(activation.router_checklist_page_view) AS INT), 0) AS router_checklist_page_view,
	NVL(CAST(SUM(activation.network_setup_page_view) AS INT), 0) AS network_setup_page_view,
	NVL(CAST(SUM(activation.router_activation_page_view) AS INT), 0) AS router_activation_page_view,
	
	--Self-Install calls within 8 days after WO (Program metrics)
	CASE WHEN NVL(MAX(orders.self_install_call), 0) = 0 AND NVL(MAX(orders.truck_roll_call), 0) = 0 THEN 1 ELSE 0 END AS rescue_no_touch,
	MAX(orders.self_install_call) AS rescue_call, 
	MAX(orders.truck_roll_call) AS rescue_truck,
	
	'${hiveconf:grain}' AS date_level,
	'Program' AS metric_type,
	activation.acct_division,
	activation.acct_division_id,
	NVL(MAX(activation.errors_authentication_setup), 0) AS errors_authentication_setup,
	NVL(MAX(activation.errors_device_signal_setup), 0) AS errors_device_signal_setup,
	NVL(MAX(activation.errors_other_setup), 0) AS errors_other_setup,
	NVL(CAST(SUM(activation.tv_activation_page_view) AS INT), 0) AS tv_activation_page_view,
	NVL(CAST(SUM(activation.voice_activation_page_view) AS INT), 0) AS voice_activation_page_view,
	NVL(CAST(SUM(activation.download_MSA) AS INT), 0) AS download_MSA,
	--avg_transaction_duration and avg_api_response_time are populated in Composite Score query
	AVG(activation.avg_transaction_duration) AS avg_transaction_duration,
	AVG(activation.avg_api_response_time) AS avg_api_response_time,
	NVL(CAST(SUM(activation.abandonment_via_exit_setup) AS INT), 0) AS abandonment_via_exit_setup,
	NVL(MAX(activation.coam_device), 0) AS coam_device,
	NVL(MAX(activation.whitelisted_coam_device), 0) AS whitelisted_coam_device,
	NVL(MAX(activation.blacklisted_coam_device), 0) AS blacklisted_coam_device,
	NVL(MAX(activation.minimally_qualified_coam_device), 0) AS minimally_qualified_coam_device,
	NVL(MAX(activation.equipment_type_scp), 0) AS equipment_type_scp,
	NVL(MAX(activation.equipment_type_monolithic), 0) AS equipment_type_monolithic,
	NVL(MAX(activation.visit_eligible), 0) AS visit_eligible,
	
	--Repair calls within 24 hours (Activation metrics)
	NVL(CAST(SUM(activation.repair_call) AS INT), 0) AS repair_call,
	NVL(CAST(SUM(activation.repair_truck_roll_call) AS INT), 0) AS repair_truck_roll_call,
	
	--Repair calls within 8 days after WO (Program metrics)
	CASE WHEN MAX(orders.repair_call) = 0 AND NOT (MAX(orders.repair_truck_roll_call) > 0 AND MAX(orders.job_class_category_code_t) > 0) THEN 1 ELSE 0 END AS repair_rescue_no_touch,
	MAX(orders.repair_call) AS repair_rescue_call,
	CASE WHEN MAX(orders.repair_truck_roll_call) > 0 AND MAX(orders.job_class_category_code_t) > 0 THEN MAX(orders.repair_truck_roll_call) ELSE 0 END AS repair_rescue_truck,
	MAX(orders.job_class_category_code_t) AS job_class_category_code_t,
	MAX(orders.job_class_category_code_all) AS job_class_category_code_all,
	
	--Repair calls within 24 hours (Activation metrics)
	CASE WHEN NVL(MAX(activation.initiated_self_installs), 0) = 1
			AND NVL(SUM(activation.repair_call), 0) = 0 AND NVL(SUM(activation.repair_truck_roll_call), 0) = 0 THEN 1
	ELSE 0 END AS repair_no_touch,
	
	orders.encrypted_account_key_256,
	orders.partition_date_denver AS work_order_date,
	NVL(CAST(SUM(activation.modem_activating) AS INT), 0) AS modem_activating,
	NVL(CAST(SUM(activation.modem_connecting) AS INT), 0) AS modem_connecting,
	NVL(CAST(SUM(activation.router_activating) AS INT), 0) AS router_activating,
	NVL(CAST(SUM(activation.router_connecting) AS INT), 0) AS router_connecting,
	NVL(CAST(SUM(activation.full_screen_takeover_page_view) AS INT), 0) AS full_screen_takeover_page_view,
	NVL(MAX(activation.activation_complete), 0) AS activation_complete,
	NVL(CAST(SUM(activation.modem_errors_preActivation) AS INT), 0) AS modem_errors_preActivation,
	NVL(CAST(SUM(activation.modem_errors_connection) AS INT), 0) AS modem_errors_connection,
	NVL(CAST(SUM(activation.modem_errors_activation) AS INT), 0) AS modem_errors_activation,
	NVL(CAST(SUM(activation.router_errors_preActivation) AS INT), 0) AS router_errors_preActivation,
	NVL(CAST(SUM(activation.router_errors_connection) AS INT), 0) AS router_errors_connection,
	NVL(CAST(SUM(activation.router_errors_activation) AS INT), 0) AS router_errors_activation,
	NVL(CAST(SUM(activation.tv_errors_preActivation) AS INT), 0) AS tv_errors_preActivation,
	NVL(CAST(SUM(activation.tv_errors_connection) AS INT), 0) AS tv_errors_connection,
	NVL(CAST(SUM(activation.tv_errors_activation) AS INT), 0) AS tv_errors_activation,
	NVL(CAST(SUM(activation.errors_authentication) AS INT), 0) AS errors_authentication,
	NVL(CAST(SUM(activation.errors_preActivation) AS INT), 0) AS errors_preActivation,
	NVL(CAST(SUM(activation.errors_activation) AS INT), 0) AS errors_activation,
	NVL(CAST(SUM(activation.other_errors) AS INT), 0) AS other_errors,
	NVL(CAST(SUM(activation.interaction) AS INT), 0) AS interaction,
	NVL(CAST(MAX(activation.modem_setup_success) AS INT), 0) AS modem_setup_success,
	NVL(CAST(MAX(activation.router_setup_success) AS INT), 0) AS router_setup_success,
	NVL(CAST(MAX(activation.tv_setup_success) AS INT), 0) AS tv_setup_success,
	NVL(CAST(MAX(activation.voice_setup_success) AS INT), 0) AS voice_setup_success,
	NVL(CAST(MAX(activation.total_devices) AS INT), 0) AS total_devices,
	NVL(CAST(SUM(activation.tv_checklist_page_view) AS INT), 0) AS tv_checklist_page_view,
	NVL(CAST(SUM(activation.tv_activating) AS INT), 0) AS tv_activating,
	NVL(CAST(SUM(activation.tv_connecting) AS INT), 0) AS tv_connecting,
	NVL(MAX(activation.pending_modem_count), 0) AS pending_modem_count,
	NVL(MAX(activation.pending_router_count), 0) AS pending_router_count,
	NVL(MAX(activation.pending_tv_count), 0) AS pending_tv_count,
	NVL(MAX(activation.pending_voice_count), 0) AS pending_voice_count,
	NVL(CAST(SUM(activation.modem_errors_postActivation) AS INT), 0) AS modem_errors_postActivation,
	NVL(CAST(SUM(activation.router_errors_postActivation) AS INT), 0) AS router_errors_postActivation,
	NVL(CAST(SUM(activation.tv_errors_postActivation) AS INT), 0) AS tv_errors_postActivation,
	NVL(CAST(SUM(activation.voice_checklist_page_view) AS INT), 0) AS voice_checklist_page_view,
	NVL(CAST(SUM(activation.voice_activating) AS INT), 0) AS voice_activating,
	'${hiveconf:START_DATE}' AS denver_date
FROM ${env:TMP_db}.si_work_orders_calls orders
LEFT JOIN si_summary_agg_master activation 
	ON activation.denver_date >= '${hiveconf:PRIOR_8DAYS_START_DATE}'
		AND activation.denver_date <= '${hiveconf:PRIOR_8DAYS_END_DATE}'
		AND activation.acct_id = orders.encrypted_padded_account_number_256
		AND date_level = 'daily'
		and metric_type = 'Activation'
GROUP BY 
	activation.acct_id,
	activation.application_group,
	activation.application_type,
	activation.mso,
	activation.user_package,
	activation.date_level,
	activation.acct_division,
	activation.acct_division_id,
	orders.encrypted_account_key_256,
	orders.partition_date_denver
;


--Aggregate call data
SELECT "\n\nFor 3a: si_summary_call_agg_master\n\n";

INSERT OVERWRITE TABLE si_summary_call_agg_master PARTITION (denver_date)
SELECT
	summary.visit_id,
	summary.device_id,
	summary.acct_id,
	summary.application_group,
	summary.application_type,
	summary.mso,
	call.issue_description,
	call.cause_description,
	call.resolution_description,
	CAST(COUNT(DISTINCT CONCAT(CAST(call.call_start_timestamp_utc AS STRING), call.encrypted_padded_account_number_256)) AS INT) AS call_count,
	summary.date_level,
	summary.denver_date
FROM (SELECT DISTINCT received_timestamp, visit_id, device_id, acct_id, application_group, application_type, mso, date_level, msa_first_initiated_self_installs_timestamp, denver_date
	FROM si_summary_page_base_master
	WHERE date_level = '${hiveconf:grain}'
		AND denver_date >= '${hiveconf:START_DATE}'
		AND denver_date < '${hiveconf:END_DATE}'
		AND event_tag = 'initiated_self_installs'
) summary
INNER JOIN ${env:TMP_db}.si_summary_call_resolution_tmp call
	ON summary.acct_id = call.encrypted_padded_account_number_256
		AND CASE
				WHEN summary.mso = 'L-CHTR' THEN 'CHR'
				WHEN summary.mso = 'L-TWC' THEN 'TWC'
				WHEN summary.mso = 'L-BHN' THEN 'BHN'
				ELSE ''
			END = call.account_agent_mso
WHERE
	(summary.received_timestamp <= call.call_start_timestamp_utc
		AND (call.call_start_timestamp_utc - summary.received_timestamp) <= 86400000)
GROUP BY summary.visit_id,
	summary.device_id,
	summary.acct_id,
	summary.application_group,
	summary.application_type,
	summary.mso,
	call.cause_description,
	call.issue_description,
	call.resolution_description,
	summary.date_level,
    summary.denver_date
;

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_agg_master_original PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_call_resolution_tmp PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_work_orders PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_work_orders_calls PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_account_key PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_call_8days PURGE;

--this table is used in Composite Score query
DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_activation_events_daily PURGE;
