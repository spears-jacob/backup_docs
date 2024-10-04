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

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/jdatehour-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_datehour AS 'Epoch_Datehour';


--Activation metrics
--Aggregate event data at visit level
SELECT "\n\nFor 1a: ${env:TMP_db}.si_summary_agg_master_monthly_tmp\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_agg_master_monthly_tmp PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_agg_master_monthly_tmp AS
SELECT
	visit_id,
	device_id,
	acct_id,
	application_group,
	application_type,
	mso,
	user_package,
	MAX(initiated_self_installs) AS initiated_self_installs,
	MAX(initiated_self_installs_new) AS initiated_self_installs_new,
	MAX(initiated_self_installs_existing) AS initiated_self_installs_existing,
	MAX(account_status_past_due) AS account_status_past_due,
	MAX(account_status_current) AS account_status_current,
	MAX(terms_conditions_accept) AS terms_conditions_accept,
	MAX(terms_conditions_disagree) AS terms_conditions_disagree,
	CAST(SUM(inactivity) AS INT) AS inactivity,
	CAST(SUM(abandonment_exiting_via_overflow_menu) AS INT) AS abandonment_exiting_via_overflow_menu,
	MAX(activation_no_touch) AS activation_no_touch,
	MAX(activation_user_call) AS activation_user_call,
	MAX(activation_self_install_call) AS activation_self_install_call,
	MAX(activation_truck_roll_call) AS activation_truck_roll_call,
	MAX(activation_success_full) AS activation_success_full,
	MAX(activation_success_partial) AS activation_success_partial,
	MAX(activation_failure) AS activation_failure,
	MAX(activation_failure_full) AS activation_failure_full,
	MAX(activation_failure_partial) AS activation_failure_partial,
	MAX(activation_failure_calls) AS activation_failure_calls,
	MAX(activation_failure_truck_rolls) AS activation_failure_truck_rolls,
	CAST(SUM(re_entry) AS INT) AS re_entry,
	CAST(SUM(retry) AS INT) AS retry,
	MAX(errors_connection_issue) AS errors_connection_issue,
	MAX(errors_equipment_setup) AS errors_equipment_setup,
	MAX(customer_satisfaction) AS customer_satisfaction,
	MAX(customer_satisfaction_submitted) AS customer_satisfaction_submitted,
	MAX(customer_satisfaction_submission_score) AS customer_satisfaction_submission_score,
	MAX(CASE WHEN customer_satisfaction_submission_score IS NULL THEN 0 
			 WHEN customer_satisfaction_submission_score > 0 THEN 1 
		ELSE 0 END) AS customer_satisfaction_completed_survey,
	CAST(SUM(home_page_view) AS INT) AS home_page_view,
	CAST(SUM(equipment_setup_page_view) AS INT) AS equipment_setup_page_view,
	CAST(SUM(terms_conditions_page_view) AS INT) AS terms_conditions_page_view,
	CAST(SUM(service_terms_conditions_page_view) AS INT) AS service_terms_conditions_page_view,
	CAST(SUM(equipment_checklist_page_view) AS INT) AS equipment_checklist_page_view,
	CAST(SUM(modem_checklist_page_view) AS INT) AS modem_checklist_page_view,
	CAST(SUM(modem_activation_page_view) AS INT) AS modem_activation_page_view,
	CAST(SUM(router_checklist_page_view) AS INT) AS router_checklist_page_view,
	CAST(SUM(network_setup_page_view) AS INT) AS network_setup_page_view,
	CAST(SUM(router_activation_page_view) AS INT) AS router_activation_page_view,
	0 AS rescue_no_touch,
	0 AS rescue_call,
	0 AS rescue_truck,
	'${hiveconf:grain}' AS date_level,
	'Activation' AS metric_type,
	acct_division,
	acct_division_id,
	MAX(errors_authentication_setup) AS errors_authentication_setup,
	MAX(errors_device_signal_setup) AS errors_device_signal_setup,
	MAX(errors_other_setup) AS errors_other_setup,
	CAST(SUM(tv_activation_page_view) AS INT) AS tv_activation_page_view,
	CAST(SUM(voice_activation_page_view) AS INT) AS voice_activation_page_view,
	CAST(SUM(download_MSA) AS INT) AS download_MSA,
	--avg_transaction_duration and avg_api_response_time are populated in Composite Score query
	AVG(avg_transaction_duration) AS avg_transaction_duration,
	AVG(avg_api_response_time) AS avg_api_response_time,
	CAST(SUM(abandonment_via_exit_setup) AS INT) AS abandonment_via_exit_setup,
	MAX(coam_device) AS coam_device,
	MAX(whitelisted_coam_device) AS whitelisted_coam_device,
	MAX(blacklisted_coam_device) AS blacklisted_coam_device,
	MAX(minimally_qualified_coam_device) AS minimally_qualified_coam_device,
	MAX(equipment_type_scp) AS equipment_type_scp,
	MAX(equipment_type_monolithic) AS equipment_type_monolithic,
	MAX(visit_eligible) AS visit_eligible,
	MAX(repair_call) AS repair_call,
	MAX(repair_truck_roll_call) AS repair_truck_roll_call,
	0 AS repair_rescue_no_touch,
	0 AS repair_rescue_call,
	0 AS repair_rescue_truck,
	0 AS job_class_category_code_t,
	0 AS job_class_category_code_all,
	MAX(repair_no_touch) AS repair_no_touch,
	CAST(NULL AS STRING) AS encrypted_account_key_256,
	CAST(NULL AS STRING) AS work_order_date,
	CAST(SUM(modem_activating) AS INT) AS modem_activating,
	CAST(SUM(modem_connecting) AS INT) AS modem_connecting,
	CAST(SUM(router_activating) AS INT) AS router_activating,
	CAST(SUM(router_connecting) AS INT) AS router_connecting,
	CAST(SUM(full_screen_takeover_page_view) AS INT) AS full_screen_takeover_page_view,
	MAX(activation_complete) AS activation_complete,
	CAST(SUM(modem_errors_preActivation) AS INT) AS modem_errors_preActivation,
	CAST(SUM(modem_errors_connection) AS INT) AS modem_errors_connection,
	CAST(SUM(modem_errors_activation) AS INT) AS modem_errors_activation,
	CAST(SUM(router_errors_preActivation) AS INT) AS router_errors_preActivation,
	CAST(SUM(router_errors_connection) AS INT) AS router_errors_connection,
	CAST(SUM(router_errors_activation) AS INT) AS router_errors_activation,
	CAST(SUM(tv_errors_preActivation) AS INT) AS tv_errors_preActivation,
	CAST(SUM(tv_errors_connection) AS INT) AS tv_errors_connection,
	CAST(SUM(tv_errors_activation) AS INT) AS tv_errors_activation,
	CAST(SUM(errors_authentication) AS INT) AS errors_authentication,
	CAST(SUM(errors_preActivation) AS INT) AS errors_preActivation,
	CAST(SUM(errors_activation) AS INT) AS errors_activation,
	CAST(SUM(other_errors) AS INT) AS other_errors,
	CAST(SUM(interaction) AS INT) AS interaction,
	MAX(modem_setup_success) AS modem_setup_success,
	MAX(router_setup_success) AS router_setup_success,
	MAX(tv_setup_success) AS tv_setup_success,
	MAX(voice_setup_success) AS voice_setup_success,
	MAX(total_devices) AS total_devices,
	SUM(tv_checklist_page_view) AS tv_checklist_page_view,
	SUM(tv_activating) AS tv_activating,
	SUM(tv_connecting) AS tv_connecting,
	MAX(pending_modem_count) AS pending_modem_count,
	MAX(pending_router_count) AS pending_router_count,
	MAX(pending_tv_count) AS pending_tv_count,
	MAX(pending_voice_count) AS pending_voice_count,
	SUM(modem_errors_postActivation) AS modem_errors_postActivation,
	SUM(router_errors_postActivation) AS router_errors_postActivation,
	SUM(tv_errors_postActivation) AS tv_errors_postActivation,
	SUM(voice_checklist_page_view) AS voice_checklist_page_view,
	SUM(voice_activating) AS voice_activating,
	'${hiveconf:START_DATE}' AS denver_date
FROM si_summary_agg_master
WHERE date_level = 'daily'
	AND denver_date >= '${hiveconf:START_DATE}'
	AND denver_date < '${hiveconf:END_DATE}'
	AND acct_id IS NOT NULL
	AND metric_type = 'Activation'
GROUP BY visit_id,
	device_id,
	acct_id,
	application_group,
	application_type,
	mso,
	user_package,
	date_level,
	denver_date,
	acct_division,
	acct_division_id
;


SELECT "\n\nFor 1b: si_summary_agg_master (Activation metrics)\n\n";

INSERT INTO TABLE ${env:TMP_db}.si_summary_agg_master_monthly_tmp
SELECT 	*
FROM si_summary_agg_master
WHERE denver_date = '${hiveconf:START_DATE}'
	AND date_level != '${hiveconf:grain}'
;


INSERT OVERWRITE TABLE si_summary_agg_master PARTITION (denver_date)
SELECT *
FROM ${env:TMP_db}.si_summary_agg_master_monthly_tmp
;



--Program 
SELECT "\n\nFor 2a: ${env:TMP_db}.si_work_orders_monthly (Program metrics)\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_work_orders_monthly PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_work_orders_monthly AS
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
WHERE orders.partition_date_denver >= '${hiveconf:START_DATE}'
	AND orders.partition_date_denver < '${hiveconf:END_DATE}'
	AND orders.legacy_company IN('BHN', 'CHR', 'TWC')
	AND orders.service_code in('INS21', 'INS22', 'INS27', 'INS30', 'V0107', 'T0125', 'T0126')
	AND orders.job_entry_date_eastern = TRANSLATE(orders.partition_date_denver, '-', '')
GROUP BY orders.legacy_company
	, orders.job_entry_date_eastern
	, orders.encrypted_account_key_256
	, orders.partition_date_denver
	, snapshot.encrypted_padded_account_number_256
;


SELECT "\n\nFor 2b: ${env:TMP_db}.si_summary_call_8days_monthly (Program metrics)\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_call_8days_monthly PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_call_8days_monthly AS
SELECT call_end_date_east, call_start_date_utc, encrypted_account_key_256, encrypted_padded_account_number_256,
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


SELECT "\n\nFor 2c: ${env:TMP_db}.si_work_orders_calls_monthly (Program metrics)\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_work_orders_calls_monthly PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_work_orders_calls_monthly AS
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
FROM ${env:TMP_db}.si_work_orders_monthly orders
LEFT JOIN ${env:TMP_db}.si_summary_call_8days_monthly calls
	ON orders.encrypted_account_key_256 = calls.encrypted_account_key_256
	AND calls.call_end_date_east >= orders.partition_date_denver
	AND DATEDIFF(DATE(calls.call_end_date_east), DATE(orders.partition_date_denver)) <= 8
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
	program.encrypted_padded_account_number_256 AS acct_id,
	program.application_group,
	program.application_type,
	program.mso,
	program.user_package,
	NVL(MAX(program.initiated_self_installs), 0) AS initiated_self_installs,
	NVL(MAX(program.initiated_self_installs_new), 0) AS initiated_self_installs_new,
	NVL(MAX(program.initiated_self_installs_existing), 0) AS initiated_self_installs_existing,
	NVL(MAX(program.account_status_past_due), 0) AS account_status_past_due,
	NVL(MAX(program.account_status_current), 0) AS account_status_current,
	NVL(MAX(program.terms_conditions_accept), 0) AS terms_conditions_accept,
	NVL(MAX(program.terms_conditions_disagree), 0) AS terms_conditions_disagree,
	NVL(CAST(SUM(program.inactivity) AS INT), 0) AS inactivity,
	NVL(CAST(SUM(program.abandonment_exiting_via_overflow_menu) AS INT), 0) AS abandonment_exiting_via_overflow_menu,
	NVL(CASE WHEN NVL(MAX(program.initiated_self_installs), 0) = 1
				AND NVL(MAX(program.activation_self_install_call), 0) = 0 AND NVL(MAX(program.activation_truck_roll_call), 0) = 0
				THEN 1
		ELSE 0 END, 0) AS activation_no_touch,
	
	--Self-Install calls within 24 hours (Activation metrics)
	NVL(CAST(SUM(program.activation_user_call) AS INT), 0) AS activation_user_call,
	NVL(CAST(SUM(program.activation_self_install_call) AS INT), 0) AS activation_self_install_call,
	NVL(CAST(SUM(program.activation_truck_roll_call) AS INT), 0) AS activation_truck_roll_call,
	
	NVL(MAX(program.activation_success_full), 0) AS activation_success_full,
	
	CASE WHEN NVL(MAX(program.activation_success_full), 0) > 0 THEN 0 ELSE NVL(MAX(program.activation_success_partial), 0) END AS activation_success_partial,
	
	NVL(MAX(program.activation_failure), 0) AS activation_failure,
	NVL(MAX(program.activation_failure_full), 0) AS activation_failure_full,
	NVL(MAX(program.activation_failure_partial), 0) AS activation_failure_partial,
	NVL(MAX(program.activation_failure_calls), 0) AS activation_failure_calls,
	NVL(MAX(program.activation_failure_truck_rolls), 0) AS activation_failure_truck_rolls,
	NVL(CAST(SUM(program.re_entry) AS INT), 0) AS re_entry,
	NVL(CAST(SUM(program.retry) AS INT), 0) AS retry,
	NVL(MAX(program.errors_connection_issue), 0) AS errors_connection_issue,
	NVL(MAX(program.errors_equipment_setup), 0) AS errors_equipment_setup,
	NVL(MAX(program.customer_satisfaction), 0) AS customer_satisfaction,
	NVL(MAX(program.customer_satisfaction_submitted), 0) AS customer_satisfaction_submitted,
	MAX(program.customer_satisfaction_submission_score) AS customer_satisfaction_submission_score,
	MAX(CASE WHEN NVL(program.customer_satisfaction_submission_score, 0) > 0 THEN 1 ELSE 0 END) AS customer_satisfaction_completed_survey,
	NVL(CAST(SUM(program.home_page_view) AS INT), 0) AS home_page_view,
	NVL(CAST(SUM(program.equipment_setup_page_view) AS INT), 0) AS equipment_setup_page_view,
	NVL(CAST(SUM(program.terms_conditions_page_view) AS INT), 0) AS terms_conditions_page_view,
	NVL(CAST(SUM(program.service_terms_conditions_page_view) AS INT), 0) AS service_terms_conditions_page_view,
	NVL(CAST(SUM(program.equipment_checklist_page_view) AS INT), 0) AS equipment_checklist_page_view,
	NVL(CAST(SUM(program.modem_checklist_page_view) AS INT), 0) AS modem_checklist_page_view,
	NVL(CAST(SUM(program.modem_activation_page_view) AS INT), 0) AS modem_activation_page_view,
	NVL(CAST(SUM(program.router_checklist_page_view) AS INT), 0) AS router_checklist_page_view,
	NVL(CAST(SUM(program.network_setup_page_view) AS INT), 0) AS network_setup_page_view,
	NVL(CAST(SUM(program.router_activation_page_view) AS INT), 0) AS router_activation_page_view,
	
	--Self-Install calls within 8 days after WO (Program metrics)
	CASE WHEN NVL(MAX(program.orders_self_install_call), 0) = 0 AND NVL(MAX(program.orders_truck_roll_call), 0) = 0 THEN 1 ELSE 0 END AS rescue_no_touch,
	MAX(program.orders_self_install_call) AS rescue_call, 
	MAX(program.orders_truck_roll_call) AS rescue_truck,
	
	'${hiveconf:grain}' AS date_level,
	'Program' AS metric_type,
	program.acct_division,
	program.acct_division_id,
	NVL(MAX(program.errors_authentication_setup), 0) AS errors_authentication_setup,
	NVL(MAX(program.errors_device_signal_setup), 0) AS errors_device_signal_setup,
	NVL(MAX(program.errors_other_setup), 0) AS errors_other_setup,
	NVL(CAST(SUM(program.tv_activation_page_view) AS INT), 0) AS tv_activation_page_view,
	NVL(CAST(SUM(program.voice_activation_page_view) AS INT), 0) AS voice_activation_page_view,
	NVL(CAST(SUM(program.download_MSA) AS INT), 0) AS download_MSA,
	--avg_transaction_duration and avg_api_response_time are populated in Composite Score query
	AVG(program.avg_transaction_duration) AS avg_transaction_duration,
	AVG(program.avg_api_response_time) AS avg_api_response_time,
	NVL(CAST(SUM(program.abandonment_via_exit_setup) AS INT), 0) AS abandonment_via_exit_setup,
	NVL(MAX(program.coam_device), 0) AS coam_device,
	NVL(MAX(program.whitelisted_coam_device), 0) AS whitelisted_coam_device,
	NVL(MAX(program.blacklisted_coam_device), 0) AS blacklisted_coam_device,
	NVL(MAX(program.minimally_qualified_coam_device), 0) AS minimally_qualified_coam_device,
	NVL(MAX(program.equipment_type_scp), 0) AS equipment_type_scp,
	NVL(MAX(program.equipment_type_monolithic), 0) AS equipment_type_monolithic,
	NVL(MAX(program.visit_eligible), 0) AS visit_eligible,
	
	--Repair calls within 24 hours (Activation metrics)
	NVL(CAST(SUM(program.repair_call) AS INT), 0) AS repair_call,
	NVL(CAST(SUM(program.repair_truck_roll_call) AS INT), 0) AS repair_truck_roll_call,
	
	--Repair calls within 8 days after WO (Program metrics)
	CASE WHEN MAX(program.orders_repair_call) = 0 AND NOT (MAX(program.orders_repair_truck_roll_call) > 0 AND MAX(program.job_class_category_code_t) > 0) THEN 1 ELSE 0 END AS repair_rescue_no_touch,
	MAX(program.orders_repair_call) AS repair_rescue_call,
	CASE WHEN MAX(program.orders_repair_truck_roll_call) > 0 AND MAX(program.job_class_category_code_t) > 0 THEN MAX(program.orders_repair_truck_roll_call) ELSE 0 END AS repair_rescue_truck,
	MAX(program.job_class_category_code_t) AS job_class_category_code_t,
	MAX(program.job_class_category_code_all) AS job_class_category_code_all,
	
	--Repair calls within 24 hours (Activation metrics)
	CASE WHEN NVL(MAX(program.initiated_self_installs), 0) = 1 
			AND NVL(SUM(program.repair_call), 0) = 0 AND NVL(SUM(program.repair_truck_roll_call), 0) = 0 THEN 1
	ELSE 0 END AS repair_no_touch,
	
	program.encrypted_account_key_256,
	program.partition_date_denver AS work_order_date,
	NVL(CAST(SUM(program.modem_activating) AS INT), 0) AS modem_activating,
	NVL(CAST(SUM(program.modem_connecting) AS INT), 0) AS modem_connecting,
	NVL(CAST(SUM(program.router_activating) AS INT), 0) AS router_activating,
	NVL(CAST(SUM(program.router_connecting) AS INT), 0) AS router_connecting,
	NVL(CAST(SUM(program.full_screen_takeover_page_view) AS INT), 0) AS full_screen_takeover_page_view,
	NVL(MAX(program.activation_complete), 0) AS activation_complete,
	NVL(CAST(SUM(program.modem_errors_preActivation) AS INT), 0) AS modem_errors_preActivation,
	NVL(CAST(SUM(program.modem_errors_connection) AS INT), 0) AS modem_errors_connection,
	NVL(CAST(SUM(program.modem_errors_activation) AS INT), 0) AS modem_errors_activation,
	NVL(CAST(SUM(program.router_errors_preActivation) AS INT), 0) AS router_errors_preActivation,
	NVL(CAST(SUM(program.router_errors_connection) AS INT), 0) AS router_errors_connection,
	NVL(CAST(SUM(program.router_errors_activation) AS INT), 0) AS router_errors_activation,
	NVL(CAST(SUM(program.tv_errors_preActivation) AS INT), 0) AS tv_errors_preActivation,
	NVL(CAST(SUM(program.tv_errors_connection) AS INT), 0) AS tv_errors_connection,
	NVL(CAST(SUM(program.tv_errors_activation) AS INT), 0) AS tv_errors_activation,
	NVL(CAST(SUM(program.errors_authentication) AS INT), 0) AS errors_authentication,
	NVL(CAST(SUM(program.errors_preActivation) AS INT), 0) AS errors_preActivation,
	NVL(CAST(SUM(program.errors_activation) AS INT), 0) AS errors_activation,
	NVL(CAST(SUM(program.other_errors) AS INT), 0) AS other_errors,
	NVL(CAST(SUM(program.interaction) AS INT), 0) AS interaction,
	NVL(MAX(program.modem_setup_success), 0) AS modem_setup_success,
	NVL(MAX(program.router_setup_success), 0) AS router_setup_success,
	NVL(MAX(program.tv_setup_success), 0) AS tv_setup_success,
	NVL(MAX(program.voice_setup_success), 0) AS voice_setup_success,
	NVL(CAST(MAX(program.total_devices) AS INT), 0) AS total_devices,
	NVL(CAST(SUM(program.tv_checklist_page_view) AS INT), 0) AS tv_checklist_page_view,
	NVL(CAST(SUM(program.tv_activating) AS INT), 0) AS tv_activating,
	NVL(CAST(SUM(program.tv_connecting) AS INT), 0) AS tv_connecting,
	NVL(MAX(program.pending_modem_count), 0) AS pending_modem_count,
	NVL(MAX(program.pending_router_count), 0) AS pending_router_count,
	NVL(MAX(program.pending_tv_count), 0) AS pending_tv_count,
	NVL(MAX(program.pending_voice_count), 0) AS pending_voice_count,
	NVL(CAST(SUM(program.modem_errors_postActivation) AS INT), 0) AS modem_errors_postActivation,
	NVL(CAST(SUM(program.router_errors_postActivation) AS INT), 0) AS router_errors_postActivation,
	NVL(CAST(SUM(program.tv_errors_postActivation) AS INT), 0) AS tv_errors_postActivation,
	NVL(CAST(SUM(program.voice_checklist_page_view) AS INT), 0) AS voice_checklist_page_view,
	NVL(CAST(SUM(program.voice_activating) AS INT), 0) AS voice_activating,
	'${hiveconf:START_DATE}' AS denver_date
FROM (
		SELECT
			activation.application_group,
			activation.application_type,
			activation.mso,
			activation.user_package,
			activation.initiated_self_installs,
			activation.initiated_self_installs_new,
			activation.initiated_self_installs_existing,
			activation.account_status_past_due,
			activation.account_status_current,
			activation.terms_conditions_accept,
			activation.terms_conditions_disagree,
			activation.inactivity,
			activation.abandonment_exiting_via_overflow_menu,
			activation.activation_no_touch,
			activation.activation_user_call,
			activation.activation_self_install_call,
			activation.activation_truck_roll_call,
			activation.activation_success_full,
			activation.activation_success_partial,
			activation.activation_failure,
			activation.activation_failure_full,
			activation.activation_failure_partial,
			activation.activation_failure_calls,
			activation.activation_failure_truck_rolls,
			activation.re_entry,
			activation.retry,
			activation.errors_connection_issue,
			activation.errors_equipment_setup,
			activation.customer_satisfaction,
			activation.customer_satisfaction_submitted,
			LAST_VALUE(CASE WHEN activation.customer_satisfaction_submission_score IS NOT NULL AND activation.customer_satisfaction_submission_score > 0 THEN activation.customer_satisfaction_submission_score ELSE NULL END, TRUE) 
				OVER (PARTITION BY activation.acct_id, activation.application_group, activation.application_type, activation.user_package ORDER BY activation.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS customer_satisfaction_submission_score,
			activation.customer_satisfaction_completed_survey,
			activation.home_page_view,
			activation.equipment_setup_page_view,
			activation.terms_conditions_page_view,
			activation.service_terms_conditions_page_view,
			activation.equipment_checklist_page_view,
			activation.modem_checklist_page_view,
			activation.modem_activation_page_view,
			activation.router_checklist_page_view,
			activation.network_setup_page_view,
			CASE WHEN activation.mso = 'L-CHTR' AND activation.application_group = 'SelfInstall' AND activation.modem_activation_page_view = 1 AND LOWER(activation.user_package) LIKE '%router%' THEN 1 ELSE activation.router_activation_page_view END AS router_activation_page_view,
			-- activation.rescue_no_touch,
			-- activation.rescue_call,
			-- activation.rescue_truck,
			activation.date_level,
			activation.metric_type,
			activation.acct_division,
			activation.acct_division_id,
			activation.errors_authentication_setup,
			activation.errors_device_signal_setup,
			activation.errors_other_setup,
			activation.tv_activation_page_view,
			activation.voice_activation_page_view,
			activation.download_MSA,	
			--avg_transaction_duration and avg_api_response_time are populated in Composite Score query
			activation.avg_transaction_duration,
			activation.avg_api_response_time,
			activation.abandonment_via_exit_setup,
			activation.coam_device,
			activation.whitelisted_coam_device,
			activation.blacklisted_coam_device,
			activation.minimally_qualified_coam_device,
			activation.equipment_type_scp,
			activation.equipment_type_monolithic,
			activation.visit_eligible,
			activation.repair_call,
			activation.repair_truck_roll_call,
			activation.repair_rescue_no_touch,
			activation.repair_rescue_call,
			activation.repair_rescue_truck,
			activation.repair_no_touch,
			activation.modem_activating,
			activation.modem_connecting,
			activation.router_activating,
			activation.router_connecting,
			activation.full_screen_takeover_page_view,
			activation.activation_complete,
			activation.modem_errors_preActivation,
			activation.modem_errors_connection,
			activation.modem_errors_activation,
			activation.router_errors_preActivation,
			activation.router_errors_connection,
			activation.router_errors_activation,
			activation.tv_errors_preActivation,
			activation.tv_errors_connection,
			activation.tv_errors_activation,
			activation.errors_authentication,
			activation.errors_preActivation,
			activation.errors_activation,
			activation.other_errors,
			activation.interaction,
			activation.modem_setup_success,
			activation.router_setup_success,
			activation.tv_setup_success,
			activation.voice_setup_success,
			activation.total_devices,
			activation.tv_checklist_page_view,
			activation.tv_activating,
			activation.tv_connecting,
			activation.pending_modem_count,
			activation.pending_router_count,
			activation.pending_tv_count,
			activation.pending_voice_count,
			activation.modem_errors_postActivation,
			activation.router_errors_postActivation,
			activation.tv_errors_postActivation,
			activation.voice_checklist_page_view,
			activation.voice_activating,
			activation.denver_date,
			orders.encrypted_padded_account_number_256,
			orders.job_class_category_code_t,
			orders.job_class_category_code_all,
			orders.self_install_call AS orders_self_install_call,
			orders.truck_roll_call AS orders_truck_roll_call,
			orders.repair_call AS orders_repair_call,
			orders.repair_truck_roll_call AS orders_repair_truck_roll_call,
			orders.encrypted_account_key_256,
			orders.partition_date_denver
		FROM ${env:TMP_db}.si_work_orders_calls_monthly orders
		LEFT JOIN si_summary_agg_master activation
			ON activation.acct_id = orders.encrypted_padded_account_number_256
			AND activation.denver_date >= '${hiveconf:PRIOR_8DAYS_START_DATE}'
			AND activation.denver_date <= '${hiveconf:PRIOR_8DAYS_END_DATE}'
			AND activation.date_level = 'daily'
			AND activation.metric_type = 'Activation'
			AND activation.denver_date >= orders.partition_date_denver
			AND DATEDIFF(DATE(activation.denver_date), DATE(orders.partition_date_denver)) <= 8

	) program
GROUP BY 
	program.encrypted_padded_account_number_256,
	program.application_group,
	program.application_type,
	program.mso,
	program.user_package,
	program.date_level,
	program.acct_division,
	program.acct_division_id,
	program.encrypted_account_key_256,
	program.partition_date_denver
;

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_agg_master_monthly_tmp PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_work_orders_monthly PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_call_8days_monthly PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_work_orders_calls_monthly PURGE;
