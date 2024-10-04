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

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/jdatehour-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_datehour AS 'Epoch_Datehour';

--Call
SELECT "\n\nFor 1a: ${env:TMP_db}.si_summary_call_resolution_tmp (${hiveconf:START_DATE})\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_call_resolution_tmp PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_call_resolution_tmp AS
SELECT DISTINCT
	call_start_timestamp_utc,
	encrypted_padded_account_number_256,
	account_agent_mso,
	COALESCE(primary_auto_disposition_cause, cause_description) AS cause_description,
	COALESCE(primary_auto_disposition_issue, issue_description) AS issue_description,
	resolution_description,
	CASE WHEN call_type = 'Self Install' AND resolution_description = 'Setup Truck Roll' THEN 1 ELSE 0 END AS truck_roll_resolution,
	CASE WHEN call_type = 'Self Install' THEN 1 ELSE 0 END AS self_install_call,
	CASE WHEN split_sum_desc = 'REPAIR' AND truck_roll_flag THEN 1 ELSE 0 END AS repair_truck_roll_call,
	CASE WHEN split_sum_desc = 'REPAIR' THEN 1 ELSE 0 END AS repair_call,
	epoch_converter(call_start_timestamp_utc, 'America/Denver') AS denver_date
FROM ${env:GLOBAL_db}.atom_cs_call_care_data_3
WHERE account_agent_mso IN('BHN', 'CHR', 'TWC')
	AND call_end_date_east >= '${hiveconf:START_DATE}'
	AND call_end_date_east <= '${hiveconf:END_DATE}'
;

--Get event data, add marks for each metric
SELECT "\n\nFor 1b: ${env:TMP_db}.si_summary_page_base_tmp (${hiveconf:START_DATE})\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_page_base_tmp PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_page_base_tmp AS
SELECT 
	received_timestamp,
	visit_id,
	device_id,
	acct_id,
	denver_date_hour,
	current_page_name,
	standardize_name,
	modal_name,
	error_type,
	error_code,
	feature_step_name,
	application_group,
	application_type,
	mso,
	user_package,
	sequence_number,
	event_tag,
	page_view_tag,
	initiated_self_installs,
	initiated_self_installs_new,
	initiated_self_installs_existing,
	account_status_past_due,
	account_status_current,
	terms_conditions_accept,
	terms_conditions_disagree,
	inactivity,
	abandonment_exiting_via_overflow_menu,
	CASE WHEN visit_last_event_flag = 1 
			AND (activation_success_full = 1 OR activation_success_partial = 1) AND rep_activation_self_install_call = 0 AND rep_activation_truck_roll_call = 0 
			THEN 1 
	ELSE 0 END AS activation_no_touch,
	rep_activation_user_call AS activation_user_call,
	rep_activation_self_install_call AS activation_self_install_call,
	rep_activation_truck_roll_call AS activation_truck_roll_call,
	activation_success_full,
	activation_success_partial,
	activation_failure,
	activation_failure_full,
	activation_failure_partial,
	CASE WHEN received_timestamp >= activation_timestamp_call AND rep_activation_self_install_call = 1 THEN 1 ELSE 0 END AS activation_failure_calls,
	CASE WHEN received_timestamp >= activation_timestamp_call AND rep_activation_truck_roll_call = 1 THEN 1 ELSE 0 END AS activation_failure_truck_rolls,
	re_entry,
	retry,
	errors_connection_issue,
	errors_equipment_setup,
	customer_satisfaction,
	customer_satisfaction_submitted,
	customer_satisfaction_submission_score,
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
	date_level,
	acct_division,
	acct_division_id,
	msa_first_initiated_self_installs_visit_id,
	msa_first_initiated_self_installs_timestamp,
	web_first_initiated_self_installs_visit_id,
	web_first_initiated_self_installs_timestamp,
	event_case_id,
	msa_complete_activation_self_installs_visit_id,
	msa_complete_activation_self_installs_timestamp,
	web_complete_activation_self_installs_visit_id,
	web_complete_activation_self_installs_timestamp,
	application_version,
	message_name,
	transaction_id,
	message_context,
	triggered_by,
	operation_success,
	api_response_time,
	api_http_verb,
	api_path,
	message_timestamp,
	errors_authentication_setup,
	errors_device_signal_setup,
	errors_other_setup,
	tv_activation_page_view,
	voice_activation_page_view,
	download_MSA,
	has_modem,
	has_router,
	has_tv,
	has_voice,
	modem_setup_success,
	router_setup_success,
	tv_setup_success,
	voice_setup_success,
	current_modem_setup_success,
	current_router_setup_success,
	current_tv_setup_success,
	current_voice_setup_success,
	prev_modem_setup_success,
	prev_router_setup_success,
	prev_tv_setup_success,
	prev_voice_setup_success,
	modem_activated_before_current_date,
	router_activated_before_current_date,
	tv_activated_before_current_date,
	voice_activated_before_current_date,
	current_modem_setup_failure,
	current_router_setup_failure,
	current_tv_setup_failure,
	current_voice_setup_failure,
	modem_activated,
	router_activated,
	tv_activated,
	voice_activated,
	modem_setup_failure,
	router_setup_failure,
	tv_setup_failure,
	voice_setup_failure,
	last_activation_page_sequence_number,
	additional_information,
	modem_activated_prev_visit_same_date,
	router_activated_prev_visit_same_date,
	tv_activated_prev_visit_same_date,
	voice_activated_prev_visit_same_date,
	abandonment_via_exit_setup,
	api_response_text,
	coam_device,
	whitelisted_coam_device,
	blacklisted_coam_device,
	minimally_qualified_coam_device,
	equipment_type_scp,
	equipment_type_monolithic,
	visit_eligible,
	code_version,
	CURRENT_TIMESTAMP AS record_load_timestamp,
	rep_repair_call AS repair_call,
	rep_repair_truck_roll_call AS repair_truck_roll_call,
	CASE WHEN visit_last_event_flag = 1 
			AND (activation_success_full = 1 OR activation_success_partial = 1) AND rep_repair_call = 0 AND rep_repair_truck_roll_call = 0 
			THEN 1 
	ELSE 0 END AS repair_no_touch,
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
	modem_count,
	router_count,
	tv_count,
	voice_count,
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
FROM(
	SELECT DISTINCT 
		event.received_timestamp,
		event.visit_id,
		event.device_id,
		event.acct_id,
		event.denver_date_hour,
		event.current_page_name,
		event.standardize_name,
		event.modal_name,
		event.error_type,
		event.error_code,
		event.feature_step_name,
		event.application_group,
		event.application_type,
		event.mso,
		event.user_package,
		event.sequence_number,
		event.event_tag,
		event.page_view_tag,
		event.initiated_self_installs,
		event.initiated_self_installs_new,
		event.initiated_self_installs_existing,
		event.account_status_past_due,
		event.account_status_current,
		event.terms_conditions_accept,
		event.terms_conditions_disagree,
		event.inactivity,
		event.abandonment_exiting_via_overflow_menu,
		event.activation_no_touch,
		event.activation_success_full,
		event.activation_success_partial,
		event.activation_failure,
		event.activation_failure_full,
		event.activation_failure_partial,
		event.activation_failure_calls,
		event.activation_failure_truck_rolls,
		event.re_entry,
		event.retry,
		event.errors_connection_issue,
		event.errors_equipment_setup,
		event.customer_satisfaction,
		event.customer_satisfaction_submitted,
		event.customer_satisfaction_submission_score,
		event.home_page_view,
		event.equipment_setup_page_view,
		event.terms_conditions_page_view,
		event.service_terms_conditions_page_view,
		event.equipment_checklist_page_view,
		event.modem_checklist_page_view,
		event.modem_activation_page_view,
		event.router_checklist_page_view,
		event.network_setup_page_view,
		event.router_activation_page_view,
		event.date_level,
		event.acct_division,
		event.acct_division_id,
		event.msa_first_initiated_self_installs_visit_id,
		event.msa_first_initiated_self_installs_timestamp,
		event.web_first_initiated_self_installs_visit_id,
		event.web_first_initiated_self_installs_timestamp,
		event.event_case_id,
		event.msa_complete_activation_self_installs_visit_id,
		event.msa_complete_activation_self_installs_timestamp,
		event.web_complete_activation_self_installs_visit_id,
		event.web_complete_activation_self_installs_timestamp,
		event.application_version,
		event.message_name,
		event.transaction_id,
		event.message_context,
		event.triggered_by,
		event.operation_success,
		event.api_response_time,
		event.api_http_verb,
		event.api_path,
		event.message_timestamp,
		event.errors_authentication_setup,
		event.errors_device_signal_setup,
		event.errors_other_setup,		
		event.tv_activation_page_view,
		event.voice_activation_page_view,
		event.download_MSA,
		event.has_modem,
		event.has_router,
		event.has_tv,
		event.has_voice,
		event.modem_setup_success,
		event.router_setup_success,
		event.tv_setup_success,
		event.voice_setup_success,
		event.current_modem_setup_success,
		event.current_router_setup_success,
		event.current_tv_setup_success,
		event.current_voice_setup_success,
		event.prev_modem_setup_success,
		event.prev_router_setup_success,
		event.prev_tv_setup_success,
		event.prev_voice_setup_success,
		event.modem_activated_before_current_date,
		event.router_activated_before_current_date,
		event.tv_activated_before_current_date,
		event.voice_activated_before_current_date,
		event.current_modem_setup_failure,
		event.current_router_setup_failure,
		event.current_tv_setup_failure,
		event.current_voice_setup_failure,
		event.modem_activated,
		event.router_activated,
		event.tv_activated,
		event.voice_activated,
		event.modem_setup_failure,
		event.router_setup_failure,
		event.tv_setup_failure,
		event.voice_setup_failure,
		event.last_activation_page_sequence_number,
		event.additional_information,
		event.modem_activated_prev_visit_same_date,
		event.router_activated_prev_visit_same_date,
		event.tv_activated_prev_visit_same_date,
		event.voice_activated_prev_visit_same_date,
		event.abandonment_via_exit_setup,
		event.api_response_text,
		event.coam_device,
		event.whitelisted_coam_device,
		event.blacklisted_coam_device,
		event.minimally_qualified_coam_device,
		event.equipment_type_scp,
		event.equipment_type_monolithic,
		event.visit_eligible,
		event.code_version,
		event.repair_no_touch,
		event.modem_activating,
		event.modem_connecting,
		event.router_activating,
		event.router_connecting,
		event.full_screen_takeover_page_view,
		event.activation_complete,
		event.modem_errors_preActivation,
		event.modem_errors_connection,
		event.modem_errors_activation,
		event.router_errors_preActivation,
		event.router_errors_connection,
		event.router_errors_activation,
		event.tv_errors_preActivation,
		event.tv_errors_connection,
		event.tv_errors_activation,
		event.errors_authentication,
		event.errors_preActivation,
		event.errors_activation,
		event.other_errors,
		event.interaction,
		event.modem_count,
		event.router_count,
		event.tv_count,
		event.voice_count,
		event.tv_checklist_page_view,
		event.tv_activating,
		event.tv_connecting,
		event.pending_modem_count,
		event.pending_router_count,
		event.pending_tv_count,
		event.pending_voice_count,
		event.modem_errors_postActivation,
		event.router_errors_postActivation,
		event.tv_errors_postActivation,
		event.voice_checklist_page_view,
		event.voice_activating,
		event.denver_date,
		CASE WHEN event.sequence_number = MAX(event.sequence_number) OVER (PARTITION BY event.acct_id, event.visit_id)
			THEN 1 
		ELSE 0 END visit_last_event_flag,
		
		--Self-Install calls
		COUNT(call.encrypted_padded_account_number_256) OVER (PARTITION BY event.visit_id, event.acct_id, event.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS rep_activation_user_call,
		MIN(CASE WHEN call.encrypted_padded_account_number_256 IS NOT NULL THEN call.call_start_timestamp_utc ELSE NULL END) OVER (PARTITION BY event.visit_id, event.acct_id, event.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS activation_timestamp_call,
		SUM(CASE WHEN call.encrypted_padded_account_number_256 IS NOT NULL AND call.self_install_call = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY event.visit_id, event.acct_id, event.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS rep_activation_self_install_call,
		SUM(CASE WHEN call.encrypted_padded_account_number_256 IS NOT NULL AND call.truck_roll_resolution = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY event.visit_id, event.acct_id, event.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS rep_activation_truck_roll_call,
		
		--Repair calls
		SUM(CASE WHEN call.encrypted_padded_account_number_256 IS NOT NULL AND call.repair_call = 1 THEN 1 ELSE 0 END) 
				OVER (PARTITION BY event.visit_id, event.acct_id, event.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS rep_repair_call,
		SUM(CASE WHEN call.encrypted_padded_account_number_256 IS NOT NULL AND call.repair_truck_roll_call = 1 THEN 1 ELSE 0 END) 
			OVER (PARTITION BY event.visit_id, event.acct_id, event.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS rep_repair_truck_roll_call
	FROM si_summary_page_base_master event
	LEFT JOIN ${env:TMP_db}.si_summary_call_resolution_tmp call
		ON event.acct_id = call.encrypted_padded_account_number_256
			AND CASE
					WHEN event.mso = 'L-CHTR' THEN 'CHR'
					WHEN event.mso = 'L-TWC' THEN 'TWC'
					WHEN event.mso = 'L-BHN' THEN 'BHN'
					ELSE ''
				END = call.account_agent_mso
			--Calls within 24 hours
			AND event.received_timestamp <= call.call_start_timestamp_utc
			AND (call.call_start_timestamp_utc - event.received_timestamp) <= 86400000
			AND event.event_tag = 'initiated_self_installs'
	WHERE
		event.denver_date = '${hiveconf:START_DATE}'
		AND event.date_level = '${hiveconf:grain}'
) call_data
;


SELECT "\n\nFor 1c: si_summary_page_base_master (${hiveconf:START_DATE})\n\n";

INSERT OVERWRITE TABLE si_summary_page_base_master PARTITION (denver_date)
SELECT *
FROM ${env:TMP_db}.si_summary_page_base_tmp
;

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_page_base_tmp PURGE;
