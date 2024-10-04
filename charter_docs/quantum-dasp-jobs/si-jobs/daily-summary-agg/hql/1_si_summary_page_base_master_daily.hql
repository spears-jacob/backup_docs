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

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
CREATE TEMPORARY FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256';

--Eligible Summary Accounts
SELECT "\n\nFor 1: ${env:TMP_db}.si_summary_eligible_accounts\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_eligible_accounts PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_eligible_accounts AS
SELECT DISTINCT 
	acct_id,
	application_group,
	
	--Self-Installs Initiation
	FIRST_VALUE(msa_first_initiated_self_installs_visit_id, TRUE) OVER (PARTITION BY acct_id ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msa_first_initiated_self_installs_visit_id,
	FIRST_VALUE(web_first_initiated_self_installs_visit_id, TRUE) OVER (PARTITION BY acct_id ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS web_first_initiated_self_installs_visit_id,
	
	FIRST_VALUE(msa_first_initiated_self_installs_timestamp, TRUE) OVER (PARTITION BY acct_id ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msa_first_initiated_self_installs_timestamp,
	FIRST_VALUE(web_first_initiated_self_installs_timestamp, TRUE) OVER (PARTITION BY acct_id ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS web_first_initiated_self_installs_timestamp,
	
	--Self-Installs Completion
	FIRST_VALUE(msa_complete_activation_self_installs_visit_id, TRUE) OVER (PARTITION BY acct_id ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msa_complete_activation_self_installs_visit_id,
	FIRST_VALUE(web_complete_activation_self_installs_visit_id, TRUE) OVER (PARTITION BY acct_id ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS web_complete_activation_self_installs_visit_id,
	
	FIRST_VALUE(msa_complete_activation_self_installs_timestamp, TRUE) OVER (PARTITION BY acct_id ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msa_complete_activation_self_installs_timestamp,
	FIRST_VALUE(web_complete_activation_self_installs_timestamp, TRUE) OVER (PARTITION BY acct_id ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS web_complete_activation_self_installs_timestamp,
	
	--Self-Installs interaction
	MAX(interaction) OVER (PARTITION BY application_group, acct_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS interaction,
	
	--User Package
	LAST_VALUE(user_package, TRUE) OVER (PARTITION BY acct_id, application_group ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prev_user_package,
	
	MAX(CASE WHEN visit_id = msa_customer_satisfaction_submission_visit_id THEN received_timestamp ELSE NULL END) OVER (PARTITION BY acct_id ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msa_customer_satisfaction_submission_timestamp,
	
	--device/service already activated in previous visits
	MAX(CASE WHEN event_tag LIKE '%modem_setup_success%' THEN 1 ELSE 0 END) OVER (PARTITION BY acct_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS modem_activated_before_current_date,
	MAX(CASE WHEN event_tag LIKE '%router_setup_success%' THEN 1 ELSE 0 END) OVER (PARTITION BY acct_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS router_activated_before_current_date,
	MAX(CASE WHEN application_group = 'MySpectrum' AND event_tag LIKE '%router_setup_success%' THEN 1 ELSE 0 END) OVER (PARTITION BY acct_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msa_router_activated_before_current_date,
	MAX(CASE WHEN event_tag LIKE '%tv_setup_success%' THEN 1 ELSE 0 END) OVER (PARTITION BY acct_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS tv_activated_before_current_date,
	MAX(CASE WHEN event_tag LIKE '%voice_setup_success%' THEN 1 ELSE 0 END) OVER (PARTITION BY acct_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS voice_activated_before_current_date,
	
	--CSAT from multiple devices
	COUNT(DISTINCT CASE WHEN application_group = 'MySpectrum' AND event_tag IN('initiated_self_installs', 're_entry') THEN device_id ELSE NULL END) OVER (PARTITION BY acct_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msa_distinct_devices_initiation,
	COUNT(DISTINCT CASE WHEN application_group = 'MySpectrum' AND event_tag = 'customer_satisfaction' THEN device_id ELSE NULL END) OVER (PARTITION BY acct_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msa_distinct_devices_feedback,
	
	--last additionalInformation field additionalInformation
	LAST_VALUE(CASE WHEN application_group = 'MySpectrum'
							AND additional_information RLIKE '(modem|internet|router|wifi|receiver|stb|video|tv|phone|voice)'
							AND event_case_id IN('mySpectrum_pageView_equipment_selfInstall_equipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment')
							THEN LOWER(additional_information)
						WHEN application_group = 'SelfInstall'
							AND additional_information RLIKE '(modem|internet|router|wifi|receiver|stb|video|tv|phone|voice)'
							AND event_case_id IN('selfInstall_pageView_equipment_selfInstallStartPage', 'selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
							THEN LOWER(additional_information)
				 ELSE NULL END
				 , TRUE)
			OVER (PARTITION BY application_group, acct_id ORDER BY message_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_additional_information
FROM (
	SELECT 
		received_timestamp,
		acct_id,
		visit_id,
		device_id,
		application_group,
		user_package,
		event_tag,
		event_case_id,
		msa_first_initiated_self_installs_visit_id,
		web_first_initiated_self_installs_visit_id,
		msa_first_initiated_self_installs_timestamp,
		web_first_initiated_self_installs_timestamp,
		msa_complete_activation_self_installs_visit_id,
		msa_complete_activation_self_installs_timestamp,
		web_complete_activation_self_installs_visit_id,
		web_complete_activation_self_installs_timestamp,
		message_timestamp,
		additional_information,
		interaction,
		
		--CSAT Submission
		LAST_VALUE(CASE WHEN event_tag = 'customer_satisfaction' AND application_group = 'MySpectrum' THEN visit_id ELSE NULL END, TRUE) OVER (PARTITION BY acct_id ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msa_customer_satisfaction_submission_visit_id
	FROM si_summary_page_base_master
	WHERE denver_date < '${hiveconf:START_DATE}'
) first_event
;


--Self-Install events
SELECT "\n\nFor 1: si_core_quantum_events\n\n";

INSERT OVERWRITE TABLE si_core_quantum_events PARTITION (partition_date_utc, partition_date_hour_utc, visit__application_details__application_name)
SELECT
		received__timestamp,
		visit__visit_id,
		visit__account__details__mso,
		visit__application_details__application_type,
		visit__application_details__app_version,
		application__error__error_type,
		application__error__error_code,
		application__api__response_text,
		application__api__response_time_ms,
		application__api__path,
		application__api__http_verb,
		message__name,
		message__sequence_number,
		message__timestamp,
		message__triggered_by,
		message__feature__feature_name,
		state__view__current_page__page_name,
		state__view__current_page__elements__standardized_name,
		state__view__modal__name,
		operation__success,
		message__context,
		message__feature__transaction_id,
		message__feature__feature_step_name,
		message__event_case_id,
		state__view__current_page__elements__element_string_value,
		state__view__current_page__additional_information,
		state__view__current_page__page_title,
		operation__user_entry__numeric,
		operation__user_entry__survey_id,
		visit__device__enc_uuid,
		visit__account__enc_account_number,
		visit__account__enc_account_billing_division,
		visit__account__enc_account_billing_division_id,
		message__time_since_last_event_ms,
		operation__additional_information,
		partition_date_utc,
		partition_date_hour_utc,
		visit__application_details__application_name
FROM (
	SELECT
		received__timestamp,
		visit__visit_id,
		visit__account__details__mso,
		visit__application_details__application_type,
		visit__application_details__app_version,
		application__error__error_type,
		application__error__error_code,
		REPLACE(application__api__response_text, chr(10), '') AS application__api__response_text,
		application__api__response_time_ms,
		application__api__path,
		application__api__http_verb,
		message__name,
		message__sequence_number,
		message__timestamp,
		message__triggered_by,
		message__feature__feature_name,
		state__view__current_page__page_name,
		state__view__current_page__elements__standardized_name,
		state__view__modal__name,
		operation__success,
		message__context,
		message__feature__transaction_id,
		message__feature__feature_step_name,
		message__event_case_id,
		state__view__current_page__elements__element_string_value,
		REPLACE(state__view__current_page__additional_information, ' ', '') AS state__view__current_page__additional_information,
		CASE aes_decrypt256(state__view__current_page__enc_page_title)
            WHEN 'equipmentSelfInstall' THEN 'equipmentSelfInstall'
            ELSE NULL
        END AS state__view__current_page__page_title,  -- column data was encrypted late Dec. 2021 due to containing random PII. We will decrypt only relevant values for ETL processing.
		operation__user_entry__numeric,
		operation__user_entry__survey_id,
		visit__device__enc_uuid,
		visit__account__enc_account_number,
		visit__account__enc_account_billing_division,
		visit__account__enc_account_billing_division_id,
		message__time_since_last_event_ms,
		operation__additional_information,
		partition_date_utc,
		partition_date_hour_utc,
		visit__application_details__application_name,
		MAX(CASE WHEN message__event_case_id LIKE 'selfInstall_%' OR message__event_case_id LIKE 'mySpectrum_%' 
                       OR message__event_case_id LIKE 'IdentityManagement_%' 
                        THEN 1 
			ELSE 0 END)
			 OVER (PARTITION BY visit__application_details__application_name, visit__account__enc_account_number) AS event_case_eligible
	FROM ${env:GLOBAL_db}.core_quantum_events_sspp
	WHERE
		partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}'
		AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}'
		AND visit__application_details__application_type  IN ('iOS', 'Android', 'Web')
		AND visit__application_details__application_name IN ('MySpectrum', 'SelfInstall', 'IDManagement')
		AND visit__account__enc_account_number IS NOT NULL
) eligible
WHERE event_case_eligible = 1
;



--Eligible Self-Install events
SELECT "\n\nFor 1: ${env:TMP_db}.si_cqe_eligible_accounts\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_cqe_eligible_accounts PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_cqe_eligible_accounts AS
SELECT
	cqe_eligible.received__timestamp,
	cqe_eligible.denver_date,
	cqe_eligible.denver_date_hour,
	cqe_eligible.visit__visit_id,
	cqe_eligible.visit__account__details__mso,
	cqe_eligible.visit__application_details__application_type,
	cqe_eligible.visit__application_details__app_version,
	cqe_eligible.application__error__error_type,
	cqe_eligible.application__error__error_code,
	cqe_eligible.application__error__error_code_numeric,
	cqe_eligible.application__api__response_text,
	cqe_eligible.application__api__response_time_ms,
	cqe_eligible.application__api__path,
	cqe_eligible.application__api__http_verb,
	cqe_eligible.message__name,
	cqe_eligible.message__sequence_number,
	cqe_eligible.message__timestamp,
	cqe_eligible.message__triggered_by,
	cqe_eligible.message__feature__feature_name,
	cqe_eligible.state__view__current_page__page_name,
	cqe_eligible.state__view__current_page__elements__standardized_name,
	cqe_eligible.state__view__modal__name,
	cqe_eligible.operation__success,
	cqe_eligible.message__context,
	cqe_eligible.message__feature__transaction_id,
	cqe_eligible.message__feature__feature_step_name,
	cqe_eligible.message__event_case_id,
	cqe_eligible.state__view__current_page__elements__element_string_value,
	cqe_eligible.state__view__current_page__additional_information,
	cqe_eligible.state__view__current_page__page_title,
	cqe_eligible.operation__user_entry__numeric,
	cqe_eligible.operation__user_entry__survey_id,
	cqe_eligible.visit__device__enc_uuid,
	cqe_eligible.visit__account__enc_account_number,
	cqe_eligible.visit__account__enc_account_billing_division,
	cqe_eligible.visit__account__enc_account_billing_division_id,
	cqe_eligible.message__time_since_last_event_ms,
	cqe_eligible.visit__application_details__application_name,
	cqe_eligible.operation__additional_information,
	cqe_eligible.msa_self_install_eligible,
	cqe_eligible.web_self_install_eligible,
	cqe_eligible.last_additional_information,
	cqe_eligible.last_additional_information_by_visit_id,
	cqe_eligible.prev_additional_information_by_visit_id,
	cqe_eligible.split_additional_information,
	cqe_eligible.next_page,
	CASE WHEN TRIM(pb_eligible.acct_id) = '' THEN NULL ELSE pb_eligible.acct_id END AS first_acct_id,
	pb_eligible.msa_first_initiated_self_installs_visit_id,
	pb_eligible.web_first_initiated_self_installs_visit_id,
	pb_eligible.msa_first_initiated_self_installs_timestamp,
	pb_eligible.web_first_initiated_self_installs_timestamp,
	pb_eligible.msa_complete_activation_self_installs_visit_id,
	pb_eligible.msa_complete_activation_self_installs_timestamp,
	pb_eligible.web_complete_activation_self_installs_visit_id,
	pb_eligible.web_complete_activation_self_installs_timestamp,
	CASE WHEN pb_eligible.interaction IS NULL THEN 0 ELSE pb_eligible.interaction END interaction,
	
	CASE WHEN TRIM(pb_eligible.prev_user_package) = '' THEN NULL ELSE pb_eligible.prev_user_package END AS prev_user_package,
	pb_eligible.msa_customer_satisfaction_submission_timestamp,
	CASE WHEN pb_eligible.modem_activated_before_current_date IS NULL THEN 0 ELSE pb_eligible.modem_activated_before_current_date END AS modem_activated_before_current_date,
	CASE WHEN pb_eligible.router_activated_before_current_date IS NULL THEN 0 ELSE pb_eligible.router_activated_before_current_date END AS router_activated_before_current_date,
	CASE WHEN pb_eligible.msa_router_activated_before_current_date IS NULL THEN 0 ELSE pb_eligible.msa_router_activated_before_current_date END AS msa_router_activated_before_current_date,
	CASE WHEN pb_eligible.tv_activated_before_current_date IS NULL THEN 0 ELSE pb_eligible.tv_activated_before_current_date END AS tv_activated_before_current_date,
	CASE WHEN pb_eligible.voice_activated_before_current_date IS NULL THEN 0 ELSE pb_eligible.voice_activated_before_current_date END AS voice_activated_before_current_date,
	CASE WHEN pb_eligible.msa_distinct_devices_initiation IS NULL THEN 0 ELSE pb_eligible.msa_distinct_devices_initiation END AS msa_distinct_devices_initiation,
	CASE WHEN pb_eligible.msa_distinct_devices_feedback IS NULL THEN 0 ELSE pb_eligible.msa_distinct_devices_feedback END AS msa_distinct_devices_feedback,
	pb_eligible.last_additional_information AS pb_last_additional_information
FROM (
	SELECT
		received__timestamp,
		epoch_converter(received__timestamp, 'America/Denver') AS denver_date,
		epoch_datehour(received__timestamp, 'America/Denver') AS denver_date_hour,
		visit__visit_id,
		visit__account__details__mso,
		visit__application_details__application_type,
		visit__application_details__app_version,
		application__error__error_type,
		application__error__error_code,
		CAST(SUBSTR(application__error__error_code, 7, 4) AS INT) AS application__error__error_code_numeric,
		application__api__response_text,
		application__api__response_time_ms,
		application__api__path,
		application__api__http_verb,
		message__name,
		message__sequence_number,
		message__timestamp,
		message__triggered_by,
		message__feature__feature_name,
		state__view__current_page__page_name,
		state__view__current_page__elements__standardized_name,
		state__view__modal__name,
		operation__success,
		message__context,
		message__feature__transaction_id,
		message__feature__feature_step_name,
		message__event_case_id,
		state__view__current_page__elements__element_string_value,
		LOWER(state__view__current_page__additional_information) AS state__view__current_page__additional_information,
		state__view__current_page__page_title,
		operation__user_entry__numeric,
		operation__user_entry__survey_id,
		visit__device__enc_uuid,
		visit__account__enc_account_number,
		visit__account__enc_account_billing_division,
		visit__account__enc_account_billing_division_id,
		message__time_since_last_event_ms,
		visit__application_details__application_name,
		operation__additional_information,
		--Eligible accounts as Self-Install
		FIRST_VALUE(CASE WHEN message__event_case_id IN('mySpectrum_applicationActivity_homeCard_selfInstall', 'mySpectrum_pageView_equipment_selfInstall_fullScreenEquipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_fullScreenEquipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_notNow')
							  OR (message__event_case_id = 'mySpectrum_pageView_feedback_satisfactionRating' AND state__view__current_page__page_title = 'equipmentSelfInstall')
							  OR (message__event_case_id = 'mySpectrum_userFeedback_satisfactionRating' AND operation__user_entry__survey_id = 'equipmentSelfInstall')
								THEN 1 
					ELSE NULL END, TRUE)
			OVER (PARTITION BY visit__application_details__application_name, visit__account__enc_account_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msa_self_install_eligible,
		
		--Web Activation: user logs in and is able to see selfInstall_error_verifyDeviceOnline* page
		FIRST_VALUE(CASE WHEN message__event_case_id IN('selfInstall_pageView_agreementTerms', 'selfInstall_pageView_equipment_selfInstallStartPage', 'selfInstall_pageView_accountPastDue', 'selfInstall_error_verifyDeviceOnline', 'selfInstall_pageView_transferAddressSelection', 'selfInstall_Generic_API_Call_Success') 
							  AND LOWER(state__view__current_page__additional_information) RLIKE '"pendingservices":\\[*".*"\\]*' THEN 1 
					ELSE NULL END, TRUE)
			OVER (PARTITION BY visit__application_details__application_name, visit__account__enc_account_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS web_self_install_eligible,
		
		--to determine user package
		LAST_VALUE(CASE WHEN visit__application_details__application_name = 'MySpectrum'
							AND LOWER(state__view__current_page__additional_information) RLIKE '(modem|internet|router|wifi|receiver|stb|video|tv|phone|voice)'
							AND message__event_case_id IN('mySpectrum_pageView_equipment_selfInstall_equipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment')
							THEN LOWER(state__view__current_page__additional_information)
						WHEN visit__application_details__application_name = 'SelfInstall'
							AND LOWER(state__view__current_page__additional_information) RLIKE '(modem|internet|router|wifi|receiver|stb|video|tv|phone|voice)'
							AND message__event_case_id IN('selfInstall_pageView_equipment_selfInstallStartPage', 'selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
							THEN LOWER(state__view__current_page__additional_information)
				 ELSE NULL END
				 , TRUE)
			OVER (PARTITION BY visit__application_details__application_name, visit__account__enc_account_number ORDER BY message__timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_additional_information,
		
		--to determine status at the end of the visit
		LAST_VALUE(CASE WHEN LOWER(state__view__current_page__additional_information) RLIKE '(modem|internet|router|wifi|receiver|stb|video|tv|phone|voice)'
							AND message__event_case_id IN('mySpectrum_pageView_equipment_selfInstall_equipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment',
							'selfInstall_pageView_equipment_selfInstallStartPage', 'selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
						THEN LOWER(state__view__current_page__additional_information)
				 ELSE NULL END, TRUE)
			OVER (PARTITION BY visit__application_details__application_name, visit__account__enc_account_number, visit__visit_id ORDER BY message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_additional_information_by_visit_id,
		
		--to determine status from prev events in same visit
		LAST_VALUE(CASE WHEN LOWER(state__view__current_page__additional_information) RLIKE '(modem|internet|router|wifi|receiver|stb|video|tv|phone|voice)'
							AND message__event_case_id IN('mySpectrum_pageView_equipment_selfInstall_equipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment',
							'selfInstall_pageView_equipment_selfInstallStartPage', 'selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
						THEN LOWER(state__view__current_page__additional_information)
				 ELSE NULL END, TRUE)
			OVER (PARTITION BY visit__application_details__application_name, visit__account__enc_account_number, visit__visit_id ORDER BY message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_additional_information_by_visit_id,
		split(LOWER(NVL(state__view__current_page__additional_information, '')), '\\}\\,\\{') AS split_additional_information,
		LEAD(state__view__current_page__page_name, 1, '') OVER (PARTITION BY visit__application_details__application_name, visit__account__enc_account_number, visit__visit_id ORDER BY message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS next_page
	FROM si_core_quantum_events
	WHERE partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}'
		AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}'
		AND visit__application_details__application_type  IN ('iOS', 'Android', 'Web')
		AND visit__application_details__application_name IN ('MySpectrum', 'SelfInstall')
		AND visit__account__enc_account_number IS NOT NULL
		-- Adding conditions to filter out mySpectrum_Generic_Error (short terms solution)
		AND NOT (message__event_case_id = 'mySpectrum_Generic_Error'
                AND application__error__error_type = 'System.NullReferenceException'
                )
) cqe_eligible 
LEFT JOIN ${env:TMP_db}.si_summary_eligible_accounts pb_eligible
	ON pb_eligible.acct_id = cqe_eligible.visit__account__enc_account_number
		AND pb_eligible.application_group = cqe_eligible.visit__application_details__application_name
WHERE 
	--user is eligible because sees ApplicationActivitiy (MSA) or logs in successfully (Web)
	(cqe_eligible.msa_self_install_eligible = 1 OR cqe_eligible.web_self_install_eligible = 1)
;

--Call
SELECT "\n\nFor 1a: ${env:TMP_db}.si_summary_call_resolution_tmp\n\n";

MSCK REPAIR TABLE ${env:GLOBAL_db}.atom_cs_call_care_data_3;

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


--Explode CQE
SELECT "\n\nFor 1b: ${env:TMP_db}.si_cqe_explode\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_cqe_explode PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_cqe_explode AS
SELECT cqe_activations.received__timestamp,
	cqe_activations.denver_date_hour,
	cqe_activations.visit__application_details__application_name,
	cqe_activations.visit__application_details__application_type,
	cqe_activations.message__timestamp,
	cqe_activations.message__event_case_id,
	cqe_activations.message__sequence_number,
	cqe_activations.visit__account__enc_account_number,
	cqe_activations.visit__visit_id,
	SUM(cqe_activations.modem_count) AS modem_count,
	SUM(cqe_activations.pending_modem_count) AS pending_modem_count,
	SUM(cqe_activations.modem_setup_success) AS modem_setup_success,
	SUM(cqe_activations.router_count) AS router_count,
	SUM(cqe_activations.pending_router_count) AS pending_router_count,
	SUM(cqe_activations.router_setup_success) AS router_setup_success,
	SUM(cqe_activations.tv_count) AS tv_count,
	SUM(cqe_activations.pending_tv_count) AS pending_tv_count,
	SUM(cqe_activations.tv_setup_success) AS tv_setup_success,
	SUM(cqe_activations.voice_count) AS voice_count,
	SUM(cqe_activations.pending_voice_count) AS pending_voice_count,
	SUM(cqe_activations.voice_setup_success) AS voice_setup_success
FROM(
	SELECT event.received__timestamp,
		event.denver_date_hour,
		event.visit__application_details__application_name,
		event.visit__application_details__application_type,
		event.message__timestamp,
		event.message__event_case_id,
		event.message__sequence_number,
		event.visit__account__enc_account_number,
		event.visit__visit_id,

		-- activations using explode and get_json_object
		-- modem
		CASE 
			-- payload format like 'device: status'
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_equipmentSetup' AND event.device_payload LIKE '%modem:%'
				THEN SIZE(SPLIT(device_payload, 'modem:')) - (CASE WHEN event.device_payload LIKE '%modem: coam%' THEN 2 ELSE 1 END)
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND ((event.device_payload LIKE '%"devicetype":"modem"%' AND event.device_payload NOT LIKE '%pendingservices%') OR event.device_payload LIKE '%internet%') THEN 1
			WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.device_payload LIKE '%internet%' THEN 1
		ELSE 0 END AS modem_count,
		CASE 
			-- payload format like 'device: status'
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_equipmentSetup' AND event.device_payload LIKE '%modem: notactivated%'
				THEN SIZE(SPLIT(event.device_payload, 'modem: notactivated')) - (CASE WHEN event.device_payload LIKE '%modem: coam%' THEN 2 ELSE 1 END)
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND ((event.device_payload LIKE '%"devicetype":"modem"%"devicestatus":"notfinished"%' AND event.device_payload NOT LIKE '%pendingservices%') OR event.device_payload LIKE '%"pendingservices":["%internet%"]%"activeservices"%') THEN 1
			WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.device_payload RLIKE '"pendingservices":\\[*".*internet.*"\\]*' THEN 1
		ELSE 0 END AS pending_modem_count,
		CASE 
			-- payload format like 'device: status'
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
                AND (event.message__feature__feature_name = 'modem' OR event.state__view__current_page__page_name IN('selfInstallModemSetup', 'selfInstallVerifyYourModem')) AND event.state__view__current_page__elements__element_string_value = 'finished'
                AND ((event.device_payload IS NULL AND event.last_additional_information LIKE '%"devicetype":"modem"%' AND event.last_additional_information NOT LIKE '%pendingservices%') 
					OR (event.device_payload IS NULL AND event.last_additional_information LIKE '%"devicetype":"modem"%internet%')
					OR event.device_payload LIKE '%internet%'
					)
				THEN 1
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'SelfInstall'
				AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
				AND event.device_payload LIKE '%"internet":"activated"%' THEN 1
		ELSE 0 END modem_setup_success,
		
		--router
		CASE 
			-- payload format like 'device: status'
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_equipmentSetup' AND event.device_payload LIKE '%router:%'
				THEN SIZE(SPLIT(event.device_payload, 'router:')) - (CASE WHEN event.device_payload LIKE '%router: scp%' THEN 2 ELSE 1 END)
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND (event.device_payload LIKE '%"devicetype":"router"%' OR event.device_payload LIKE '%wifi%') THEN 1
			WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.device_payload LIKE '%wifi%' THEN 1
		ELSE 0 END AS router_count,
		CASE 
			-- payload format like 'device: status'
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_equipmentSetup' AND event.device_payload LIKE '%router: notactivated%'
				THEN SIZE(SPLIT(event.device_payload, 'router: notactivated')) - (CASE WHEN event.device_payload LIKE '%router: scp%' THEN 1 ELSE 0 END)
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.device_payload LIKE '%"devicetype":"router"%"devicestatus":"notfinished"%' THEN 1
			WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.device_payload RLIKE '"pendingservices":\\[*".*wifi.*"\\]*' THEN 1
		ELSE 0 END AS pending_router_count,
		CASE 
			-- payload format like 'device: status'
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
				AND (event.message__feature__feature_name = 'router' OR event.state__view__current_page__page_name = 'selfInstallRouterSetup') AND event.state__view__current_page__elements__element_string_value = 'finished'
				THEN 1
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'SelfInstall'
				AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
				AND event.device_payload LIKE '%"wifi":"activated"%' THEN 1
		ELSE 0 END router_setup_success,
		
		--tv
		CASE 
			-- payload format like 'device: status'
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_equipmentSetup' AND event.device_payload LIKE '%receiver:%'
				THEN SIZE(SPLIT(event.device_payload, 'receiver:')) - (CASE WHEN event.device_payload LIKE '%receiver: oneway%' OR event.device_payload LIKE '%receiver: twoway%' THEN 2 ELSE 1 END)
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND (event.device_payload LIKE '%"devicetype":"stb"%' OR event.device_payload RLIKE '(video|tv)') THEN 1
			WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.device_payload RLIKE '(video|tv)' THEN 1
		ELSE 0 END AS tv_count,
		CASE 
			-- payload format like 'device: status'
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_equipmentSetup' AND event.device_payload LIKE '%receiver: notactivated%'
				THEN SIZE(SPLIT(event.device_payload, 'receiver: notactivated')) - (CASE WHEN event.device_payload LIKE '%receiver: oneway%' OR event.device_payload LIKE '%receiver: twoway%' THEN 1 ELSE 0 END)
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.device_payload LIKE '%"devicetype":"stb"%"devicestatus":"notfinished"%' THEN 1
			WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.device_payload RLIKE '"pendingservices":\\[*".*(video|tv).*"\\]*' THEN 1
		ELSE 0 END AS pending_tv_count,
		CASE 
			-- payload format like 'device: status'
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
				AND (event.message__feature__feature_name = 'receiver' OR event.state__view__current_page__page_name IN('Insert Batteries Into Remote', 'selfInstallReceiverSetup')) AND event.state__view__current_page__elements__element_string_value = 'finished'
				THEN 1
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'SelfInstall'
				AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
				AND event.device_payload RLIKE '"(video|tv)":"activated"' THEN 1
		ELSE 0 END tv_setup_success,
		
		--voice
		CASE
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.device_payload RLIKE '(phone|voice)' THEN 1
			WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.device_payload RLIKE '(phone|voice)' THEN 1
		ELSE 0 END AS voice_count,
		CASE 
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.device_payload RLIKE '"pendingservices":\\[".*(phone|voice).*"\\].*"activeservices"' THEN 1
			WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.device_payload RLIKE '"pendingservices":\\[*".*(phone|voice).*"\\]*' THEN 1
		ELSE 0 END AS pending_voice_count,
		CASE 
			-- payload format like 'device: status'
			WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
                AND (
                    -- in case Voice service is activated on initial modem activation
                    ((event.message__feature__feature_name = 'modem' OR event.state__view__current_page__page_name LIKE 'selfInstallPhoneSetup%' OR event.state__view__current_page__page_name IN('selfInstallModemSetup', 'selfInstallVerifyYourModem')) AND event.state__view__current_page__elements__element_string_value = 'finished' 
						AND ((event.device_payload IS NULL AND event.last_additional_information RLIKE '"activeservices":\\[".*(phone|voice).*"\\]' )
							OR event.device_payload RLIKE '(phone|voice)'
						)
					)
                    -- optional flow for Voice activation
                    OR ((event.message__feature__feature_name RLIKE '(phone|voice)' OR event.state__view__current_page__page_name LIKE 'selfInstallPhoneSetup%' OR event.state__view__current_page__page_name IN('selfInstallModemSetup', 'selfInstallVerifyYourModem')) AND event.state__view__current_page__elements__element_string_value = 'finished' 
						AND ((event.device_payload IS NULL AND event.last_additional_information LIKE '%"devicetype":"modem"%voice%')
							OR event.device_payload RLIKE '(phone|voice)'
						)
					)
                ) THEN 1
			-- payload in json format
			WHEN event.visit__application_details__application_name = 'SelfInstall'
				AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
				AND (event.device_payload RLIKE '"(phone|voice)":"activated"'
					--in case user has two lines
					OR event.device_payload RLIKE '"(phone|voice)":"activated, activated"') THEN 1
		ELSE 0 END voice_setup_success
	FROM (SELECT *
		FROM ${env:TMP_db}.si_cqe_eligible_accounts
		LATERAL VIEW OUTER EXPLODE(split_additional_information) deviceTable AS device_payload
		WHERE (state__view__current_page__additional_information IS NOT NULL AND state__view__current_page__additional_information != '')
			OR message__event_case_id IN('mySpectrum_applicationActivity_equipment_selfInstall_status', 'mySpectrum_pageView_equipment_selfInstall_equipmentSetup'
										, 'selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
	) event
) cqe_activations
GROUP BY received__timestamp,
		denver_date_hour,
		visit__application_details__application_name,
		visit__application_details__application_type,
		message__timestamp,
		message__event_case_id,
		message__sequence_number,
		visit__account__enc_account_number,
		visit__visit_id
;

--Tag relevant events
SELECT "\n\nFor 1b: ${env:TMP_db}.si_summary_page_base_tag\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_page_base_tag PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_summary_page_base_tag AS
SELECT 
	page_base_tag.received_timestamp,
	page_base_tag.visit_id,
	page_base_tag.device_id,
	page_base_tag.acct_id,
	page_base_tag.acct_division,
	page_base_tag.acct_division_id,
	page_base_tag.denver_date,
	page_base_tag.denver_date_hour,
	page_base_tag.current_page_name,
	page_base_tag.standardize_name,
	page_base_tag.modal_name,
	page_base_tag.error_type,
	page_base_tag.error_code,
	page_base_tag.feature_step_name,
	page_base_tag.sequence_number,
	page_base_tag.time_since_last_event_ms,
	page_base_tag.event_case_id,
	page_base_tag.additional_information,
	page_base_tag.application_group,
	page_base_tag.application_type,
	page_base_tag.mso,
	page_base_tag.prev_user_package,
	page_base_tag.user_package_over,
	
	--Web Activation is success for multiple services in same success event
	CASE WHEN page_base_tag.application_group = 'SelfInstall' AND page_base_tag.page_view_tag = 'web_activation_page_view'
			THEN SUBSTR(CONCAT(CASE WHEN page_base_tag.modem_activated = 1 THEN '+modem_setup_success' ELSE '' END
								, CASE WHEN page_base_tag.router_activated = 1 THEN '+router_setup_success' ELSE '' END
								, CASE WHEN page_base_tag.tv_activated = 1 THEN '+tv_setup_success' ELSE '' END
								, CASE WHEN page_base_tag.voice_activated = 1 THEN '+voice_setup_success' ELSE '' END
								, CASE WHEN page_base_tag.current_modem_setup_failure = 1 THEN '+modem_setup_failure' ELSE '' END
								, CASE WHEN page_base_tag.current_router_setup_failure = 1 THEN '+router_setup_failure' ELSE '' END
								, CASE WHEN page_base_tag.current_tv_setup_failure = 1 THEN '+tv_setup_failure' ELSE '' END
								, CASE WHEN page_base_tag.current_voice_setup_failure = 1 THEN '+voice_setup_failure' ELSE '' END
							  ), 2
						)
	ELSE page_base_tag.event_tag END AS event_tag,
	page_base_tag.page_view_tag,
	page_base_tag.initiated_self_installs_type,
	page_base_tag.customer_satisfaction_submission_score,
	page_base_tag.inactivity,
	page_base_tag.visit_first_event_flag,
	page_base_tag.visit_last_event_flag,
	page_base_tag.first_acct_id,
	page_base_tag.msa_first_initiated_self_installs_visit_id,
	page_base_tag.msa_first_initiated_self_installs_timestamp,
	page_base_tag.web_first_initiated_self_installs_visit_id,
	page_base_tag.web_first_initiated_self_installs_timestamp,
	page_base_tag.msa_complete_activation_self_installs_visit_id,
	page_base_tag.msa_complete_activation_self_installs_timestamp,
	page_base_tag.web_complete_activation_self_installs_visit_id,
	page_base_tag.web_complete_activation_self_installs_timestamp,
	page_base_tag.application_version,
	page_base_tag.message_name,
	page_base_tag.transaction_id,
	page_base_tag.message_context,
	page_base_tag.triggered_by,
	page_base_tag.operation_success,
	page_base_tag.api_response_text,
	page_base_tag.api_response_time,
	page_base_tag.api_http_verb,
	page_base_tag.api_path,
	page_base_tag.message_timestamp,
	page_base_tag.has_modem,
	page_base_tag.has_router,
	page_base_tag.has_tv,
	page_base_tag.has_voice,
	SUBSTR(CONCAT(CASE WHEN page_base_tag.has_modem THEN '+Modem' ELSE '' END
				, CASE WHEN page_base_tag.has_router THEN '+Router' ELSE '' END
				, CASE WHEN page_base_tag.has_tv THEN '+TV' ELSE '' END
				, CASE WHEN page_base_tag.has_voice THEN '+Voice' ELSE '' END), 2
		) AS user_package,

	--activation - success
	MAX(page_base_tag.modem_count)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS modem_count,
	MAX(page_base_tag.pending_modem_count)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS pending_modem_count,
	CASE WHEN page_base_tag.application_group = 'MySpectrum'
			THEN SUM(page_base_tag.modem_setup_success)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		 WHEN page_base_tag.application_group = 'SelfInstall'
			THEN MAX(page_base_tag.modem_setup_success)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
	ELSE 0 END AS modem_setup_success,
	
	MAX(page_base_tag.router_count)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS router_count,
	MAX(page_base_tag.pending_router_count)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS pending_router_count,
	CASE WHEN page_base_tag.application_group = 'MySpectrum'
			THEN SUM(page_base_tag.router_setup_success)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		 WHEN page_base_tag.application_group = 'SelfInstall'
			THEN MAX(page_base_tag.router_setup_success)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
	ELSE 0 END AS router_setup_success,
	
	MAX(page_base_tag.tv_count)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS tv_count,
	MAX(page_base_tag.pending_tv_count)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS pending_tv_count,
	CASE WHEN page_base_tag.application_group = 'MySpectrum'
			THEN SUM(page_base_tag.tv_setup_success)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		 WHEN page_base_tag.application_group = 'SelfInstall'
			THEN MAX(page_base_tag.tv_setup_success)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
	ELSE 0 END AS tv_setup_success,
	
	MAX(page_base_tag.voice_count)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS voice_count,
	MAX(page_base_tag.pending_voice_count)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS pending_voice_count,
	CASE WHEN page_base_tag.application_group = 'MySpectrum'
			THEN SUM(page_base_tag.voice_setup_success)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		 WHEN page_base_tag.application_group = 'SelfInstall'
			THEN MAX(page_base_tag.voice_setup_success)
		OVER (PARTITION BY page_base_tag.denver_date, page_base_tag.application_group, page_base_tag.acct_id, page_base_tag.visit_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
	ELSE 0 END AS voice_setup_success,

	page_base_tag.current_modem_setup_success,
	page_base_tag.current_router_setup_success,
	page_base_tag.current_tv_setup_success,
	page_base_tag.current_voice_setup_success,
	page_base_tag.prev_modem_setup_success,
	page_base_tag.prev_router_setup_success,
	page_base_tag.prev_tv_setup_success,
	page_base_tag.prev_voice_setup_success,
	page_base_tag.modem_activated_before_current_date,
	page_base_tag.router_activated_before_current_date,
	page_base_tag.tv_activated_before_current_date,
	page_base_tag.voice_activated_before_current_date,
	page_base_tag.current_modem_setup_failure,
	page_base_tag.current_router_setup_failure,
	page_base_tag.current_tv_setup_failure,
	page_base_tag.current_voice_setup_failure,
	page_base_tag.modem_activated,
	page_base_tag.router_activated,
	page_base_tag.tv_activated,
	page_base_tag.voice_activated,

	--get last activation event and determine whether all devices/services have been activated
	MAX(CASE WHEN page_base_tag.application_group = 'MySpectrum' 
				AND (page_base_tag.event_tag RLIKE '(modem|router|tv|voice)_setup_success' OR page_base_tag.event_tag LIKE '%activation_complete%' OR page_base_tag.event_tag RLIKE '(modem|router|tv|voice)_errors'
					OR (page_base_tag.operation_success = false AND page_base_tag.message_context = 'equipmentSelfInstall' AND page_base_tag.triggered_by = 'application' AND page_base_tag.message_name = 'featureStop')
				) THEN sequence_number
			 WHEN page_base_tag.application_group = 'SelfInstall' 
				AND (page_base_tag.event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
				) THEN sequence_number
		ELSE 0 END) 
		OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.denver_date, page_base_tag.application_group ORDER BY page_base_tag.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_activation_page_sequence_number,
	0 AS activation_failure,

	--activation - failure
	MAX(CASE WHEN page_base_tag.application_group = 'MySpectrum' AND page_base_tag.event_tag LIKE '%modem_errors_%' THEN 1
			 WHEN page_base_tag.application_group = 'SelfInstall' AND (modem_setup_failure = 1 OR page_base_tag.event_tag LIKE '%modem_errors_%') THEN 1
		ELSE 0 END
		) OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.denver_date, page_base_tag.application_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS modem_setup_failure,
	MAX(CASE WHEN page_base_tag.application_group = 'MySpectrum' AND page_base_tag.event_tag LIKE '%router_errors_%' THEN 1
			 WHEN page_base_tag.application_group = 'SelfInstall' AND (router_setup_failure = 1 OR page_base_tag.event_tag LIKE '%router_errors_%') THEN 1
		ELSE 0 END
		) OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.denver_date, page_base_tag.application_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS router_setup_failure,
	MAX(CASE WHEN page_base_tag.application_group = 'MySpectrum' AND page_base_tag.event_tag LIKE '%tv_errors_%' THEN 1
			 WHEN page_base_tag.application_group = 'SelfInstall' AND (tv_setup_failure = 1 OR page_base_tag.event_tag LIKE '%tv_errors_%') THEN 1
		ELSE 0 END) OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.denver_date, page_base_tag.application_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS tv_setup_failure,
	MAX(CASE WHEN page_base_tag.application_group = 'MySpectrum' AND page_base_tag.event_tag LIKE '%voice_errors_%' THEN 1
			 WHEN page_base_tag.application_group = 'SelfInstall' AND (voice_setup_failure = 1 OR page_base_tag.event_tag LIKE '%voice_errors_%') THEN 1
		ELSE 0 END) OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.denver_date, page_base_tag.application_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS voice_setup_failure,
	page_base_tag.equipment_type_scp,
	page_base_tag.equipment_type_monolithic,
	page_base_tag.visit_eligible,
	page_base_tag.interaction,
	MAX(CASE WHEN page_base_tag.event_tag = 'activation_complete' OR page_base_tag.event_tag = 'activation_complete_featureStop' THEN 1 ELSE 0 END)
		OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.application_group, page_base_tag.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_activation_complete
FROM(
	SELECT 
		pb_user_package.received_timestamp,
		pb_user_package.visit_id,
		pb_user_package.device_id,
		pb_user_package.acct_id,
		pb_user_package.acct_division,
		pb_user_package.acct_division_id,
		pb_user_package.denver_date,
		pb_user_package.denver_date_hour,
		pb_user_package.current_page_name,
		pb_user_package.standardize_name,
		pb_user_package.modal_name,
		pb_user_package.error_type,
		pb_user_package.error_code,
		pb_user_package.feature_step_name,
		pb_user_package.sequence_number,
		pb_user_package.time_since_last_event_ms,
		pb_user_package.event_case_id,
		pb_user_package.additional_information,
		pb_user_package.application_group,
		pb_user_package.application_type,
		pb_user_package.mso,
		pb_user_package.prev_user_package,
		pb_user_package.user_package_over,
		pb_user_package.event_tag,
		pb_user_package.page_view_tag,
		pb_user_package.initiated_self_installs_type,
		pb_user_package.customer_satisfaction_submission_score,
		pb_user_package.inactivity,
		pb_user_package.visit_first_event_flag,
		pb_user_package.visit_last_event_flag,
		pb_user_package.first_acct_id,
		pb_user_package.msa_first_initiated_self_installs_visit_id,
		pb_user_package.msa_first_initiated_self_installs_timestamp,
		pb_user_package.web_first_initiated_self_installs_visit_id,
		pb_user_package.web_first_initiated_self_installs_timestamp,
		pb_user_package.msa_complete_activation_self_installs_visit_id,
		pb_user_package.msa_complete_activation_self_installs_timestamp,
		pb_user_package.web_complete_activation_self_installs_visit_id,
		pb_user_package.web_complete_activation_self_installs_timestamp,
		pb_user_package.application_version,
		pb_user_package.message_name,
		pb_user_package.transaction_id,
		pb_user_package.message_context,
		pb_user_package.triggered_by,
		pb_user_package.operation_success,
		pb_user_package.api_response_text,
		pb_user_package.api_response_time,
		pb_user_package.api_http_verb,
		pb_user_package.api_path,
		pb_user_package.message_timestamp,
		pb_user_package.last_additional_information,
		pb_user_package.last_additional_information_by_visit_id,

		--Package flags
		CASE WHEN pb_user_package.application_group = 'MySpectrum'
				AND (
					-- payload without services
					(pb_user_package.user_package_over LIKE '%"devicetype":"modem"%' AND pb_user_package.user_package_over NOT LIKE '%pendingservices%')
					-- payload with services
					OR pb_user_package.user_package_over LIKE '%"devicetype":"modem"%internet%'
					-- package already calculated
					OR pb_user_package.user_package_over LIKE 'modem%'
				)
				THEN TRUE
			WHEN pb_user_package.application_group = 'SelfInstall' AND (pb_user_package.user_package_over LIKE 'modem%' OR pb_user_package.user_package_over LIKE '%internet%') THEN TRUE
			ELSE FALSE END AS has_modem,
		CASE WHEN pb_user_package.user_package_over RLIKE '(router|wifi)' THEN TRUE ELSE FALSE END AS has_router,
		CASE WHEN pb_user_package.user_package_over RLIKE '(receiver|stb|video|tv)' THEN TRUE ELSE FALSE END AS has_tv,
		CASE WHEN pb_user_package.user_package_over RLIKE '(phone|voice)' THEN TRUE
			ELSE FALSE END AS has_voice,
		
		--activation - success
		pb_user_package.modem_count,
		pb_user_package.pending_modem_count,
		pb_user_package.modem_setup_success,
		pb_user_package.router_count,
		pb_user_package.pending_router_count,
		pb_user_package.router_setup_success,
		pb_user_package.tv_count,
		pb_user_package.pending_tv_count,
		pb_user_package.tv_setup_success,
		pb_user_package.voice_count,
		pb_user_package.pending_voice_count,
		pb_user_package.voice_setup_success,

		pb_user_package.current_modem_setup_success,
		pb_user_package.current_router_setup_success,
		pb_user_package.current_tv_setup_success,
		pb_user_package.current_voice_setup_success,
		pb_user_package.prev_modem_setup_success,
		pb_user_package.prev_router_setup_success,
		pb_user_package.prev_tv_setup_success,
		pb_user_package.prev_voice_setup_success,
		pb_user_package.modem_activated_before_current_date,
		pb_user_package.router_activated_before_current_date,
		pb_user_package.tv_activated_before_current_date,
		pb_user_package.voice_activated_before_current_date,
		pb_user_package.current_modem_setup_failure,
		pb_user_package.current_router_setup_failure,
		pb_user_package.current_tv_setup_failure,
		pb_user_package.current_voice_setup_failure,

		CASE WHEN pb_user_package.current_modem_setup_success <> pb_user_package.prev_modem_setup_success THEN 1 ELSE 0 END AS modem_activated,
		CASE WHEN pb_user_package.current_router_setup_success <> pb_user_package.prev_router_setup_success THEN 1 ELSE 0 END AS router_activated,
		CASE WHEN pb_user_package.current_tv_setup_success <> pb_user_package.prev_tv_setup_success THEN 1 ELSE 0 END AS tv_activated,
		CASE WHEN pb_user_package.current_voice_setup_success <> pb_user_package.prev_voice_setup_success THEN 1 ELSE 0 END AS voice_activated,

		pb_user_package.modem_setup_failure,
		pb_user_package.router_setup_failure,
		pb_user_package.tv_setup_failure,
		pb_user_package.voice_setup_failure,
		
		--equipment type
		pb_user_package.equipment_type_scp,
		pb_user_package.equipment_type_monolithic,
		
		pb_user_package.visit_eligible,
		pb_user_package.interaction
	FROM (
		SELECT
			event.received__timestamp AS received_timestamp,
			event.visit__visit_id AS visit_id,
			event.visit__application_details__app_version AS application_version,
			event.visit__device__enc_uuid AS device_id,
			event.visit__account__enc_account_number AS acct_id,
			event.visit__account__enc_account_billing_division AS acct_division,
			event.visit__account__enc_account_billing_division_id AS acct_division_id,
			event.denver_date,
			event.denver_date_hour,
			event.state__view__current_page__page_name AS current_page_name,
			event.state__view__current_page__elements__standardized_name AS standardize_name,
			event.state__view__modal__name AS modal_name,
			event.operation__success AS operation_success,
			event.application__error__error_type AS error_type,
			event.application__error__error_code AS error_code,
			event.application__api__response_text AS api_response_text,
			event.application__api__response_time_ms AS api_response_time,
			event.application__api__path AS api_path,
			event.application__api__http_verb AS api_http_verb,
			event.message__feature__feature_step_name AS feature_step_name,
			event.message__context AS message_context,
			event.message__feature__transaction_id AS transaction_id,
			event.message__name AS message_name,
			event.message__sequence_number AS sequence_number,
			event.message__timestamp AS message_timestamp,
			event.message__triggered_by AS triggered_by,
			event.message__time_since_last_event_ms AS time_since_last_event_ms,
			event.message__event_case_id AS event_case_id,
			event.state__view__current_page__additional_information AS additional_information,

		---------------------------------------------------------------------------------------------------
		-- Available Filters/Breakdowns
		---------------------------------------------------------------------------------------------------
			--Application Group (applicationplatform_partition, Platform: 'MySpectrum')
			event.visit__application_details__application_name AS application_group,

			--Application Type (iOS, Android)
			event.visit__application_details__application_type AS application_type,

			--MSO
			CASE
				WHEN event.visit__account__details__mso IN('CHARTER','CHTR','CHTR','CHR') THEN 'L-CHTR' --use CHR when joining to dev.atom_cs_call_care_data_3
				WHEN event.visit__account__details__mso = 'TWC' THEN 'L-TWC'
				WHEN event.visit__account__details__mso IN('BH', 'BHN') THEN 'L-BHN'
				ELSE NULL
			END AS mso,

			--Package
			NVL(event.prev_user_package, '') AS prev_user_package,
			LOWER(CASE WHEN last_additional_information IS NULL OR TRIM(last_additional_information) = '' THEN pb_last_additional_information 
			ELSE last_additional_information END) AS user_package_over,
			

		---------------------------------------------------------------------------------------------------
		-- Self-Install Definitions
		---------------------------------------------------------------------------------------------------

			CASE
				--Total Initiated
				--2.	Total Initiated Self-Installs (Starts)
				WHEN --iOS, Android
					(
						event.visit__application_details__application_name = 'MySpectrum'
						AND (
							(msa_first_initiated_self_installs_timestamp IS NULL
								AND event.message__event_case_id = 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment'
								AND event.received__timestamp = MIN(CASE WHEN event.message__event_case_id = 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment' THEN event.received__timestamp ELSE NULL END) OVER (PARTITION BY event.visit__account__enc_account_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
							)
							OR (first_acct_id IS NOT NULL AND event.message__event_case_id = 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment'
								AND event.received__timestamp = msa_first_initiated_self_installs_timestamp)
							)
					)
					--Web
					OR (
						event.visit__application_details__application_name = 'SelfInstall'
						AND (
							(web_first_initiated_self_installs_timestamp IS NULL AND event.message__event_case_id = 'selfInstall_pageView_equipment_selfInstallStartPage'
								AND event.message__timestamp = MIN(CASE WHEN event.message__event_case_id = 'selfInstall_pageView_equipment_selfInstallStartPage' AND event.state__view__current_page__additional_information RLIKE '"pendingservices":\\[*".*"\\]*' THEN event.message__timestamp ELSE NULL END) OVER (PARTITION BY event.visit__account__enc_account_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
							)
							OR (web_first_initiated_self_installs_timestamp IS NOT NULL AND event.message__event_case_id = 'selfInstall_pageView_equipment_selfInstallStartPage'
								AND event.message__timestamp = web_first_initiated_self_installs_timestamp
								AND event.state__view__current_page__additional_information RLIKE '"pendingservices":\\[*".*"\\]*'
								)
							)
							
						)
					THEN 'initiated_self_installs'
					
				WHEN --iOS, Android (this tag detects all the occurrences of featureStart to insert this event into Page Base table, there is another flag (interaction) which is activated only for the very first occurrence of featureStart)
					event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_featureStart_equipment_selfInstall'
					THEN 'interaction'

				--i.	Past Due
				WHEN --iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_modalView_equipment_selfInstall_redirectToBilling')
					--Web
					OR (event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_pageView_accountPastDue')
					THEN 'account_status_past_due'

				--ii.	Current
				--Comment: it is calculated in aggregation


			--c.	Total Terms & Conditions
				--i.	Accept
				WHEN --iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum' AND event.state__view__current_page__page_name = 'selfInstallTerms' 
						AND event.message__event_case_id = 'mySpectrum_selectAction_equipment_selfInstall_termsAgree')
					--Web
					OR (event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_selectAction_AcceptanceTermsContinue' 
						AND LEAD(event.message__event_case_id, 1) OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 'selfInstall_Generic_API_Call_Success'
					)
					THEN 'terms_conditions_accept'

				--ii.	Disagree
				WHEN --iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum' AND event.state__view__current_page__page_name = 'selfInstallTerms' AND event.message__event_case_id = 'mySpectrum_selectAction_equipment_selfInstall_termsDisagreeConfirmAndExit')
					--Web
					OR (event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_error_acceptTermsConditions')
					THEN 'terms_conditions_disagree'

			--d.	Total Abandonment
				WHEN 
					--iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_selectAction_equipment_selfInstallActionSheetExit' AND event.message__context = 'equipmentSelfInstall')
					THEN 'abandonment_exiting_via_overflow_menu'
				WHEN 
					--iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_selectAction_equipment_selfInstall_errorExitSetup' AND event.message__context = 'equipmentSelfInstall')
					THEN 'abandonment_via_exit_setup'


			--e.	Total Activations
				--iv.	Success
					--1.	Overall
						--a.	Full (activation complete)
						WHEN --iOS, Android
							(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_fullActivationStatus')
							--Web: there is no way to determine full activation based on single event
							THEN 'activation_complete'

						WHEN --iOS, Android
							(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_featureStop_equipment_selfInstall_triggeredByApplicationSuccess')
							--Web: there is no way to determine full activation based on single event
							THEN 'activation_complete_featureStop'

						--b.	Partial
						WHEN --iOS, Android
							(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_modemSetupSuccess' 
								AND event.state__view__current_page__page_name = 'selfInstallModemSetupSuccess'
							)
							--Web: there is no way to determine modem activation based on single event
							THEN 'modem_setup_success'
						
						WHEN --iOS, Android
							(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_routerSetupSuccess' 
								AND event.state__view__current_page__page_name = 'selfInstallRouterSetupSuccess' 
							)
							--Web: there is no way to determine router activation based on single event
							THEN 'router_setup_success'
						
						WHEN --iOS, Android
							(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_receiverSetupSuccess' 
								AND event.state__view__current_page__page_name = 'selfInstallReceiverSetupSuccess' 
							)
							--Web: there is no way to determine router activation based on single event
							THEN 'receiver_setup_success'
						
						WHEN --iOS, Android
							event.visit__application_details__application_name = 'MySpectrum' 
							AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
							AND (event.message__feature__feature_name = 'modem' OR event.state__view__current_page__page_name IN('selfInstallModemSetup', 'selfInstallVerifyYourModem')) AND event.state__view__current_page__elements__element_string_value = 'finished'
							THEN 'modem_status_finished'
						
						WHEN --iOS, Android
							event.visit__application_details__application_name = 'MySpectrum' 
							AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
							AND (event.message__feature__feature_name = 'router' OR event.state__view__current_page__page_name = 'selfInstallRouterSetup') AND event.state__view__current_page__elements__element_string_value = 'finished'
							THEN 'router_status_finished'
							
						WHEN --iOS, Android
							event.visit__application_details__application_name = 'MySpectrum' 
							AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
							AND (event.message__feature__feature_name RLIKE '(receiver|tv|video)' OR event.state__view__current_page__page_name IN('Insert Batteries Into Remote', 'selfInstallReceiverSetup')) AND event.state__view__current_page__elements__element_string_value = 'finished'
							THEN 'tv_status_finished'
						
						WHEN --iOS, Android
							event.visit__application_details__application_name = 'MySpectrum' 
							AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
							AND (event.message__feature__feature_name RLIKE '(phone|voice)' OR event.state__view__current_page__page_name LIKE 'selfInstallPhoneSetup%' OR event.state__view__current_page__page_name IN('selfInstallModemSetup', 'selfInstallVerifyYourModem')) AND event.state__view__current_page__elements__element_string_value = 'finished'
							THEN 'voice_status_finished'
							
							
							--Web: there is no way to determine tv/voice activation based on single event

				--v.	Failure
						--c.	Partial
							--iOS, Android (Modem)
								--it depends on error tag and page name (modem)
							--iOS, Android (Router)
								--it depends on error tag and page name (router)
							
							--Web: there is not an individual event for each device/service to identify which one failed

				--vi.	Total Re-Entry			
				WHEN --iOS, Android
					--based on featureStart
					(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_featureStart_equipment_selfInstall_continueSelfInstall')
					
					--Web
					--based on subsequent visits after first initiated
					OR (event.visit__application_details__application_name = 'SelfInstall' AND web_first_initiated_self_installs_timestamp IS NULL
						AND event.message__event_case_id = 'selfInstall_pageView_equipment_selfInstallStartPage'
						AND event.state__view__current_page__additional_information RLIKE '"pendingservices":\\[*".*"\\]*'
						AND event.message__timestamp > MIN(CASE WHEN event.message__event_case_id = 'selfInstall_pageView_equipment_selfInstallStartPage' AND event.state__view__current_page__additional_information RLIKE '"pendingservices":\\[*".*"\\]*'
							THEN event.message__timestamp ELSE NULL END) OVER (PARTITION BY event.denver_date, event.visit__application_details__application_name, event.visit__account__enc_account_number ORDER BY event.message__timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
						)
					OR (event.visit__application_details__application_name = 'SelfInstall' AND web_first_initiated_self_installs_timestamp IS NOT NULL
						AND event.message__event_case_id = 'selfInstall_pageView_equipment_selfInstallStartPage'
						AND event.message__timestamp > web_first_initiated_self_installs_timestamp
						AND event.state__view__current_page__additional_information RLIKE '"pendingservices":\\[*".*"\\]*'
						)
					THEN 're_entry'

				--vii.	Total Retry
				WHEN --iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum' 
						AND event.message__event_case_id IN('mySpectrum_selectAction_equipment_selfInstall_unableToConnectTryAgain', 'mySpectrum_selectAction_equipment_selfInstall_errorTryAgain')
					)
					-- web: since selfInstall_error_tryAgain is used to capture preActivation and activation errors, using this EID for retry will not allow to capture those errors. Moving retry condition for Web to CASE where capturing metrics based on event_tag
					THEN 'retry'

			--f.	Total Errors
				--i.	Connection
				WHEN --iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_Generic_Error' AND SUBSTR(application__error__error_code, 1, 5) = 'MSASI' 
						AND event.application__error__error_code_numeric BETWEEN 0 AND 999 
					)
					--Web
					OR (event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_Generic_Error' AND SUBSTR(application__error__error_code, 1, 5) = 'WEBSI'
						AND event.application__error__error_code_numeric BETWEEN 0 AND 999
					)
					THEN 'errors_connection_issue'

				--ii.	Equipment Set Up
				WHEN --iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_Generic_Error' AND SUBSTR(application__error__error_code, 1, 5) = 'MSASI' 
						AND event.application__error__error_code_numeric BETWEEN 1000 AND 1999 
					)
					OR (event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_Generic_Error' AND SUBSTR(application__error__error_code, 1, 5) = 'WEBSI'
						AND event.application__error__error_code_numeric BETWEEN 1000 AND 1999 
					)
					THEN 'errors_equipment_setup'
				
				--Authentication
				WHEN --iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_Generic_Error' AND SUBSTR(application__error__error_code, 1, 5) = 'MSASI' 
						AND event.application__error__error_code_numeric BETWEEN 2000 AND 2999 
					)
					--Web
					OR (event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_Generic_Error' AND SUBSTR(application__error__error_code, 1, 5) = 'WEBSI'
						AND event.application__error__error_code_numeric BETWEEN 2000 AND 2999
					)
					THEN 'errors_authentication_setup'
				
				--Device Signal
				WHEN --iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_Generic_Error' AND SUBSTR(application__error__error_code, 1, 5) = 'MSASI' 
						AND event.application__error__error_code_numeric BETWEEN 3000 AND 3999 
					)
					--Web
					OR (event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_Generic_Error' AND SUBSTR(application__error__error_code, 1, 5) = 'WEBSI'
						AND event.application__error__error_code_numeric BETWEEN 3000 AND 3999 
					)
					THEN 'errors_device_signal_setup'
				
				--Others
				WHEN --iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_Generic_Error' AND SUBSTR(application__error__error_code, 1, 5) = 'MSASI' 
						AND event.application__error__error_code_numeric BETWEEN 4000 AND 4999
					)
					--Web
					OR (event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_Generic_Error' AND SUBSTR(application__error__error_code, 1, 5) = 'WEBSI'
						AND event.application__error__error_code_numeric BETWEEN 4000 AND 4999
					)
					THEN 'errors_other_setup'
					
					
				-- New logic for error categories based on modalView, feature name and operation additional information
					-- authentication
				WHEN (event.visit__application_details__application_name = 'SelfInstall'
						AND event.message__event_case_id = 'selfInstall_error_verifyLoginCredentials'
						AND event.message__feature__feature_name IS NULL AND event.application__error__error_type LIKE 'authentication%'
					)
					THEN 'errors_authentication'
					-- pre-activation
				WHEN (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id IN('selfInstall_error_deviceQualification_blacklistedCOAM', 'selfInstall_error_deviceQualification_minimallyQualifiedCOAM', 'selfInstall_error_missingEquipment', 'selfInstall_error_portPhoneTransfer', 'selfInstall_error_callUs', 'selfInstall_error_tryAgain')
						AND event.message__feature__feature_name = 'modem' AND event.application__error__error_type LIKE 'preActivation%'
					)
					THEN 'modem_errors_preActivation'
				WHEN (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id IN('selfInstall_error_deviceQualification_blacklistedCOAM', 'selfInstall_error_deviceQualification_minimallyQualifiedCOAM', 'selfInstall_error_missingEquipment', 'selfInstall_error_callUs', 'selfInstall_error_tryAgain')
						AND event.message__feature__feature_name = 'router' AND event.application__error__error_type LIKE 'preActivation%'
					)
					THEN 'router_errors_preActivation'
				WHEN (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id IN('selfInstall_error_deviceQualification_blacklistedCOAM', 'selfInstall_error_deviceQualification_minimallyQualifiedCOAM', 'selfInstall_error_missingEquipment', 'selfInstall_error_callUs', 'selfInstall_error_tryAgain')
						AND event.message__feature__feature_name = 'receiver' AND event.application__error__error_type LIKE 'preActivation%'
					)
					THEN 'tv_errors_preActivation'
				WHEN (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id IN('selfInstall_error_LOBOutage', 'selfInstall_error_acceptTermsConditions', 'selfInstall_error_callUs', 'selfInstall_error_tryAgain')
						AND event.message__feature__feature_name IS NULL AND event.application__error__error_type LIKE 'preActivation%'
					)
					THEN 'errors_preActivation'
					-- connection
				WHEN (event.visit__application_details__application_name = 'MySpectrum' 
						AND event.message__event_case_id IN('mySpectrum_modalView_equipment_selfInstall_callToTroubleshoot', 'mySpectrum_modalView_equipment_selfInstall_callToActivate', 'mySpectrum_modalView_equipment_selfInstall_activationError', 'mySpectrum_modalView_equipment_selfInstall_factoryReset', 'mySpectrum_modalView_equipment_selfInstall_modemConnectionIssue')
						AND event.message__feature__feature_name = 'modem' AND event.application__error__error_type LIKE 'connection%'
					)
					OR (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id IN('selfInstall_error_verifyDeviceOnline', 'selfInstall_error_verifyDeviceSignal')
						AND event.message__feature__feature_name = 'modem' AND event.application__error__error_type LIKE 'connection%'
					)
					THEN 'modem_errors_connection'
				WHEN (event.visit__application_details__application_name = 'MySpectrum' 
						AND event.message__event_case_id IN('mySpectrum_modalView_equipment_selfInstall_callToTroubleshoot', 'mySpectrum_modalView_equipment_selfInstall_callToActivate', 'mySpectrum_modalView_equipment_selfInstall_activationError', 'mySpectrum_modalView_equipment_selfInstall_resetRouter')
						AND event.message__feature__feature_name = 'router' AND event.application__error__error_type LIKE 'connection%'
					)
					OR (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id IN('selfInstall_error_verifyDeviceOnline', 'selfInstall_error_verifyDeviceSignal')
						AND event.message__feature__feature_name = 'router' AND event.application__error__error_type LIKE 'connection%'
					)
					THEN 'router_errors_connection'
				WHEN (event.visit__application_details__application_name = 'MySpectrum' 
						AND event.message__event_case_id IN('mySpectrum_modalView_equipment_selfInstall_receiverConnectionIssue', 'mySpectrum_modalView_equipment_selfInstall_receiverOneWayActivationError')
						AND event.message__feature__feature_name = 'receiver' AND event.application__error__error_type LIKE 'connection%'
					)
					OR (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id IN('selfInstall_error_verifyDeviceOnline', 'selfInstall_error_verifyDeviceSignal')
						AND event.message__feature__feature_name = 'receiver' AND event.application__error__error_type LIKE 'connection%'
					)
					THEN 'tv_errors_connection'
					--activation
				WHEN (event.visit__application_details__application_name = 'MySpectrum' 
						AND event.message__event_case_id IN('mySpectrum_modalView_equipment_selfInstall_callToTroubleshoot', 'mySpectrum_modalView_equipment_selfInstall_callToActivate', 'mySpectrum_modalView_equipment_selfInstall_activationError', 'mySpectrum_modalView_equipment_selfInstall_factoryReset', 'mySpectrum_modalView_equipment_selfInstall_modemConnectionIssue')
						AND event.message__feature__feature_name = 'modem' AND event.application__error__error_type LIKE 'activation%'
					)
					OR (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id IN('selfInstall_error_verifyEquipmentSetup', 'selfInstall_error_deviceQualification_nonCOAM', 'selfInstall_error_callUs', 'selfInstall_error_tryAgain')
						AND event.message__feature__feature_name = 'modem' AND event.application__error__error_type LIKE 'activation%'
					)
					THEN 'modem_errors_activation'
				WHEN (event.visit__application_details__application_name = 'MySpectrum' 
						AND event.message__event_case_id IN('mySpectrum_modalView_equipment_selfInstall_callToTroubleshoot', 'mySpectrum_modalView_equipment_selfInstall_callToActivate', 'mySpectrum_modalView_equipment_selfInstall_activationError', 'mySpectrum_modalView_equipment_selfInstall_resetRouter')
						AND event.message__feature__feature_name = 'router' AND event.application__error__error_type LIKE 'activation%'
					)
					OR (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id IN('selfInstall_error_verifyEquipmentSetup', 'selfInstall_error_deviceQualification_nonCOAM', 'selfInstall_error_callUs', 'selfInstall_error_tryAgain')
						AND event.message__feature__feature_name = 'router' AND event.application__error__error_type LIKE 'activation%'
					)
					THEN 'router_errors_activation'
				WHEN (event.visit__application_details__application_name = 'MySpectrum' 
						AND event.message__event_case_id IN('mySpectrum_modalView_equipment_selfInstall_receiverConnectionIssue', 'mySpectrum_modalView_equipment_selfInstall_receiverOneWayActivationError')
						AND event.message__feature__feature_name = 'receiver' AND event.application__error__error_type LIKE 'activation%'
					)
					OR (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id IN('selfInstall_error_verifyEquipmentSetup', 'selfInstall_error_deviceQualification_nonCOAM', 'selfInstall_error_callUs', 'selfInstall_error_tryAgain')
						AND event.message__feature__feature_name = 'receiver' AND event.application__error__error_type LIKE 'activation%'
					)
					THEN 'tv_errors_activation'
				WHEN (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id IN('selfInstall_error_callUs', 'selfInstall_error_tryAgain')
						AND event.message__feature__feature_name IS NULL AND event.application__error__error_type LIKE 'activation%'
					)
					THEN 'errors_activation'
				WHEN (event.visit__application_details__application_name = 'MySpectrum' 
						AND event.message__event_case_id IN('mySpectrum_modalView_equipment_selfInstall_callToTroubleshoot', 'mySpectrum_modalView_equipment_selfInstall_callToActivate', 'mySpectrum_modalView_equipment_selfInstall_activationError')
						AND event.message__feature__feature_name = 'not equipment related' AND event.application__error__error_type LIKE 'other%'
					)
					THEN 'other_errors'
					-- post-activation
				WHEN (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id = 'selfInstall_error_callUs'
						AND event.message__feature__feature_name = 'modem' AND event.application__error__error_type LIKE 'postActivation%'
					)
					THEN 'modem_errors_postActivation'
				WHEN (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id = 'selfInstall_error_callUs'
						AND event.message__feature__feature_name = 'router' AND event.application__error__error_type LIKE 'postActivation%'
					)
					THEN 'router_errors_postActivation'
				WHEN (event.visit__application_details__application_name = 'SelfInstall' 
						AND event.message__event_case_id = 'selfInstall_error_callUs'
						AND event.message__feature__feature_name = 'receiver' AND event.application__error__error_type LIKE 'postActivation%'
					)
					THEN 'tv_errors_postActivation'
					
					

			--g.	Customer Satisfaction Score (Web: N/A)
				--ii.	People who were successful/not
				WHEN --iOS, Android
					event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_feedback_satisfactionRating' 
					AND event.state__view__current_page__page_title = 'equipmentSelfInstall'
					THEN 'customer_satisfaction'

				--iii.	Total - Those who received and submitted
				WHEN --iOS, Android
					event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_selectAction_customerFeedback_feedbackFormSend'
					AND LAG(event.message__event_case_id, 1) OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 'mySpectrum_userFeedback_feedbackForm' 
					AND event.operation__user_entry__survey_id = 'equipmentSelfInstall'
					AND LEAD(UPPER(event.message__event_case_id), 1, '') OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) NOT LIKE '%ERROR%'
					THEN 'customer_satisfaction_submitted'

				--iv.	Average Submission Score of Total
				--Pending: review question type property
				WHEN --iOS, Android
					event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_userFeedback_satisfactionRating'
					AND event.operation__user_entry__survey_id = 'equipmentSelfInstall'
					AND LEAD(event.message__event_case_id, 1) OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 'mySpectrum_selectAction_experienceRatingCont'
					THEN 'customer_satisfaction_submission_score'
					
				WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_selectAction_downloadMSA' THEN 'download_MSA'
				
				--COAM
				WHEN (event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_equipmentSetup'
						AND LOWER(event.state__view__current_page__additional_information) LIKE '%coam%'
					)
					OR (event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_Generic_API_Call_Success', 'selfInstall_Generic_API_Call_Failure')
						AND event.application__api__response_text IN('equipmentWhitelisted', 'equipmentBlacklisted', 'equipmentMinimallyQualified')
					)
					THEN 'coam_device'
			ELSE NULL END AS event_tag,


			CASE
			---------------------------------------------------------------------------------------------------
			-- Funnel view (iOS, Android)
			---------------------------------------------------------------------------------------------------
				--Home Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_homeCard_selfInstall' THEN 'home_page_view'

				--Equipment Set Up Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstallStartPage' THEN 'equipment_setup_page_view'

				--Terms&Conditions
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.state__view__current_page__page_name = 'selfInstallTerms' AND event.message__event_case_id = 'mySpectrum_selectAction_equipment_selfInstall_termsAgree' THEN 'terms_conditions_page_view'

				--Equipment Checklist Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_equipmentSetup' THEN 'equipment_checklist_page_view'

				--Modem Checklist Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_modemSetupChecklist' THEN 'modem_checklist_page_view'
				
				--Modem connecting
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' AND event.state__view__current_page__elements__element_string_value = 'connecting'
					AND (event.message__feature__feature_name = 'modem' OR event.state__view__current_page__page_name IN('selfInstallModemSetup', 'selfInstallVerifyYourModem') OR event.next_page = 'selfInstallModemSetup') THEN 'modem_connecting'
					
				--Modem activating
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' AND event.state__view__current_page__elements__element_string_value = 'activating'
					AND (event.message__feature__feature_name = 'modem' OR event.state__view__current_page__page_name IN('selfInstallModemSetup', 'selfInstallVerifyYourModem')) THEN 'modem_activating'

				--Modem Activation Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_modemSetupSuccess' THEN 'modem_activation_page_view'
				
				--Voice Checklist Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_phoneSetupChecklist' THEN 'voice_checklist_page_view'
				
				--Voice activating
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' AND event.state__view__current_page__elements__element_string_value = 'activating'
					AND (event.message__feature__feature_name RLIKE '(phone|voice)' OR event.state__view__current_page__page_name LIKE 'selfInstallPhoneSetup%' OR event.state__view__current_page__page_name IN('selfInstallModemSetup', 'selfInstallVerifyYourModem')) THEN 'voice_activating'
				
				--Voice Activation Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_sharePhoneNumber' THEN 'voice_activation_page_view'

				--Router Checklist Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_routerSetupChecklist' THEN 'router_checklist_page_view'
				
				--Router connecting
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' AND event.state__view__current_page__elements__element_string_value = 'connecting'
					AND (event.message__feature__feature_name = 'router' OR event.state__view__current_page__page_name = 'selfInstallRouterSetup' OR event.next_page = 'selfInstallRouterSetup') THEN 'router_connecting'
					
				--Router activating
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' AND event.state__view__current_page__elements__element_string_value = 'activating'
					AND (event.message__feature__feature_name = 'router' OR event.state__view__current_page__page_name = 'selfInstallRouterSetup') THEN 'router_activating'
				
				--Network Setup Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_networkSetup' THEN 'network_setup_page_view'

				--Router Activation Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_routerSetupSuccess' THEN 'router_activation_page_view'

				--Total Activations, Total Failures, Total Calls, Total Abandonment, Total Retry are calculated above as part of Self-Install Definitions
				
				--Full Screen Takeover Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_fullScreenEquipmentSetup' THEN 'full_screen_takeover_page_view'
				
				
				--TV Checklist Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_receiverSetupChecklist' THEN 'tv_checklist_page_view'
				
				--TV connecting
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' AND event.state__view__current_page__elements__element_string_value = 'connecting'
					AND (event.message__feature__feature_name RLIKE '(receiver|video|tv)' OR event.state__view__current_page__page_name IN('Insert Batteries Into Remote', 'selfInstallReceiverSetup')) THEN 'tv_connecting'
					
				--TV activating
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' AND event.state__view__current_page__elements__element_string_value = 'activating'
					AND (event.message__feature__feature_name  RLIKE '(receiver|video|tv)' OR event.state__view__current_page__page_name IN('Insert Batteries Into Remote', 'selfInstallReceiverSetup')) THEN 'tv_activating'

				--TV Activation Page
				WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_receiverSetupSuccess' THEN 'tv_activation_page_view'
				
			
			---------------------------------------------------------------------------------------------------
			-- Funnel view (Web)
			---------------------------------------------------------------------------------------------------
				--Home Page
				WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_selectAction_unAuth_signIn', 'selfInstall_login_autoSuccess', 'selfInstall_login_manualSuccess', 'selfInstall_login_transparentSuccess') THEN 'home_page_view'
				
				--Transfer Page
				WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_pageView_transferAddressSelection' THEN 'transfer_page_view' --not included in the funnel
				
				--Terms&Conditions
				WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_selectAction_AcceptanceTermsContinue' 
					AND LEAD(event.message__event_case_id, 1) OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 'selfInstall_Generic_API_Call_Success' THEN 'terms_conditions_page_view'
				
				--Start Page
				WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_pageView_equipment_selfInstallStartPage' THEN 'equipment_checklist_page_view'
				
				--Ported Phone Confirmation Page
				WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_selectAction_portPhoneTransferContinue' 
					AND LEAD(event.message__event_case_id, 1) OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 'selfInstall_Generic_API_Call_Success' THEN 'ported_phone_confirmation_page_view' --not included in the funnel
				
				--Modem/Router/TV/Voice Success Page is calculated based on additionalInformation field
				--Activation Page
				WHEN event.visit__application_details__application_name = 'SelfInstall' 
					AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess') THEN 'web_activation_page_view'
				
			ELSE NULL END AS page_view_tag,

			CASE
				--ii.	New - pilot should only see this
				WHEN 
					(
						event.visit__application_details__application_name = 'MySpectrum' AND first_acct_id IS NULL AND event.message__event_case_id = 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment'
						AND event.received__timestamp = MIN(CASE WHEN event.message__event_case_id = 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment' THEN event.received__timestamp ELSE NULL END) OVER (PARTITION BY event.visit__account__enc_account_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
						--date when equipment arrives
						--Pending: Currently not getting Equipment data (if customer ordered equipment and what they ordered). Jordan is working with BI Team to get access to this dataset
					)
					OR (
						event.visit__application_details__application_name = 'SelfInstall'
						AND (
							(event.web_first_initiated_self_installs_timestamp IS NULL 
								AND event.message__timestamp = MIN(CASE WHEN event.message__event_case_id = 'selfInstall_pageView_equipment_selfInstallStartPage' THEN event.message__timestamp ELSE NULL END) OVER (PARTITION BY event.visit__account__enc_account_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
								AND event.state__view__current_page__additional_information RLIKE '"pendingservices":\\[*".*"\\]*'
							)
							OR (event.web_first_initiated_self_installs_timestamp IS NOT NULL AND event.message__event_case_id = 'selfInstall_pageView_equipment_selfInstallStartPage'
								AND event.message__timestamp = event.web_first_initiated_self_installs_timestamp
								AND event.state__view__current_page__additional_information RLIKE '"pendingservices":\\[*".*"\\]*'
								)
							)
						--date when equipment arrives
						--Pending: Currently not getting Equipment data (if customer ordered equipment and what they ordered). Jordan is working with BI Team to get access to this dataset
					)
					THEN 'initiated_self_installs_new'

				--iii.	Existing - FUTURE
				--Pending: this can be calculated in aggregation once we know what the source is to get date when equipment arrives
				WHEN 1 = 0 THEN 'initiated_self_installs_existing'
			ELSE NULL END AS initiated_self_installs_type,
			
			CASE WHEN --iOS, Android (Web: N/A)
				event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_userFeedback_satisfactionRating'
				AND event.operation__user_entry__survey_id = 'equipmentSelfInstall'
				AND LEAD(event.message__event_case_id, 1) OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 'mySpectrum_selectAction_experienceRatingCont'
				THEN event.operation__user_entry__numeric ELSE NULL END AS customer_satisfaction_submission_score,

			--Abandonment by inactivity
			CASE WHEN --iOS, Android (period of inactivity greater than 10 min between one event and other (after 2020-11-14, the period of inactivity is 20 min))
					event.visit__application_details__application_name = 'MySpectrum' AND event.message__time_since_last_event_ms > CASE WHEN event.denver_date < '2020-11-15' THEN 600000 ELSE 1200000 END THEN 1 
				 
				 --Web (Terms and Agreements: user reading and takes longer than 15 minutes)
				 WHEN event.visit__application_details__application_name = 'SelfInstall' 
					AND event.message__event_case_id IN('selfInstall_pageView_equipment_selfInstallStartPage', 'selfInstall_pageView_accountPastDue', 'selfInstall_pageView_transferAddressSelection')
						AND (event.received__timestamp - (FIRST_VALUE(CASE WHEN event.message__event_case_id = 'selfInstall_pageView_agreementTerms' THEN event.received__timestamp ELSE NULL END) OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))) > 900000
					THEN 11
				 --Web (Activation page takes more than 6 min)
				 WHEN event.visit__application_details__application_name = 'SelfInstall' 
					AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
					AND (event.received__timestamp - MAX(CASE WHEN event.message__event_case_id IN('selfInstall_pageView_equipment_selfInstallStartPage', 'selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess') THEN event.received__timestamp
							ELSE NULL END
							) OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) > 360000
					THEN 12
				 --Web (If browser closes prior to modem reboot, will not see the success message)
				 WHEN event.visit__application_details__application_name = 'SelfInstall' 
					AND MAX(CASE WHEN event.message__event_case_id = 'selfInstall_assetDisplayed_modemRebootSuccess' THEN 1 ELSE 0 END) OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 0
					THEN 13
			ELSE 0 END AS inactivity,
			
			CASE WHEN event.message__sequence_number = MIN(event.message__sequence_number) OVER (PARTITION BY event.denver_date, event.visit__application_details__application_name, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
				 THEN 1 ELSE 0 END visit_first_event_flag,
			CASE WHEN event.message__sequence_number = MAX(event.message__sequence_number) OVER (PARTITION BY event.denver_date, event.visit__application_details__application_name, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
				 THEN 1 ELSE 0 END visit_last_event_flag,
			first_acct_id,
			event.msa_first_initiated_self_installs_visit_id,
			event.msa_first_initiated_self_installs_timestamp,
			event.web_first_initiated_self_installs_visit_id,
			event.web_first_initiated_self_installs_timestamp,
			event.msa_complete_activation_self_installs_visit_id,
			event.msa_complete_activation_self_installs_timestamp,
			event.web_complete_activation_self_installs_visit_id,
			event.web_complete_activation_self_installs_timestamp,
			event.last_additional_information,
			event.last_additional_information_by_visit_id,
			event.prev_additional_information_by_visit_id,
			event.modem_activated_before_current_date,
			event.router_activated_before_current_date,
			event.tv_activated_before_current_date,
			event.voice_activated_before_current_date,
			
			-- activations using explode and get_json_object
			NVL(si_cqe_explode.modem_count, 0) AS modem_count,
			NVL(si_cqe_explode.pending_modem_count, 0) AS pending_modem_count,
			CASE WHEN event.visit__application_details__application_name = 'MySpectrum'
					AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
					AND LAG(event.message__event_case_id, 1, '') OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
					AND event.message__time_since_last_event_ms < 1000
					THEN 0
			ELSE NVL(si_cqe_explode.modem_setup_success, 0) END AS modem_setup_success,
			
			NVL(si_cqe_explode.router_count, 0) AS router_count,
			NVL(si_cqe_explode.pending_router_count, 0) AS pending_router_count,
			CASE WHEN event.visit__application_details__application_name = 'MySpectrum'
					AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
					AND LAG(event.message__event_case_id, 1, '') OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
					AND event.message__time_since_last_event_ms < 1000
					THEN 0
			ELSE NVL(si_cqe_explode.router_setup_success, 0) END AS router_setup_success,
			
			NVL(si_cqe_explode.tv_count, 0) AS tv_count,
			NVL(si_cqe_explode.pending_tv_count, 0) AS pending_tv_count,
			CASE WHEN event.visit__application_details__application_name = 'MySpectrum'
					AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
					AND LAG(event.message__event_case_id, 1, '') OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
					AND event.message__time_since_last_event_ms < 1000
					THEN 0
			ELSE NVL(si_cqe_explode.tv_setup_success, 0) END AS tv_setup_success,
			
			NVL(si_cqe_explode.voice_count, 0) AS voice_count,
			NVL(si_cqe_explode.pending_voice_count, 0) AS pending_voice_count,
			CASE WHEN event.visit__application_details__application_name = 'MySpectrum'
					AND event.message__event_case_id = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
					AND LAG(event.message__event_case_id, 1, '') OVER (PARTITION BY event.denver_date, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 'mySpectrum_applicationActivity_equipment_selfInstall_status' 
					AND event.message__time_since_last_event_ms < 1000
					THEN 0
			ELSE NVL(si_cqe_explode.voice_setup_success, 0) END AS voice_setup_success,
			
			
			--activations in current event (indivdual events)
			CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
					AND event.state__view__current_page__additional_information LIKE '%"internet":"activated"%' THEN 1
				 WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id IN('mySpectrum_pageView_equipment_selfInstall_equipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment')
						AND event.state__view__current_page__additional_information LIKE '%modem: activated%' THEN 1
			ELSE 0 END AS current_modem_setup_success,
			CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
					AND event.state__view__current_page__additional_information LIKE '%"wifi":"activated"%' THEN 1
				 WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id IN('mySpectrum_pageView_equipment_selfInstall_equipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment')
						AND event.state__view__current_page__additional_information LIKE '%router: activated%' THEN 1
			ELSE 0 END AS current_router_setup_success,
			CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
				AND event.state__view__current_page__additional_information RLIKE '"(video|tv)":"activated"' THEN 1 ELSE 0 END AS current_tv_setup_success,
			CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
				AND (event.state__view__current_page__additional_information RLIKE '"(phone|voice)":"activated"'
						--in case user has two lines
						OR event.state__view__current_page__additional_information RLIKE '"(phone|voice)":"activated, activated"'
					) THEN 1 ELSE 0 END AS current_voice_setup_success,
			
			--activations in prev success event (between one Activation page and another)
			CASE WHEN event.prev_additional_information_by_visit_id IS NULL THEN event.modem_activated_before_current_date
				 WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
					AND event.prev_additional_information_by_visit_id LIKE '%"internet":"activated"%' THEN 1
				 WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id IN('mySpectrum_pageView_equipment_selfInstall_equipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment')
					AND event.prev_additional_information_by_visit_id LIKE '%modem: activated%' THEN 1 
			ELSE 0 END AS prev_modem_setup_success,
			CASE WHEN event.prev_additional_information_by_visit_id IS NULL THEN event.router_activated_before_current_date
				 WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
					AND event.prev_additional_information_by_visit_id LIKE '%"wifi":"activated"%' THEN 1 
				 WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id IN('mySpectrum_pageView_equipment_selfInstall_equipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_setupEquipment')
					AND event.prev_additional_information_by_visit_id LIKE '%router: activated%' THEN 1
			ELSE 0 END AS prev_router_setup_success,
			CASE WHEN event.prev_additional_information_by_visit_id IS NULL THEN event.tv_activated_before_current_date
				 WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
					AND event.prev_additional_information_by_visit_id RLIKE '"(video|tv)":"activated"' THEN 1
			ELSE 0 END AS prev_tv_setup_success,
			CASE WHEN event.prev_additional_information_by_visit_id IS NULL THEN event.voice_activated_before_current_date
				 WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
					AND (event.prev_additional_information_by_visit_id RLIKE '"(phone|voice)":"activated"'
							--in case user has two lines
							OR event.prev_additional_information_by_visit_id RLIKE '"(phone|voice)":"activated, activated"'
						) THEN 1
			ELSE 0 END AS prev_voice_setup_success,
			
			--failures at the end of the visit (across the visit)
			MAX(CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
						AND event.state__view__current_page__additional_information LIKE '%"internet":"failed"%' THEN 1
				ELSE 0 END)
					OVER (PARTITION BY event.denver_date, event.visit__application_details__application_name, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS modem_setup_failure,
			MAX(CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
						AND event.state__view__current_page__additional_information LIKE '%"wifi":"failed"%' THEN 1
				ELSE 0 END)
					OVER (PARTITION BY event.denver_date, event.visit__application_details__application_name, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS router_setup_failure,
			MAX(CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
						AND event.state__view__current_page__additional_information RLIKE '"(video|tv)":"failed"' THEN 1
				ELSE 0 END)
				OVER (PARTITION BY event.denver_date, event.visit__application_details__application_name, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS tv_setup_failure,
			MAX(CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
					AND (event.state__view__current_page__additional_information RLIKE '"(phone|voice)":"failed"'
						--in case user has two lines
						OR event.state__view__current_page__additional_information RLIKE '"(phone|voice)":"failed, failed"'
						OR event.state__view__current_page__additional_information RLIKE '"(phone|voice)":"activated, failed"'
						OR event.state__view__current_page__additional_information RLIKE '"(phone|voice)":"failed, activated"') THEN 1
				ELSE 0 END)
				OVER (PARTITION BY event.denver_date, event.visit__application_details__application_name, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS voice_setup_failure,
			
			
			--failures in current event (individual events)
			CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
				AND event.state__view__current_page__additional_information LIKE '%"internet":"failed"%' THEN 1 ELSE 0 END AS current_modem_setup_failure,
			CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
				AND event.state__view__current_page__additional_information LIKE '%"wifi":"failed"%' THEN 1 ELSE 0 END AS current_router_setup_failure,
			CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
				AND event.state__view__current_page__additional_information RLIKE '"(video|tv)":"failed"' THEN 1 ELSE 0 END AS current_tv_setup_failure,
			CASE WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id IN('selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure', 'selfInstall_assetDisplayed_modemRebootSuccess')
				AND (event.state__view__current_page__additional_information RLIKE '"(phone|voice)":"failed"'
						OR event.state__view__current_page__additional_information RLIKE '"(phone|voice)":"failed, failed"'
						OR event.state__view__current_page__additional_information RLIKE '"(phone|voice)":"activated, failed"'
						OR event.state__view__current_page__additional_information RLIKE '"(phone|voice)":"failed, activated"'
					) THEN 1 ELSE 0 END AS current_voice_setup_failure,
					
			--equipment type (SCP, Monolithic)
			CASE WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_equipmentSetup' 
				AND LOWER(event.state__view__current_page__additional_information) LIKE '%scp%'
				THEN 1 ELSE 0 END AS equipment_type_scp,
				
			CASE WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_equipmentSetup' 
				AND LOWER(event.state__view__current_page__additional_information) LIKE '%monolithic%'
				THEN 1 ELSE 0 END AS equipment_type_monolithic,
			
			MAX(CASE WHEN event.visit__application_details__application_name = 'MySpectrum' AND event.message__event_case_id IN('mySpectrum_applicationActivity_homeCard_selfInstall', 'mySpectrum_pageView_equipment_selfInstall_fullScreenEquipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_fullScreenEquipmentSetup', 'mySpectrum_selectAction_equipment_selfInstall_notNow') THEN 1 
					 WHEN event.visit__application_details__application_name = 'SelfInstall' AND event.message__event_case_id = 'selfInstall_pageView_equipment_selfInstallStartPage'
						AND event.state__view__current_page__additional_information RLIKE '"pendingservices":\\[*".*"\\]*' THEN 1 
				ELSE 0 END)
				OVER (PARTITION BY event.denver_date, event.visit__application_details__application_name, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
				AS visit_eligible,
				
			CASE WHEN --iOS, Android
					(event.visit__application_details__application_name = 'MySpectrum'
						AND event.interaction = 0
						AND event.message__event_case_id = 'mySpectrum_featureStart_equipment_selfInstall'
						AND event.message__timestamp = MIN(CASE WHEN event.message__event_case_id = 'mySpectrum_featureStart_equipment_selfInstall' THEN event.message__timestamp ELSE NULL END) OVER (PARTITION BY event.visit__application_details__application_name, event.visit__account__enc_account_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
						AND event.message__sequence_number <> LAG(event.message__sequence_number, 1, -1) OVER (PARTITION BY event.visit__application_details__application_name, event.visit__account__enc_account_number, event.visit__visit_id ORDER BY event.message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
					)
					THEN 1 ELSE 0 END AS interaction
		FROM ${env:TMP_db}.si_cqe_eligible_accounts event
		LEFT JOIN ${env:TMP_db}.si_cqe_explode
			ON si_cqe_explode.received__timestamp                            = event.received__timestamp
			AND si_cqe_explode.denver_date_hour                              = event.denver_date_hour
			AND si_cqe_explode.visit__application_details__application_name  = event.visit__application_details__application_name
			AND si_cqe_explode.visit__application_details__application_type  = event.visit__application_details__application_type
			AND si_cqe_explode.message__timestamp                            = event.message__timestamp
			AND si_cqe_explode.message__event_case_id                        = event.message__event_case_id
			AND si_cqe_explode.message__sequence_number                      = event.message__sequence_number
			AND si_cqe_explode.visit__account__enc_account_number            = event.visit__account__enc_account_number
			AND si_cqe_explode.visit__visit_id                               = event.visit__visit_id
	) pb_user_package
) page_base_tag
;

--Get event data, add marks for each metric
SELECT "\n\nFor 1c: si_summary_page_base_master\n\n";

INSERT OVERWRITE TABLE si_summary_page_base_master PARTITION (denver_date)
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
	CASE WHEN TRIM(user_package) = '' THEN NULL ELSE user_package END AS user_package,
	sequence_number,
	CASE WHEN sequence_number = last_activation_page_sequence_number AND activation_complete_internal = 1 
			THEN CONCAT(event_tag, '+activation_complete')
	ELSE event_tag END AS event_tag,
	page_view_tag,
	initiated_self_installs,
	initiated_self_installs_new,
	initiated_self_installs_existing,
	account_status_past_due,
	CASE WHEN MAX(account_status_past_due) OVER (PARTITION BY visit_id, acct_id, denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 0 THEN 1 ELSE 0 END AS account_status_current,
	terms_conditions_accept,
	terms_conditions_disagree,
	inactivity,
	abandonment_exiting_via_overflow_menu,

	--ACTIVATIONS
	--Total activations
		--Total No Touch
		CASE WHEN visit_last_event_flag = 1 
				AND (modem_setup_success = 1 OR router_setup_success = 1 OR tv_setup_success = 1 OR voice_setup_success = 1
					) AND activation_self_install_call = 0 AND activation_truck_roll_call = 0 
				THEN 1 
		ELSE 0 END AS activation_no_touch,

		--Total Calls
		activation_user_call,

		--Self-Install Calls
		activation_self_install_call,

		--Total Truck Roll
		activation_truck_roll_call,

	--activation - success
	CASE WHEN visit_last_event_flag = 1 AND activation_success_full = 1 THEN 1 ELSE 0 END AS activation_success_full,
	
	CASE WHEN visit_last_event_flag = 1 AND activation_success_full = 0 AND (modem_setup_success > 0 OR router_setup_success > 0 OR tv_setup_success > 0 OR voice_setup_success > 0) THEN 1
	ELSE 0 END activation_success_partial,

	--activation - failure
	CASE WHEN visit_last_event_flag = 1 AND activation_success_full = 0 AND (modem_setup_failure + router_setup_failure + tv_setup_failure + voice_setup_failure) > 0 THEN 1 ELSE 0 END AS activation_failure,

	CASE WHEN visit_last_event_flag = 1 AND activation_success_full = 0
		AND (modem_setup_failure + router_setup_failure + tv_setup_failure + voice_setup_failure) > 0
		AND (CASE WHEN pending_modem_count > 0 THEN 1 ELSE 0 END + CASE WHEN pending_router_count > 0 THEN 1 ELSE 0 END + CASE WHEN pending_tv_count > 0 THEN 1 ELSE 0 END + CASE WHEN pending_voice_count > 0 THEN 1 ELSE 0 END) = (modem_setup_failure + router_setup_failure + tv_setup_failure + voice_setup_failure) THEN 1
	 ELSE 0 END activation_failure_full,

	CASE WHEN visit_last_event_flag = 1 AND activation_success_full = 0 
		AND (modem_setup_failure + router_setup_failure + tv_setup_failure + voice_setup_failure) > 0
		AND (CASE WHEN pending_modem_count > 0 THEN 1 ELSE 0 END + CASE WHEN pending_router_count > 0 THEN 1 ELSE 0 END + CASE WHEN pending_tv_count > 0 THEN 1 ELSE 0 END + CASE WHEN pending_voice_count > 0 THEN 1 ELSE 0 END) <> (modem_setup_failure + router_setup_failure + tv_setup_failure + voice_setup_failure) THEN 1
	ELSE 0 END activation_failure_partial,

	--Calls
	CASE WHEN received_timestamp >= activation_timestamp_call AND activation_self_install_call = 1 THEN 1 ELSE 0 END AS activation_failure_calls,

	--Truck Rolls
	CASE WHEN received_timestamp >= activation_timestamp_call AND activation_truck_roll_call = 1 THEN 1 ELSE 0 END AS activation_failure_truck_rolls,

	re_entry,
	retry,

	--ERRORS
	errors_connection_issue,
	errors_equipment_setup,


	--CUSTOMER SATISFACTION
	customer_satisfaction,
	customer_satisfaction_submitted,
	customer_satisfaction_submission_score,

	--FUNNEL VIEW
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

	'${hiveconf:grain}' AS date_level,
	acct_division,
	acct_division_id,
	msa_first_initiated_self_installs_visit_id,
	msa_first_initiated_self_installs_timestamp,
	web_first_initiated_self_installs_visit_id,
	web_first_initiated_self_installs_timestamp,
	event_case_id,
	msa_complete_activation_self_installs_visit_id,
	msa_complete_activation_self_installs_timestamp,
	FIRST_VALUE(CASE WHEN application_group = 'SelfInstall' AND web_complete_activation_self_installs_timestamp IS NULL AND sequence_number = last_activation_page_sequence_number AND activation_complete_internal = 1 THEN visit_id
		ELSE web_complete_activation_self_installs_visit_id END, TRUE) OVER (PARTITION BY acct_id, denver_date ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS web_complete_activation_self_installs_visit_id,
	FIRST_VALUE(CASE WHEN application_group = 'SelfInstall' AND web_complete_activation_self_installs_timestamp IS NULL AND sequence_number = last_activation_page_sequence_number AND activation_complete_internal = 1 THEN received_timestamp
		ELSE web_complete_activation_self_installs_timestamp END, TRUE) OVER (PARTITION BY acct_id, denver_date ORDER BY received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS web_complete_activation_self_installs_timestamp,
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
	
	--ERRORS (cont)
	errors_authentication_setup,
	errors_device_signal_setup,
	errors_other_setup,
	
	--FUNNEL VIEW (cont)
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
	
	--COAM device
	coam_device,
	whitelisted_coam_device,
	blacklisted_coam_device,
	minimally_qualified_coam_device,
	
	--equipment type
	equipment_type_scp,
	equipment_type_monolithic,
	
	visit_eligible,
	
	--audit fields
	'v8.11' AS code_version,
	CURRENT_TIMESTAMP AS record_load_timestamp,
	
	--repair calls
	repair_call,
	repair_truck_roll_call,
	CASE WHEN visit_last_event_flag = 1 
				AND (modem_setup_success = 1 OR router_setup_success = 1 OR tv_setup_success = 1 OR voice_setup_success = 1
					) AND repair_call = 0 AND repair_truck_roll_call = 0 
				THEN 1 
		ELSE 0 END AS repair_no_touch,
	
	--FUNNEL VIEW (cont)
	modem_activating,
	modem_connecting,
	router_activating,
	router_connecting,
	full_screen_takeover_page_view,
	
	--activation - success (cont)
	activation_complete,
	
	--ERRORS (cont, new logic based on modalView, feature name and operation additional information)
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
	
	--device count
	modem_count,
	router_count,
	tv_count,
	voice_count,
	
	--FUNNEL VIEW (cont)
	tv_checklist_page_view,
	tv_activating,
	tv_connecting,
	
	--device count (cont)
	pending_modem_count,
	pending_router_count,
	pending_tv_count,
	pending_voice_count,
	
	--ERRORS (cont, new logic based on modalView, feature name and operation additional information)
	modem_errors_postActivation,
	router_errors_postActivation,
	tv_errors_postActivation,
	
	--FUNNEL VIEW (cont)
	voice_checklist_page_view,
	voice_activating,
	
	denver_date
FROM(
	SELECT
		received_timestamp,
		denver_date,
		visit_id,
		device_id,
		acct_id,
		acct_division,
		acct_division_id,
		denver_date_hour,
		current_page_name,
		standardize_name,
		modal_name,
		error_type,
		error_code,
		feature_step_name,
		event_case_id,
		application_group,
		application_type,
		mso,
		user_package,
		sequence_number,
		event_tag,
		page_view_tag,
		CASE WHEN event_tag = 'initiated_self_installs' THEN 1 ELSE 0 END AS initiated_self_installs,
		interaction,
		CASE WHEN initiated_self_installs_type = 'initiated_self_installs_new' THEN 1 ELSE 0 END AS initiated_self_installs_new,
		CASE WHEN initiated_self_installs_type = 'initiated_self_installs_existing' THEN 1 ELSE 0 END AS initiated_self_installs_existing,
		CASE WHEN event_tag = 'account_status_past_due' THEN 1 ELSE 0 END AS account_status_past_due,
		CASE WHEN event_tag = 'terms_conditions_accept' THEN 1 ELSE 0 END AS terms_conditions_accept,
		CASE WHEN event_tag = 'terms_conditions_disagree' THEN 1 ELSE 0 END AS terms_conditions_disagree,
		
		CASE WHEN 
				--iOS, Android
				(
					application_group = 'MySpectrum'
					AND (
						--there was a period of more than 10 min of inactivity from prev event, user has initiated o reentry and has not completed the process
						(inactivity = 1 
							AND received_timestamp >= initiation_reentry_self_install_timestamp 
							AND received_timestamp <= CASE WHEN msa_complete_activation_self_installs_timestamp IS NULL THEN CAST(922337203685470001 AS BIGINT) ELSE msa_complete_activation_self_installs_timestamp END
						)
						--it is the last visit event, has not completed Self-Install yet, subsequent visit in same day is after 10 min (after 2020-11-14 inactivity is after 20 min)
						OR (visit_last_event_flag = 1 AND received_timestamp >= initiation_reentry_self_install_timestamp 
							AND received_timestamp <= CASE WHEN msa_complete_activation_self_installs_timestamp IS NULL THEN CAST(922337203685470002 AS BIGINT) ELSE msa_complete_activation_self_installs_timestamp END
							AND (LEAD(received_timestamp, 1, CAST(922337203685470003 AS BIGINT)) OVER (PARTITION BY denver_date, application_group, acct_id ORDER BY received_timestamp, sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - received_timestamp) > CASE WHEN denver_date < '2020-11-15' THEN 600000 ELSE 1200000 END
							AND LAST_VALUE(event_tag, TRUE) OVER (PARTITION BY denver_date, acct_id, visit_id ORDER BY sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) != 'abandonment_exiting_via_overflow_menu'
							)
					)
				)
				--Web
				OR (application_group = 'SelfInstall' AND received_timestamp >= initiation_reentry_self_install_timestamp AND inactivity = 11)
				OR (application_group = 'SelfInstall' AND received_timestamp >= initiation_reentry_self_install_timestamp AND inactivity = 12)
				OR (application_group = 'SelfInstall' AND received_timestamp >= initiation_reentry_self_install_timestamp AND visit_last_event_flag = 1 AND inactivity = 13 AND (has_modem = 1 OR has_voice = 1))
			THEN 1 ELSE 0 END AS inactivity,
		
		CASE WHEN application_group = 'MySpectrum' AND event_tag = 'abandonment_exiting_via_overflow_menu' 
				AND received_timestamp >= initiation_reentry_self_install_timestamp AND received_timestamp <= CASE WHEN msa_complete_activation_self_installs_timestamp IS NULL THEN CAST(922337203685470004 AS BIGINT) ELSE msa_complete_activation_self_installs_timestamp END
			THEN 1 ELSE 0 END AS abandonment_exiting_via_overflow_menu,
		
		visit_first_event_flag,
		visit_last_event_flag,
		first_acct_id,
		msa_first_initiated_self_installs_visit_id,
		msa_first_initiated_self_installs_timestamp,
		web_first_initiated_self_installs_visit_id,
		web_first_initiated_self_installs_timestamp,
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
		has_modem,
		has_router,
		has_tv,
		has_voice,
		modem_count,
		pending_modem_count,
		modem_setup_success,
		router_count,
		pending_router_count,
		router_setup_success,
		tv_count,
		pending_tv_count,
		tv_setup_success,
		voice_count,
		pending_voice_count,
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
		last_activation_page_sequence_number,
		modem_setup_failure,
		router_setup_failure,
		tv_setup_failure,
		voice_setup_failure,
		additional_information,
		api_response_text,
		modem_activated_prev_visit_same_date,
		router_activated_prev_visit_same_date,
		tv_activated_prev_visit_same_date,
		voice_activated_prev_visit_same_date,
		CASE WHEN application_group = 'MySpectrum' AND event_tag = 'abandonment_via_exit_setup' 
				AND received_timestamp >= initiation_reentry_self_install_timestamp AND received_timestamp <= CASE WHEN msa_complete_activation_self_installs_timestamp IS NULL THEN CAST(922337203685470010 AS BIGINT) ELSE msa_complete_activation_self_installs_timestamp END
			THEN 1 ELSE 0 END AS abandonment_via_exit_setup,

		--ACTIVATIONS
		--Total activations
			--Total No Touch
			--Comment: calculated at aggregation level

			--Total Calls
			NVL(user_call, 0) AS activation_user_call,
			timestamp_call AS activation_timestamp_call,

			--Self-Install Calls
			NVL(self_install_call, 0) AS activation_self_install_call,

			--Total Truck Roll
			NVL(truck_roll_call, 0) AS activation_truck_roll_call,
			
			--Repair calls
			NVL(repair_call, 0) AS repair_call,
			NVL(repair_truck_roll_call, 0) AS repair_truck_roll_call,

		--activation - success
		MAX(CASE 
				 WHEN user_package = 'Modem' AND modem_is_done = 1 THEN 1
				 WHEN user_package = 'Router' AND router_is_done = 1 THEN 1
				 WHEN user_package = 'TV' AND tv_is_done = 1 THEN 1
				 WHEN user_package = 'Voice' AND voice_is_done = 1 THEN 1
				 WHEN user_package = 'Modem+Router' AND modem_is_done = 1 AND router_is_done = 1 THEN 1
				 WHEN user_package = 'Modem+Router+TV' AND modem_is_done = 1 AND router_is_done = 1 AND tv_is_done = 1 THEN 1
				 WHEN user_package = 'Modem+Router+Voice' AND modem_is_done = 1 AND router_is_done = 1 AND voice_is_done = 1 THEN 1
				 WHEN user_package = 'Modem+Router+TV+Voice' AND modem_is_done = 1 AND router_is_done = 1 AND tv_is_done = 1 AND voice_is_done = 1 THEN 1
				 WHEN user_package = 'Modem+TV+Voice' AND modem_is_done = 1 AND tv_is_done = 1 AND voice_is_done = 1 THEN 1
				 WHEN user_package = 'Modem+TV' AND modem_is_done = 1 AND tv_is_done = 1 THEN 1
				 WHEN user_package = 'Modem+Voice' AND modem_is_done = 1 AND voice_is_done = 1 THEN 1
				 WHEN user_package = 'Router+TV+Voice' AND router_is_done = 1 AND tv_is_done = 1 AND voice_is_done = 1 THEN 1
				 WHEN user_package = 'Router+TV' AND router_is_done = 1 AND tv_is_done = 1 THEN 1
				 WHEN user_package = 'Router+Voice' AND router_is_done = 1 AND voice_is_done = 1 THEN 1
				 WHEN user_package = 'TV+Voice' AND tv_is_done = 1 AND voice_is_done = 1 THEN 1
			ELSE 0 END) OVER (PARTITION BY visit_id, acct_id, denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS activation_complete_internal,
		MAX( 
			CASE WHEN max_activation_complete = 1 THEN 1 
				WHEN -- there are pending devices/services
					(pending_modem_count + pending_router_count + pending_tv_count + pending_voice_count) > 0
					-- pending devices/services is equalt to activations (+1 in case modem appears as not activated at the beggining of the visit but it appears as activated after modem activation)
				 	AND (pending_modem_count + pending_router_count + pending_tv_count + pending_voice_count) = (modem_setup_success + router_setup_success + tv_setup_success + voice_setup_success) + (CASE WHEN activation_complete = 1 AND ((pending_modem_count > modem_setup_success) OR (pending_router_count > router_setup_success) or (pending_tv_count > tv_setup_success) OR (pending_voice_count > voice_setup_success)) THEN 1 ELSE 0 END) THEN 1
			ELSE 0 END
		)
			OVER (PARTITION BY visit_id, acct_id, denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS activation_success_full,
		
		activation_complete,

			--Calls
			--Comment: calculated at aggregation level

			--Truck Rolls
			--Comment: calculated at aggregation level

		CASE WHEN application_group = 'MySpectrum' AND event_tag = 're_entry' AND msa_first_initiated_self_installs_timestamp < received_timestamp THEN 1 
			 WHEN application_group = 'SelfInstall' AND event_tag = 're_entry' THEN 1
		ELSE 0 END AS re_entry,
		has_initiation_reentry_self_install,
		initiation_reentry_self_install_timestamp,
		
		CASE WHEN application_group = 'MySpectrum' AND event_tag = 'retry' THEN 1
			 WHEN application_group = 'SelfInstall' AND event_case_id = 'selfInstall_error_tryAgain' THEN 1
		ELSE 0 END AS retry,
		
		CASE WHEN event_tag = 'download_MSA' THEN 1 ELSE 0 END AS download_MSA,

		--ERRORS
		CASE WHEN event_tag = 'errors_connection_issue' THEN 1 ELSE 0 END AS errors_connection_issue,
		CASE WHEN event_tag = 'errors_equipment_setup' THEN 1 ELSE 0 END AS errors_equipment_setup,
		CASE WHEN event_tag = 'errors_authentication_setup' THEN 1 ELSE 0 END AS errors_authentication_setup,
		CASE WHEN event_tag = 'errors_device_signal_setup' THEN 1 ELSE 0 END AS errors_device_signal_setup,
		CASE WHEN event_tag = 'errors_other_setup' THEN 1 ELSE 0 END AS errors_other_setup,
		
		CASE WHEN event_tag = 'modem_errors_preActivation' THEN 1 ELSE 0 END AS modem_errors_preActivation,
		CASE WHEN event_tag = 'modem_errors_connection' THEN 1 ELSE 0 END AS modem_errors_connection,
		CASE WHEN event_tag = 'modem_errors_activation' THEN 1 ELSE 0 END AS modem_errors_activation,
		CASE WHEN event_tag = 'modem_errors_postActivation' THEN 1 ELSE 0 END AS modem_errors_postActivation,
		CASE WHEN event_tag = 'router_errors_preActivation' THEN 1 ELSE 0 END AS router_errors_preActivation,
		CASE WHEN event_tag = 'router_errors_connection' THEN 1 ELSE 0 END AS router_errors_connection,
		CASE WHEN event_tag = 'router_errors_activation' THEN 1 ELSE 0 END AS router_errors_activation,
		CASE WHEN event_tag = 'router_errors_postActivation' THEN 1 ELSE 0 END AS router_errors_postActivation,
		CASE WHEN event_tag = 'tv_errors_preActivation' THEN 1 ELSE 0 END AS tv_errors_preActivation,
		CASE WHEN event_tag = 'tv_errors_connection' THEN 1 ELSE 0 END AS tv_errors_connection,
		CASE WHEN event_tag = 'tv_errors_activation' THEN 1 ELSE 0 END AS tv_errors_activation,
		CASE WHEN event_tag = 'tv_errors_postActivation' THEN 1 ELSE 0 END AS tv_errors_postActivation,
		CASE WHEN event_tag = 'errors_authentication' THEN 1 ELSE 0 END AS errors_authentication,
		CASE WHEN event_tag = 'errors_preActivation' THEN 1 ELSE 0 END AS errors_preActivation,
		CASE WHEN event_tag = 'errors_activation' THEN 1 ELSE 0 END AS errors_activation,
		CASE WHEN event_tag = 'other_errors' THEN 1 ELSE 0 END AS other_errors,

		--CUSTOMER SATISFACTION
		CASE WHEN event_tag = 'customer_satisfaction' THEN 1 ELSE 0 END AS customer_satisfaction,
		CASE WHEN event_tag = 'customer_satisfaction_submitted' THEN 1 ELSE 0 END AS customer_satisfaction_submitted,
		CASE WHEN event_tag = 'customer_satisfaction_submission_score'
			AND sequence_number = MAX(CASE WHEN event_tag = 'customer_satisfaction_submission_score' THEN sequence_number ELSE NULL END) OVER (PARTITION BY visit_id, acct_id, denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			THEN customer_satisfaction_submission_score ELSE NULL END AS customer_satisfaction_submission_score,

		--FUNNEL VIEW
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'home_page_view' THEN 1 
			 WHEN application_group = 'SelfInstall' AND page_view_tag = 'home_page_view' THEN 1 
		ELSE 0 END AS home_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'equipment_setup_page_view' THEN 1 ELSE 0 END AS equipment_setup_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'terms_conditions_page_view' THEN 1
			 WHEN application_group = 'SelfInstall' AND page_view_tag = 'terms_conditions_page_view' THEN 1
		ELSE 0 END AS terms_conditions_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'service_terms_conditions_page_view' THEN 1 ELSE 0 END AS service_terms_conditions_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'equipment_checklist_page_view' THEN 1
			 WHEN application_group = 'SelfInstall' AND page_view_tag = 'equipment_checklist_page_view' THEN 1
		ELSE 0 END AS equipment_checklist_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'modem_checklist_page_view' THEN 1 ELSE 0 END AS modem_checklist_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'modem_activating' THEN 1 ELSE 0 END AS modem_activating,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'modem_connecting' THEN 1 ELSE 0 END AS modem_connecting,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'modem_activation_page_view' THEN 1 
			 WHEN application_group = 'SelfInstall' AND page_view_tag = 'web_activation_page_view' AND modem_activated = 1 THEN 1 
		ELSE 0 END AS modem_activation_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'router_checklist_page_view' THEN 1 ELSE 0 END AS router_checklist_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'router_activating' THEN 1 ELSE 0 END AS router_activating,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'router_connecting' THEN 1 ELSE 0 END AS router_connecting,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'network_setup_page_view' THEN 1 ELSE 0 END AS network_setup_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'router_activation_page_view' THEN 1 
			 WHEN application_group = 'SelfInstall' AND page_view_tag = 'web_activation_page_view' AND router_activated = 1 THEN 1 
		ELSE 0 END AS router_activation_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'tv_checklist_page_view' THEN 1 ELSE 0 END AS tv_checklist_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'tv_activating' THEN 1 ELSE 0 END AS tv_activating,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'tv_connecting' THEN 1 ELSE 0 END AS tv_connecting,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'tv_activation_page_view' THEN 1 
			 WHEN application_group = 'SelfInstall' AND page_view_tag = 'web_activation_page_view' AND tv_activated = 1 THEN 1 ELSE 0 END AS tv_activation_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'voice_checklist_page_view' THEN 1 ELSE 0 END AS voice_checklist_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'voice_activating' THEN 1 ELSE 0 END AS voice_activating,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'voice_activation_page_view' THEN 1 
			WHEN application_group = 'SelfInstall' AND page_view_tag = 'web_activation_page_view' AND voice_activated = 1 THEN 1 ELSE 0 END AS voice_activation_page_view,
		CASE WHEN application_group = 'MySpectrum' AND page_view_tag = 'full_screen_takeover_page_view' THEN 1 ELSE 0 END AS full_screen_takeover_page_view,
		
		--COAM device
		CASE WHEN application_group IN('MySpectrum', 'SelfInstall') AND event_tag = 'coam_device' THEN 1 ELSE 0 END AS coam_device,
		CASE WHEN application_group = 'SelfInstall' AND event_tag = 'coam_device' AND api_response_text = 'equipmentWhitelisted' THEN 1 ELSE 0 END AS whitelisted_coam_device,
		CASE WHEN application_group = 'SelfInstall' AND event_tag = 'coam_device' AND api_response_text = 'equipmentBlacklisted' THEN 1 ELSE 0 END AS blacklisted_coam_device,
		CASE WHEN application_group = 'SelfInstall' AND event_tag = 'coam_device' AND api_response_text = 'equipmentMinimallyQualified' THEN 1 ELSE 0 END AS minimally_qualified_coam_device,
		
		--equipment type
		equipment_type_scp,
		equipment_type_monolithic,
		
		visit_eligible
	FROM(
		SELECT  DISTINCT
			page_base_tag.received_timestamp,
			page_base_tag.visit_id,
			page_base_tag.device_id,
			page_base_tag.acct_id,
			page_base_tag.acct_division,
			page_base_tag.acct_division_id,
			page_base_tag.denver_date,
			page_base_tag.denver_date_hour,
			page_base_tag.current_page_name,
			page_base_tag.standardize_name,
			page_base_tag.modal_name,
			page_base_tag.error_type,
			page_base_tag.error_code,
			page_base_tag.feature_step_name,
			page_base_tag.sequence_number,
			page_base_tag.time_since_last_event_ms,
			page_base_tag.event_case_id,
			page_base_tag.application_group,
			page_base_tag.application_type,
			page_base_tag.mso,
			page_base_tag.user_package,
			page_base_tag.prev_user_package,			
			page_base_tag.event_tag,
			page_base_tag.page_view_tag,
			page_base_tag.initiated_self_installs_type,
			page_base_tag.customer_satisfaction_submission_score,
			page_base_tag.inactivity,
			page_base_tag.visit_first_event_flag,
			page_base_tag.visit_last_event_flag,
			page_base_tag.first_acct_id,
			page_base_tag.has_initiation_reentry_self_install,
			page_base_tag.initiation_reentry_self_install_timestamp,
			page_base_tag.msa_first_initiated_self_installs_visit_id,
			page_base_tag.msa_first_initiated_self_installs_timestamp,
			page_base_tag.web_first_initiated_self_installs_visit_id,
			page_base_tag.web_first_initiated_self_installs_timestamp,
			page_base_tag.msa_complete_activation_self_installs_visit_id,
			page_base_tag.msa_complete_activation_self_installs_timestamp,
			page_base_tag.web_complete_activation_self_installs_visit_id,
			page_base_tag.web_complete_activation_self_installs_timestamp,
			page_base_tag.application_version,
			page_base_tag.message_name,
			page_base_tag.transaction_id,
			page_base_tag.message_context,
			page_base_tag.triggered_by,
			page_base_tag.operation_success,
			page_base_tag.api_response_time,
			page_base_tag.api_http_verb,
			page_base_tag.api_path,
			page_base_tag.message_timestamp,
			page_base_tag.has_modem,
			page_base_tag.has_router,
			page_base_tag.has_tv,
			page_base_tag.has_voice,
			page_base_tag.modem_count,
			page_base_tag.pending_modem_count,
			page_base_tag.modem_setup_success,
			page_base_tag.router_count,
			page_base_tag.pending_router_count,
			page_base_tag.router_setup_success,
			page_base_tag.tv_count,
			page_base_tag.pending_tv_count,
			page_base_tag.tv_setup_success,
			page_base_tag.voice_count,
			page_base_tag.pending_voice_count,
			page_base_tag.voice_setup_success,
			page_base_tag.current_modem_setup_success,
			page_base_tag.current_router_setup_success,
			page_base_tag.current_tv_setup_success,
			page_base_tag.current_voice_setup_success,
			page_base_tag.prev_modem_setup_success,
			page_base_tag.prev_router_setup_success,
			page_base_tag.prev_tv_setup_success,
			page_base_tag.prev_voice_setup_success,
			page_base_tag.modem_activated_before_current_date,
			page_base_tag.router_activated_before_current_date,
			page_base_tag.tv_activated_before_current_date,
			page_base_tag.voice_activated_before_current_date,
			page_base_tag.current_modem_setup_failure,
			page_base_tag.current_router_setup_failure,
			page_base_tag.current_tv_setup_failure,
			page_base_tag.current_voice_setup_failure,
			page_base_tag.modem_activated,
			page_base_tag.router_activated,
			page_base_tag.tv_activated,
			page_base_tag.voice_activated,
			page_base_tag.last_activation_page_sequence_number,
			page_base_tag.activation_failure,
			page_base_tag.modem_setup_failure,
			page_base_tag.router_setup_failure,
			page_base_tag.tv_setup_failure,
			page_base_tag.voice_setup_failure,
			page_base_tag.additional_information,
			page_base_tag.api_response_text,
			page_base_tag.modem_activated_prev_visit_same_date,
			page_base_tag.router_activated_prev_visit_same_date,
			page_base_tag.tv_activated_prev_visit_same_date,
			page_base_tag.voice_activated_prev_visit_same_date,
			page_base_tag.equipment_type_scp,
			page_base_tag.equipment_type_monolithic,
			page_base_tag.visit_eligible,
			page_base_tag.interaction,
			page_base_tag.activation_complete,
			page_base_tag.max_activation_complete,
			
			
			CASE WHEN page_base_tag.visit_id = FIRST_VALUE(CASE WHEN page_base_tag.modem_setup_success = 1 THEN page_base_tag.visit_id ELSE NULL END, TRUE) 
									OVER (PARTITION BY page_base_tag.acct_id, page_base_tag.denver_date ORDER BY page_base_tag.received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
					OR page_base_tag.modem_activated_before_current_date = 1
					OR page_base_tag.modem_activated_prev_visit_same_date = 1 THEN 1 ELSE 0 END AS modem_is_done,
			CASE WHEN page_base_tag.visit_id = FIRST_VALUE(CASE WHEN page_base_tag.router_setup_success = 1 THEN page_base_tag.visit_id ELSE NULL END, TRUE) 
									OVER (PARTITION BY page_base_tag.acct_id, page_base_tag.denver_date ORDER BY page_base_tag.received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
					OR page_base_tag.router_activated_before_current_date = 1
					OR page_base_tag.router_activated_prev_visit_same_date = 1 THEN 1 ELSE 0 END AS router_is_done,
			CASE WHEN page_base_tag.visit_id = FIRST_VALUE(CASE WHEN page_base_tag.tv_setup_success = 1 THEN page_base_tag.visit_id ELSE NULL END, TRUE) 
									OVER (PARTITION BY page_base_tag.acct_id, page_base_tag.denver_date ORDER BY page_base_tag.received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
					OR page_base_tag.tv_activated_before_current_date = 1
					OR page_base_tag.tv_activated_prev_visit_same_date = 1 THEN 1 ELSE 0 END AS tv_is_done,
			CASE WHEN page_base_tag.visit_id = FIRST_VALUE(CASE WHEN page_base_tag.voice_setup_success = 1 THEN page_base_tag.visit_id ELSE NULL END, TRUE) 
									OVER (PARTITION BY page_base_tag.acct_id, page_base_tag.denver_date ORDER BY page_base_tag.received_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
					OR page_base_tag.voice_activated_before_current_date = 1
					OR page_base_tag.voice_activated_prev_visit_same_date = 1 THEN 1 ELSE 0 END AS voice_is_done,
			
			--ii.	Total Calls - 30
			COUNT(call.encrypted_padded_account_number_256) 
				OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_call,
			MIN(CASE WHEN call.encrypted_padded_account_number_256 IS NOT NULL THEN call.call_start_timestamp_utc ELSE NULL END) 
				OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS timestamp_call,
			SUM(CASE WHEN call.encrypted_padded_account_number_256 IS NOT NULL AND call.self_install_call = 1 THEN 1 ELSE 0 END) 
				OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS self_install_call,

			--iii.	Total Truck Rolls - 15
			SUM(CASE WHEN call.encrypted_padded_account_number_256 IS NOT NULL AND call.truck_roll_resolution = 1 THEN 1 ELSE 0 END) 
				OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS truck_roll_call,
				
			--Repair calls
			SUM(CASE WHEN call.encrypted_padded_account_number_256 IS NOT NULL AND call.repair_call = 1 THEN 1 ELSE 0 END) 
				OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS repair_call,
			SUM(CASE WHEN call.encrypted_padded_account_number_256 IS NOT NULL AND call.repair_truck_roll_call = 1 THEN 1 ELSE 0 END) 
				OVER (PARTITION BY page_base_tag.visit_id, page_base_tag.acct_id, page_base_tag.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS repair_truck_roll_call
		FROM(
			SELECT
				event.received_timestamp,
				event.visit_id,
				event.device_id,
				event.acct_id,
				event.acct_division,
				event.acct_division_id,
				event.denver_date,
				event.denver_date_hour,
				event.current_page_name,
				event.standardize_name,
				event.modal_name,
				event.error_type,
				event.error_code,
				event.feature_step_name,
				event.sequence_number,
				event.time_since_last_event_ms,
				event.event_case_id,
				event.application_group,
				event.application_type,
				event.mso,
				event.user_package,
				event.prev_user_package,
				
				--Web only: SCP/ACS-managed routers in legacy charter market are always shown active to the customer. Once HSD is provisioned these devices are enabled by default without the need for provisioning
				CASE WHEN event.application_group = 'SelfInstall' AND event.mso = 'L-CHTR' AND event.event_tag LIKE '%modem_setup_success%' AND user_package LIKE '%Router%' THEN CONCAT(event.event_tag, '+router_setup_success_by_default') ELSE event.event_tag END AS event_tag,
				event.page_view_tag,
				event.initiated_self_installs_type,
				event.customer_satisfaction_submission_score,
				event.inactivity,
				event.visit_first_event_flag,
				event.visit_last_event_flag,
				event.first_acct_id,
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
				event.has_modem,
				event.has_router,
				event.has_tv,
				event.has_voice,
				event.modem_count,
				CASE WHEN event.pending_modem_count = 0 AND event.modem_setup_success > 0 THEN event.modem_setup_success ELSE event.pending_modem_count END AS pending_modem_count,
				event.modem_setup_success,
				event.router_count,
				CASE WHEN event.pending_router_count = 0 AND event.router_setup_success > 0 THEN event.router_setup_success ELSE event.pending_router_count END AS pending_router_count,
				CASE WHEN event.pending_router_count > 0 AND event.router_setup_success = 0 AND max_activation_complete = 1 THEN event.pending_router_count ELSE event.router_setup_success END AS router_setup_success,
				event.tv_count,
				CASE WHEN event.pending_tv_count = 0 AND event.tv_setup_success > 0 THEN event.tv_setup_success ELSE event.pending_tv_count END AS pending_tv_count,
				CASE WHEN event.pending_tv_count > 0 AND event.tv_setup_success = 0 AND max_activation_complete = 1 THEN event.pending_tv_count ELSE event.tv_setup_success END AS tv_setup_success,
				event.voice_count,
				CASE WHEN event.pending_voice_count = 0 AND event.voice_setup_success > 0 THEN event.voice_setup_success ELSE event.pending_voice_count END AS pending_voice_count,
				CASE WHEN event.pending_voice_count > 0 AND event.voice_setup_success = 0 AND max_activation_complete = 1 THEN event.pending_voice_count ELSE event.voice_setup_success END AS voice_setup_success,
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
				event.last_activation_page_sequence_number,
				event.activation_failure,
				event.modem_setup_failure,
				event.router_setup_failure,
				event.tv_setup_failure,
				event.voice_setup_failure,
				event.additional_information,
				event.api_response_text,
				event.equipment_type_scp,
				event.equipment_type_monolithic,
				event.visit_eligible,
				event.interaction,
				
				CASE WHEN event.event_tag = 'activation_complete' THEN 1 
					 WHEN event.event_tag = 'activation_complete_featureStop' AND MAX(CASE WHEN event.event_tag = 'activation_complete' THEN 1 ELSE 0 END) OVER (PARTITION BY event.visit_id, event.acct_id, event.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 0 THEN 1
				ELSE 0 END AS activation_complete,
				
				event.max_activation_complete,
				
				--activation in prev visit in same date
				MAX(event.modem_setup_success) OVER (PARTITION BY event.acct_id, event.denver_date ORDER BY event.message_timestamp, event.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS modem_activated_prev_visit_same_date,
				MAX(event.router_setup_success) OVER (PARTITION BY event.acct_id, event.denver_date ORDER BY event.message_timestamp, event.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS router_activated_prev_visit_same_date,
				MAX(event.tv_setup_success) OVER (PARTITION BY event.acct_id, event.denver_date ORDER BY event.message_timestamp, event.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS tv_activated_prev_visit_same_date,
				MAX(event.voice_setup_success) OVER (PARTITION BY event.acct_id, event.denver_date ORDER BY event.message_timestamp, event.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS voice_activated_prev_visit_same_date,
								
				--has initiated/re-entried Self-Install
				MAX(CASE WHEN event.event_tag = 'initiated_self_installs' OR event.event_tag = 're_entry' THEN 1 ELSE 0 END) OVER (PARTITION BY event.visit_id, event.acct_id, event.denver_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS has_initiation_reentry_self_install,
				FIRST_VALUE(CASE WHEN event.event_tag = 'initiated_self_installs' OR event.event_tag = 're_entry' THEN received_timestamp ELSE NULL END, TRUE) OVER (PARTITION BY event.visit_id, event.acct_id, event.denver_date ORDER BY event.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS initiation_reentry_self_install_timestamp,
				
				--Event/timestamp when user initiates Self-Install for the first time (MSA)
				CASE WHEN event.msa_first_initiated_self_installs_visit_id IS NULL THEN FIRST_VALUE(CASE WHEN event.event_tag = 'initiated_self_installs' AND application_group = 'MySpectrum' THEN event.visit_id ELSE NULL END, TRUE) OVER (PARTITION BY event.acct_id ORDER BY event.message_timestamp, event.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
					ELSE event.msa_first_initiated_self_installs_visit_id END AS msa_first_initiated_self_installs_visit_id,
				CASE WHEN event.msa_first_initiated_self_installs_timestamp IS NULL THEN FIRST_VALUE(CASE WHEN event.event_tag = 'initiated_self_installs' AND application_group = 'MySpectrum' THEN event.received_timestamp ELSE NULL END, TRUE) OVER (PARTITION BY event.acct_id ORDER BY event.message_timestamp, event.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
					ELSE event.msa_first_initiated_self_installs_timestamp END AS msa_first_initiated_self_installs_timestamp,

				--Event/timestamp when user initiates Self-Install for the first time (Web)
				CASE WHEN event.web_first_initiated_self_installs_visit_id IS NULL THEN FIRST_VALUE(CASE WHEN event.event_tag = 'initiated_self_installs' AND application_group = 'SelfInstall' THEN event.visit_id ELSE NULL END, TRUE) OVER (PARTITION BY event.acct_id ORDER BY event.message_timestamp, event.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
					ELSE event.web_first_initiated_self_installs_visit_id END AS web_first_initiated_self_installs_visit_id,
				CASE WHEN event.web_first_initiated_self_installs_timestamp IS NULL THEN FIRST_VALUE(CASE WHEN event.event_tag = 'initiated_self_installs' AND application_group = 'SelfInstall' THEN event.received_timestamp ELSE NULL END, TRUE) OVER (PARTITION BY event.acct_id ORDER BY event.message_timestamp, event.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
					ELSE event.web_first_initiated_self_installs_timestamp END AS web_first_initiated_self_installs_timestamp,
				
				--Event/timestamp when user completes Self-Install for the first time (MSA)
				CASE WHEN event.msa_complete_activation_self_installs_visit_id IS NULL 
						THEN FIRST_VALUE(CASE 
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem' AND event.event_tag = 'modem_setup_success' THEN event.visit_id
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Router' AND event.event_tag = 'router_setup_success' THEN event.visit_id
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'TV' AND event.event_tag = 'tv_setup_success' THEN event.visit_id
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Voice' AND event.event_tag = 'voice_setup_success' THEN event.visit_id
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+Router' AND event.event_tag = 'router_setup_success' THEN event.visit_id
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+Router+TV' AND event.event_tag = 'tv_setup_success' THEN event.visit_id
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+Router+TV+Voice' AND event.event_tag = 'voice_setup_success' THEN event.visit_id
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+TV+Voice' AND event.event_tag = 'voice_setup_success' THEN event.visit_id
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+TV' AND event.event_tag = 'tv_setup_success' THEN event.visit_id
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+Voice' AND event.event_tag = 'voice_setup_success' THEN event.visit_id
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'TV+Voice' AND event.event_tag = 'voice_setup_success' THEN event.visit_id
										ELSE NULL END, TRUE) 
								OVER (PARTITION BY event.acct_id ORDER BY event.message_timestamp, event.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
					ELSE event.msa_complete_activation_self_installs_visit_id END AS msa_complete_activation_self_installs_visit_id,
				
				
				CAST(
					CASE WHEN event.msa_complete_activation_self_installs_timestamp IS NULL 
							THEN FIRST_VALUE(CASE 
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem' AND event.event_tag = 'modem_setup_success' THEN event.received_timestamp
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Router' AND event.event_tag = 'router_setup_success' THEN event.received_timestamp
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'TV' AND event.event_tag = 'tv_setup_success' THEN event.received_timestamp
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Voice' AND event.event_tag = 'voice_setup_success' THEN event.received_timestamp
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+Router' AND event.event_tag = 'router_setup_success' THEN event.received_timestamp
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+Router+TV' AND event.event_tag = 'tv_setup_success' THEN event.received_timestamp
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+Router+TV+Voice' AND event.event_tag = 'voice_setup_success' THEN event.received_timestamp
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+TV+Voice' AND event.event_tag = 'voice_setup_success' THEN event.received_timestamp
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+TV' AND event.event_tag = 'tv_setup_success' THEN event.received_timestamp
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'Modem+Voice' AND event.event_tag = 'voice_setup_success' THEN event.received_timestamp
											  WHEN event.application_group = 'MySpectrum' AND event.user_package = 'TV+Voice' AND event.event_tag = 'voice_setup_success' THEN event.received_timestamp
											ELSE NULL END, TRUE) 
									OVER (PARTITION BY event.acct_id ORDER BY event.message_timestamp, event.sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
						ELSE event.msa_complete_activation_self_installs_timestamp 
					END
				AS BIGINT) AS msa_complete_activation_self_installs_timestamp,
				
				--Event/timestamp when user completes Self-Install for the first time (Web) - will be calculated at after determining all devices have been activated
				event.web_complete_activation_self_installs_visit_id,
				event.web_complete_activation_self_installs_timestamp
			FROM ${env:TMP_db}.si_summary_page_base_tag event
			WHERE
				--Records wit no tag are not needed for Summary aggregation
				(event.event_tag IS NOT NULL OR event.page_view_tag IS NOT NULL OR event.visit_first_event_flag = 1 OR event.visit_last_event_flag = 1 OR message_name = 'featureStop'
				OR event.event_case_id = 'selfInstall_error_tryAgain'
				OR 	(event.application_group = 'MySpectrum'
					AND (
						   (event.api_http_verb = 'GET'  AND event.api_path = '/api/pub/equipment-activation/v1/equipment')
						OR (event.api_http_verb = 'GET'  AND event.api_path = '/api/pub/equipment-activation/v1/eligibility')
						OR (event.api_http_verb = 'POST' AND event.api_path = '/api/pub/equipment-activation/v1/connectivitycheck')
						OR (event.api_http_verb = 'POST' AND event.api_path = '/api/pub/equipment-activation/v1/activate')
						OR (event.api_http_verb = 'POST' AND event.api_path = '/api/pub/equipment-activation/v1/status')
						OR (event.api_http_verb = 'GET'  AND event.api_path = '/api/pub/equipment/v3/lob/Internet')
						OR (event.api_http_verb = 'POST' AND event.api_path LIKE '/api/pub/equipment/v2/router/%')
						)
				)
				OR
					(event.application_group = 'SelfInstall'
						AND (
							   (event.api_http_verb = 'GET'  AND event.api_path = '/check-auto-activation-eligible')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/auto-activation-eligible')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/modem-details')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/device-qualifications')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/auto-activation-device-online/device-online-statuses')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/closeSodiOrder')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/inventory/move-equipment')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/outage-info')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/activation/reprovision-service-requests')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/routing-details')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/conversations/customerevent')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/modem/resetModem')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/modem/checkForResetSuccess')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/wifi-router-device-type-check')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/activation/checkIfVoiceAvailable/')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/activation/checkForVideoAndRetrieveEmtaDevice')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/agreement/logAgreements')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/agreement/getAgreements/')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/agreement/saveAgreements/')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/agreement/saveMinimallyQualifiedDeviceAgreement')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/agreement/minimally-qualified-device-agreement-text')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/status/auto-activation')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/status/auto-activation-health')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/auto-activation/activate-service-requests')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/inventory/devicesAndServices')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/auto-activation-device-online/device-online-statuses')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/account/retrieveCustomerDetails')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/account/check-inactive-account-status')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/account/verify-Seasonal-Status')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/account/verify-Delinquency-Status')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/login/attempt')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/login/logout')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/jcaptcha/validateJcaptcha')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/modem-provision-details')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/fedauth-authenticate')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/ospPortingDataCorrection')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/userlogin')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/account/findAccount')
							OR (event.api_http_verb = 'POST' AND event.api_path = '/sdp/publishDevicesAndServicesAfterActivation')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/invalidAttempts')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/getAccountNumberLengths')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/auto-activation-health-check-enabled-status')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/getModemResetConfig/')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/createIdentityURL')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/resetIdentityURL')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/bypassSpectrumIDLoginPage')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/makePaymentURL')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/checkForCaptcha')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/post-activation-inventory-check-delay')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/captchaKey')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/getTimeForE911TPVPooling')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/quantum-endpoint')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/quantum-feature-flag')
							OR (event.api_http_verb = 'GET'  AND event.api_path = '/property/environment')
							)
					)
				)
		) page_base_tag
		LEFT JOIN ${env:TMP_db}.si_summary_call_resolution_tmp call
			ON page_base_tag.acct_id = call.encrypted_padded_account_number_256
				AND CASE
						WHEN page_base_tag.mso = 'L-CHTR' THEN 'CHR'
						WHEN page_base_tag.mso = 'L-TWC' THEN 'TWC'
						WHEN page_base_tag.mso = 'L-BHN' THEN 'BHN'
						ELSE ''
					END = call.account_agent_mso
				--Calls within 24 hours
				AND page_base_tag.received_timestamp <= call.call_start_timestamp_utc
				AND (call.call_start_timestamp_utc - page_base_tag.received_timestamp) <= 86400000
				AND page_base_tag.event_tag = 'initiated_self_installs'
	) selfinstall_flag
) selfinstall_base
;

--Cross-Platform
INSERT OVERWRITE TABLE si_cross_platform PARTITION (denver_date)
SELECT
	acct_id,
	mso,
	user_package,
	msa_first_initiated_self_installs_visit_id,
	msa_first_initiated_self_installs_timestamp,
	msa_complete_activation_self_installs_visit_id,
	msa_complete_activation_self_installs_timestamp,
	web_first_initiated_self_installs_visit_id,
	web_first_initiated_self_installs_timestamp,
	web_complete_activation_self_installs_visit_id,
	web_complete_activation_self_installs_timestamp,
	msa_initiated_self_install,
	web_initiated_self_install,
	msa_complete_self_install,
	web_complete_self_install,
	device_activated,
	denver_date
FROM(
	SELECT 
		current_activation.acct_id,
		current_activation.mso,
		MAX(current_activation.user_package) AS user_package,
		MIN(prev_initiation.msa_first_initiated_self_installs_visit_id) AS msa_first_initiated_self_installs_visit_id,
		MIN(prev_initiation.msa_first_initiated_self_installs_timestamp) AS msa_first_initiated_self_installs_timestamp,
		MAX(current_activation.msa_complete_activation_self_installs_visit_id) AS msa_complete_activation_self_installs_visit_id,
		MAX(current_activation.msa_complete_activation_self_installs_timestamp) AS msa_complete_activation_self_installs_timestamp,
		MIN(prev_initiation.web_first_initiated_self_installs_visit_id) AS web_first_initiated_self_installs_visit_id,
		MIN(prev_initiation.web_first_initiated_self_installs_timestamp) AS web_first_initiated_self_installs_timestamp,
		MAX(current_activation.web_complete_activation_self_installs_visit_id) AS web_complete_activation_self_installs_visit_id,
		MAX(current_activation.web_complete_activation_self_installs_timestamp) AS web_complete_activation_self_installs_timestamp,
		MAX(CASE WHEN prev_initiation.msa_first_initiated_self_installs_timestamp > 0 THEN 1 ELSE 0 END) AS msa_initiated_self_install,
		MAX(CASE WHEN prev_initiation.web_first_initiated_self_installs_timestamp > 0 THEN 1 ELSE 0 END) AS web_initiated_self_install,
		--something was activated
		MAX(CASE WHEN current_activation.application_group = 'MySpectrum' AND (current_activation.activation_success_full = 1 OR current_activation.activation_success_partial = 1) THEN 1 ELSE 0 END) AS msa_complete_self_install,
		MAX(CASE WHEN current_activation.application_group = 'SelfInstall' AND (current_activation.activation_success_full = 1 OR current_activation.activation_success_partial = 1) THEN 1 ELSE 0 END) AS web_complete_self_install,
		MAX(CASE WHEN current_activation.activation_success_full = 1 OR current_activation.activation_success_partial = 1 THEN 1 ELSE 0 END) AS device_activated,
		current_activation.denver_date
	FROM si_summary_page_base_master current_activation
	LEFT JOIN (
		SELECT DISTINCT
			acct_id,
			mso,
			user_package,
			--MSA
			msa_first_initiated_self_installs_visit_id,
			msa_first_initiated_self_installs_timestamp,
			--Web
			web_first_initiated_self_installs_visit_id,
			web_first_initiated_self_installs_timestamp
		FROM si_summary_page_base_master
		WHERE denver_date <= '${hiveconf:START_DATE}'
			AND event_tag = 'initiated_self_installs'
			AND (msa_first_initiated_self_installs_timestamp IS NOT NULL OR web_first_initiated_self_installs_timestamp IS NOT NULL)
	) prev_initiation
		ON current_activation.acct_id = prev_initiation.acct_id
			AND current_activation.mso = prev_initiation.mso
	WHERE denver_date >= '${hiveconf:START_DATE}'
		AND denver_date < '${hiveconf:END_DATE}'
		--user has activity in both the platforms
		AND prev_initiation.msa_first_initiated_self_installs_timestamp IS NOT NULL
		AND prev_initiation.web_first_initiated_self_installs_timestamp IS NOT NULL
	GROUP BY current_activation.acct_id,
			current_activation.mso,
			current_activation.user_package,
			current_activation.denver_date
) AS act
WHERE device_activated = 1
;

DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_eligible_accounts PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_summary_page_base_tag PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_cqe_explode PURGE;
