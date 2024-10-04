USE ${env:DASP_db};

-- TODO: find suitable parameters for tez. current split.maxsize and split.minsize parameters seem like are not used.
-- SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
set hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;
-- default logic is ignored and Tez tries to group splits into the specified number of groups. Change that parameter carefully.
set tez.grouping.split-count=1200;

set hive.vectorized.execution.enabled = false;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

SELECT "\n\nFor : ${env:TMP_db}.si_composite_score_core_events_daily\n\n";
--create common temporary TABLES

DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_core_events_daily PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_composite_score_core_events_daily AS
SELECT
	received_timestamp,
	application_type,
	application_version,
	mso,
	acct_id,
	visit_id,
	application_group,
	message_name,
	sequence_number,
	transaction_id,
	event_case_id,
	user_package,
	event_tag,

	--must be same condition as 'activation start' in transaction_status (calculated field)
	CASE WHEN event_tag = 'initiated_self_installs' THEN received_timestamp ELSE NULL END AS transaction_start_timestamp,

	CASE WHEN --message_name = 'featureStop' AND 
		received_timestamp = MAX(CASE WHEN event_tag IN('activation_success', 'activation_failure') THEN received_timestamp ELSE NULL END) OVER (PARTITION BY visit_id, acct_id, denver_date, application_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN received_timestamp 
	ELSE NULL END AS transaction_end_timestamp,

	CASE WHEN event_tag = 'api_response_time' THEN api_response_time ELSE NULL END AS api_response_time,

	--Definitions
	CASE
		--Activation Start
		WHEN event_tag = 'initiated_self_installs' THEN 'activation start'

		--Activation End
		WHEN --message_name = 'featureStop' AND 
			received_timestamp = MAX(CASE WHEN event_tag IN('activation_success', 'activation_failure') THEN received_timestamp ELSE NULL END) OVER (PARTITION BY visit_id, acct_id, denver_date, application_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			THEN 'activation end'
	ELSE NULL END AS transaction_status,

	--Comment: overall activation (modem and router)
	CASE WHEN event_tag = 'activation_success' THEN 1 ELSE 0 END AS activation_success,

	CASE WHEN event_tag = 'activation_failure' THEN 1 ELSE 0 END AS activation_failure,

	'${hiveconf:grain}' AS date_level,
	denver_date
FROM(
	SELECT
		event.received_timestamp,
		event.application_type,
		event.application_version,
		event.mso,
		event.acct_id,
		event.visit_id,
		event.application_group,
		event.message_name,
		event.sequence_number,
		event.transaction_id,
		event.event_case_id,
		event.api_response_time,
		event.user_package,
		event.denver_date,

		CASE
			WHEN event.event_tag IN('initiated_self_installs', 're_entry') AND received_timestamp = initiated_self_installs_timestamp THEN 'initiated_self_installs'

			WHEN
				--iOS, Android
				(event.application_group = 'MySpectrum'
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
				THEN 'api_response_time'

			--Comment: overall activation (modem and router)
			WHEN 
				--iOS, Android
				(
					event.application_group = 'MySpectrum'
					AND event.sequence_number BETWEEN event.penultimate_activation_page_sequence_number AND event.last_activation_page_sequence_number
					AND (event.event_tag RLIKE '(modem|router|tv|voice)_setup_success' OR event.event_tag LIKE '%activation_complete%' OR event.event_tag RLIKE '(modem|router|tv|voice)_status_finished')
				)
				--Web
				OR (
					event.application_group = 'SelfInstall'
					AND event.sequence_number BETWEEN event.penultimate_activation_page_sequence_number AND event.last_activation_page_sequence_number
					AND (event.event_tag LIKE '%activation_complete%' OR event.event_tag RLIKE '(modem|router|tv|voice)_setup_success')
				)
				THEN 'activation_success'

			WHEN 
				--iOS, Android
				(
					event.application_group = 'MySpectrum' 
					AND event.sequence_number BETWEEN event.penultimate_activation_page_sequence_number AND event.last_activation_page_sequence_number
					AND event.operation_success = false AND event.message_context = 'equipmentSelfInstall' 
					AND event.triggered_by = 'application' AND event.message_name = 'featureStop'
				)
				--Web
				OR (
					event.application_group = 'SelfInstall'
					AND event.sequence_number BETWEEN event.penultimate_activation_page_sequence_number AND event.last_activation_page_sequence_number
					AND event.operation_success = false AND event.message_context = 'equipmentSelfInstall' 
					AND event.triggered_by = 'application' AND event.message_name = 'error'
				)
				THEN 'activation_failure'
		ELSE NULL END AS event_tag
	FROM(
		SELECT
			received_timestamp,
			application_type,
			application_version,
			application_group,
			user_package,
			mso,
			acct_id,
			visit_id,
			message_name,
			sequence_number,
			transaction_id,
			event_case_id,
			event_tag,
			message_context,
			triggered_by,
			operation_success,
			current_page_name,
			api_response_time,
			api_http_verb,
			api_path,
			last_activation_page_sequence_number,
			denver_date,
			activation_complete,
			penultimate_activation_page_sequence_number,
			initiated_self_installs_timestamp
		FROM(
			SELECT
				received_timestamp,
				application_type,
				application_version,
				application_group,
				user_package,
				mso,
				acct_id,
				visit_id,
				message_name,
				sequence_number,
				transaction_id,
				event_case_id,
				event_tag,
				message_context,
				triggered_by,
				operation_success,
				current_page_name,
				api_response_time,
				api_http_verb,
				api_path,
				last_activation_page_sequence_number,
				denver_date,
				
				MAX(CASE WHEN event_tag LIKE '%activation_complete%' THEN 1 ELSE 0 END) 
					OVER (PARTITION BY visit_id, acct_id, denver_date, application_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS activation_complete,
				
				MAX(CASE WHEN application_group = 'MySpectrum' AND sequence_number < last_activation_page_sequence_number 
							AND (event_tag RLIKE '(modem|router|tv|voice)_status_finished' OR event_tag RLIKE '(modem|router|tv|voice)_errors') THEN sequence_number
						 WHEN application_group = 'SelfInstall' AND sequence_number < last_activation_page_sequence_number 
							AND (event_case_id IN('selfInstall_pageView_equipment_selfInstallStartPage', 'selfInstall_pageView_equipmentSetupSuccess', 'selfInstall_pageView_equipmentSetupFullFailure', 'selfInstall_pageView_equipmentSetupPartialFailure')
							) THEN sequence_number
					ELSE 0 END) 
					OVER (PARTITION BY visit_id, acct_id, denver_date, application_group ORDER BY sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS penultimate_activation_page_sequence_number,
				
				MIN(CASE WHEN event_tag IN('initiated_self_installs', 're_entry') THEN received_timestamp ELSE 922337203685470001 END)
					OVER (PARTITION BY visit_id, acct_id, denver_date, application_group ORDER BY sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS initiated_self_installs_timestamp,
				
				
				--eligible conditions for composite score
				MAX(CASE 
						WHEN (CASE WHEN pending_modem_count > 0 THEN 1 ELSE 0 END + CASE WHEN pending_router_count > 0 THEN 1 ELSE 0 END + CASE WHEN pending_tv_count > 0 THEN 1 ELSE 0 END + CASE WHEN pending_voice_count > 0 THEN 1 ELSE 0 END)
							= (CASE WHEN (modem_setup_success > 0 OR modem_setup_failure > 0) THEN 1 ELSE 0 END + CASE WHEN (router_setup_success > 0 OR router_setup_failure > 0) THEN 1 ELSE 0 END + CASE WHEN (tv_setup_success > 0 OR tv_setup_failure > 0) THEN 1 ELSE 0 END + CASE WHEN (voice_setup_success > 0 OR voice_setup_failure > 0) THEN 1 ELSE 0 END)
						THEN 1
					ELSE 0 END)
					OVER (PARTITION BY visit_id, acct_id, denver_date, application_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS composite_score_eligible
			FROM si_summary_page_base_master
			WHERE denver_date >= '${hiveconf:START_DATE}'
				AND denver_date < '${hiveconf:END_DATE}'
		) attempt
		WHERE composite_score_eligible = 1
	) event
) composite_score_tag
;


SELECT "\n\nFor 1: Self-Install\n\n";
--Self-Install (MySpectrum)
SET activation_allowed_failures = 1;
SET activation_average_transaction_duration_sec = array(60, 180, 240, 300);
SET activation_average_API_response_time_ms = array(450, 950, 3450, 5915);
SET activation_weights_duration_then_API_response_time = array(0.5, 0.5);


SELECT "\n\nFor 1a: ${env:TMP_db}.si_composite_score_activation_events_daily\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_activation_events_daily PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_composite_score_activation_events_daily AS
SELECT
	denver_date,
	application_group,
	mso,
	acct_id,
	visit_id,
	application_version,
	user_package,
	date_level,
	SUM(activation_success) AS activation_success,
	SUM(activation_failure) AS activation_failure,
	AVG(transaction_duration) AS avg_transaction_duration,
	AVG(avg_api_response_time) AS avg_api_response_time
FROM(
	SELECT
		t1.denver_date,
		t1.application_group,
		t1.mso,
		t1.acct_id,
		t1.visit_id,
		t1.application_version,
		t1.user_package,
		t1.date_level,
		AVG(activation_success) AS activation_success,
		AVG(activation_failure) AS activation_failure,
		CAST(AVG(transaction_end_timestamp - transaction_start_timestamp) / 1000 AS BIGINT) AS transaction_duration,
		AVG(api_response_time) AS avg_api_response_time
	FROM(
		SELECT
			denver_date,
			application_group,
			application_version,
			user_package,
			mso,
			acct_id,
			visit_id,
			date_level,
			SUM(activation_success) AS activation_success,
			SUM(activation_failure) AS activation_failure,
			MAX(transaction_start_timestamp) AS transaction_start_timestamp,
			MAX(transaction_end_timestamp) AS transaction_end_timestamp,
			MAX(CASE WHEN transaction_status = 'activation start' THEN TRUE ELSE FALSE END) AS has_activation_start,
			MAX(CASE WHEN transaction_status = 'activation end' THEN TRUE ELSE FALSE END) AS has_activation_end,
			
			MIN(CASE WHEN transaction_status = 'activation start' THEN sequence_number ELSE 922337203685470003 END) AS min_sequence_number,
			MAX(CASE WHEN transaction_status = 'activation end' THEN sequence_number ELSE 0 END) AS max_sequence_number
		FROM ${env:TMP_db}.si_composite_score_core_events_daily
		WHERE date_level = '${hiveconf:grain}'
		GROUP BY
			denver_date,
			application_group,
			application_version,
			user_package,
			mso,
			acct_id,
			visit_id,
			date_level
	) AS t1
	LEFT JOIN(
		SELECT
			received_timestamp,
			application_group,
			application_version,
			visit_id,
			sequence_number,
			api_response_time
		FROM ${env:TMP_db}.si_composite_score_core_events_daily
		WHERE api_response_time IS NOT NULL
			AND date_level = '${hiveconf:grain}'
	) t2 ON t1.application_group = t2.application_group
			AND t1.application_version = t2.application_version
			AND t1.visit_id = t2.visit_id
			-- AND t2.received_timestamp BETWEEN t1.transaction_start_timestamp AND t1.transaction_end_timestamp
			AND t2.sequence_number BETWEEN t1.min_sequence_number AND t1.max_sequence_number
	WHERE has_activation_start AND has_activation_end
	GROUP BY
		t1.denver_date,
		t1.mso,
		t1.acct_id,
		t1.visit_id,
		t1.application_group,
		t1.application_version,
		t1.user_package,
		t1.date_level
) v
GROUP BY
	denver_date,
	mso,
	acct_id,
	visit_id,
	application_group,
	application_version,
	user_package,
	date_level
;



SELECT "\n\nFor 1b: ${env:TMP_db}.si_composite_score_activation_daily\n\n";
--Convert real numbers to score buckets
DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_activation_daily PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_composite_score_activation_daily AS
SELECT
	denver_date,
	mso,
	acct_id,
	visit_id,
	application_group,
	application_version,
	user_package,
	date_level,
	IF(activation_success >= 1, TRUE, FALSE) AS has_activation_success,
	IF(activation_failure > ${hiveconf:activation_allowed_failures}, TRUE, FALSE) AS has_more_than_allowed_failured_activations,
	activation_success,
	activation_failure,
	CASE
		WHEN avg_transaction_duration <= ${hiveconf:activation_average_transaction_duration_sec}[0] THEN 8
		WHEN ${hiveconf:activation_average_transaction_duration_sec}[0] < avg_transaction_duration AND avg_transaction_duration <= ${hiveconf:activation_average_transaction_duration_sec}[1] THEN 5
		WHEN ${hiveconf:activation_average_transaction_duration_sec}[1] < avg_transaction_duration AND avg_transaction_duration <= ${hiveconf:activation_average_transaction_duration_sec}[2] THEN 3
		WHEN ${hiveconf:activation_average_transaction_duration_sec}[2] < avg_transaction_duration AND avg_transaction_duration <= ${hiveconf:activation_average_transaction_duration_sec}[3] THEN 2
		WHEN ${hiveconf:activation_average_transaction_duration_sec}[3] < avg_transaction_duration THEN 1
	END AS transaction_duration,
	CASE
		WHEN avg_api_response_time <= ${hiveconf:activation_average_API_response_time_ms}[0] THEN 8
		WHEN ${hiveconf:activation_average_API_response_time_ms}[0] < avg_api_response_time AND avg_api_response_time <= ${hiveconf:activation_average_API_response_time_ms}[1] THEN 5
		WHEN ${hiveconf:activation_average_API_response_time_ms}[1] < avg_api_response_time AND avg_api_response_time <= ${hiveconf:activation_average_API_response_time_ms}[2] THEN 3
		WHEN ${hiveconf:activation_average_API_response_time_ms}[2] < avg_api_response_time AND avg_api_response_time <= ${hiveconf:activation_average_API_response_time_ms}[3] THEN 2
		WHEN ${hiveconf:activation_average_API_response_time_ms}[3] < avg_api_response_time THEN 1
	END AS api_response_time,
	avg_transaction_duration,
	avg_api_response_time
FROM ${env:TMP_db}.si_composite_score_activation_events_daily
;


SELECT "\n\nFor 1c: ${env:TMP_db}.si_composite_score_activation_visit_daily\n\n";
DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_activation_visit_daily PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_composite_score_activation_visit_daily AS
SELECT
	denver_date,
	mso,
	acct_id,
	visit_id,
	application_group,
	application_version,
	user_package,
	date_level,
	IF( has_activation_success AND NOT has_more_than_allowed_failured_activations,
		( ${hiveconf:activation_weights_duration_then_API_response_time}[0] * transaction_duration +
			${hiveconf:activation_weights_duration_then_API_response_time}[1] * api_response_time
		)/8,
		IF(transaction_duration IS NULL OR api_response_time IS NULL, NULL, 0)
	) AS activation_performance,
	IF(has_activation_success AND NOT has_more_than_allowed_failured_activations,
		 ${hiveconf:activation_weights_duration_then_API_response_time}[0] * transaction_duration/8,0) AS transaction_duration_filtered,
	IF(has_activation_success AND NOT has_more_than_allowed_failured_activations,
		 ${hiveconf:activation_weights_duration_then_API_response_time}[1] * api_response_time/8,0) AS api_response_time_filtered,
	${hiveconf:activation_weights_duration_then_API_response_time}[0] * transaction_duration/8 AS transaction_duration_bucket,
	${hiveconf:activation_weights_duration_then_API_response_time}[1] * api_response_time/8 AS api_response_time_bucket,
	IF(has_activation_success AND NOT has_more_than_allowed_failured_activations,1,0) AS activation_both_true_yes,
	IF(has_activation_success AND NOT has_more_than_allowed_failured_activations,0,1) AS activation_both_true_no,
	IF(has_activation_success,1,0) AS activation_success_true_yes,
	IF(has_activation_success,0,1) AS activation_success_true_no,
	IF(NOT has_more_than_allowed_failured_activations,1,0) AS activation_failure_true_not,
	IF(NOT has_more_than_allowed_failured_activations,0,1) AS activation_failure_true_yes,
	avg_transaction_duration,
	avg_api_response_time
FROM ${env:TMP_db}.si_composite_score_activation_daily
;



SELECT "\n\nFor 1d: si_composite_score_kpi_dist_daily\n\n";
INSERT OVERWRITE TABLE si_composite_score_kpi_dist_daily PARTITION (denver_date)
SELECT
	CAST(CONCAT(
		CASE WHEN grouping(user_package) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(application_version) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(application_group) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(activation_performance) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(denver_date) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(date_level) = 0 THEN 1 ELSE 0 END
    ) AS INT) AS grouping_id,
	application_group,
	application_version,
	user_package,
	'self_install_activation_dist' AS metric_name,
	COUNT(1) AS number_rows,
	COUNT(visit_id) AS number_visits,
	CAST(COUNT(DISTINCT visit_id) AS DOUBLE)/COUNT(1) pct_uniq_visits,
	CAST(COUNT(transaction_duration_bucket) AS DOUBLE)/COUNT(1) AS pct_duration,
	CAST(COUNT(api_response_time_bucket) AS DOUBLE)/COUNT(1) AS pct_api_response_time,
	activation_performance AS quality_score,
	AVG(transaction_duration_bucket) AS transaction_duration_bucket,
	AVG(api_response_time_bucket) AS api_response_time_bucket,
	SUM(activation_both_true_yes) as all_true_yes,
	SUM(activation_success_true_yes) as success_true_yes,
	SUM(activation_failure_true_not) as failure_true_not,
	date_level,
	AVG(avg_transaction_duration) AS avg_transaction_duration,
	AVG(avg_api_response_time) AS avg_api_response_time,
	denver_date
FROM ${env:TMP_db}.si_composite_score_activation_visit_daily
GROUP BY
	date_level,
	denver_date,
	activation_performance,
	application_group,
	application_version,
	user_package
GROUPING SETS(
	(date_level, denver_date, activation_performance),
	(date_level, denver_date, application_group, activation_performance),
	(date_level, denver_date, application_group, application_version, activation_performance),
	(date_level, denver_date, user_package),
	(date_level, denver_date, application_group, user_package)
)
;

SELECT "\n\nFor 1e: si_composite_score_kpi_agg_daily\n\n";

--calculate final average score using the formula
INSERT OVERWRITE TABLE si_composite_score_kpi_agg_daily PARTITION (denver_date)
SELECT
	CAST(CONCAT(
		CASE WHEN grouping(user_package) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(application_version) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(application_group) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(denver_date) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(date_level) = 0 THEN 1 ELSE 0 END
    ) AS INT) AS grouping_id,
	'total' AS timeframe,
	LOWER(application_group) AS application_group,
	application_version,
	user_package,
	'self_install_activation_composite' AS metric_name,
	AVG(activation_performance) AS metric_value,
	AVG(transaction_duration_filtered)/AVG(activation_performance) AS transaction_duration_filtered_ratio,
	AVG(api_response_time_filtered)/AVG(activation_performance) AS api_response_time_filtered_ratio,
	AVG(transaction_duration_bucket) AS transaction_duration_bucket,
	AVG(api_response_time_bucket) AS api_response_time_bucket,
	AVG(activation_performance)/(AVG(transaction_duration_bucket)+AVG(api_response_time_bucket)) as activation_both_derived,
	SUM(CAST(activation_both_true_yes AS DOUBLE))/SUM(CAST(activation_both_true_yes AS DOUBLE) + activation_both_true_no) as activation_both,
	SUM(CAST(activation_success_true_yes AS DOUBLE))/SUM(CAST(activation_success_true_yes + activation_success_true_no AS DOUBLE)) as activation_success,
	SUM(CAST(activation_failure_true_not AS DOUBLE))/SUM(CAST(activation_failure_true_not + activation_failure_true_yes AS DOUBLE)) as activation_failure_not,
	COUNT(DISTINCT visit_id) AS number_uniq_visit,
	date_level,
	AVG(avg_transaction_duration) AS avg_transaction_duration,
	AVG(avg_api_response_time) AS avg_api_response_time,
	denver_date
FROM (
	SELECT *
		FROM ${env:TMP_db}.si_composite_score_activation_visit_daily
	 WHERE transaction_duration_bucket IS NOT NULL
		 AND api_response_time_bucket IS NOT NULL
) AS t1
GROUP BY
	date_level,
	denver_date,
	application_group,
	application_version,
	user_package
GROUPING SETS(
	(date_level, denver_date),                                            --11
	(date_level, denver_date, application_group),                         --111
	(date_level, denver_date, application_group, application_version),    --1111
	(date_level, denver_date, user_package),                              --10011
	(date_level, denver_date, application_group, user_package)            --10111
)
;

--calculate Composite score using the formula added on 11-16-2020
INSERT OVERWRITE TABLE si_composite_score_kpi_agg_visit_daily PARTITION (denver_date)
SELECT
	CAST(CONCAT(
		CASE WHEN grouping(user_package) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(application_version) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(visit_id) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(acct_id) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(mso) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(application_group) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(denver_date) = 0 THEN 1 ELSE 0 END,
		CASE WHEN grouping(date_level) = 0 THEN 1 ELSE 0 END
    ) AS INT) AS grouping_id,
	'total' AS timeframe,
	LOWER(application_group) AS application_group,
	mso,
	acct_id,
	visit_id,
	application_version,
	user_package,
	'self_install_activation_composite' AS metric_name,
	AVG(activation_performance) AS metric_value,
	AVG(transaction_duration_filtered)/AVG(activation_performance) AS transaction_duration_filtered_ratio,
	AVG(api_response_time_filtered)/AVG(activation_performance) AS api_response_time_filtered_ratio,
	AVG(transaction_duration_bucket) AS transaction_duration_bucket,
	AVG(api_response_time_bucket) AS api_response_time_bucket,
	AVG(activation_performance)/(AVG(transaction_duration_bucket)+AVG(api_response_time_bucket)) as activation_both_derived,
	SUM(CAST(activation_both_true_yes AS DOUBLE))/SUM(CAST(activation_both_true_yes AS DOUBLE) + activation_both_true_no) as activation_both,
	SUM(CAST(activation_success_true_yes AS DOUBLE))/SUM(CAST(activation_success_true_yes + activation_success_true_no AS DOUBLE)) as activation_success,
	SUM(CAST(activation_failure_true_not AS DOUBLE))/SUM(CAST(activation_failure_true_not + activation_failure_true_yes AS DOUBLE)) as activation_failure_not,
	1 AS number_uniq_visit,
	date_level,
	AVG(avg_transaction_duration) AS avg_transaction_duration,
	AVG(avg_api_response_time) AS avg_api_response_time,
	denver_date
FROM (
	SELECT *
		FROM ${env:TMP_db}.si_composite_score_activation_visit_daily
	 WHERE transaction_duration_bucket IS NOT NULL
		 AND api_response_time_bucket IS NOT NULL
) AS t1
GROUP BY
	date_level,
	denver_date,
	application_group,
	mso,
	acct_id,
	visit_id,
	application_version,
	user_package
GROUPING SETS(
	(date_level, denver_date, application_group, mso, acct_id, visit_id, application_version, user_package),  --11111111
	(date_level, denver_date)                                                                                 --11
)
;


DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_core_events_daily PURGE;
----DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_activation_events_daily PURGE; --this table is used in Agg query
DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_activation_daily PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.si_composite_score_activation_visit_daily PURGE;
