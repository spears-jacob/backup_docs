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
set hive.exec.max.dynamic.partitions=3000;
set hive.exec.max.dynamic.partitions.pernode=3000;

--Secure any PII data in si_core_quantum_events table
SELECT "\n\nFor 1: si_core_quantum_events\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.si_core_quantum_events_tmp;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.si_core_quantum_events_tmp AS
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
		IF(state__view__current_page__page_title = 'equipmentSelfInstall', 'equipmentSelfInstall', NULL) AS state__view__current_page__page_title,
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
FROM si_core_quantum_events
WHERE
    partition_date_hour_utc >= '${hiveconf:SECURE_START_DATE_HOUR_UTC}'
    AND partition_date_hour_utc < '${hiveconf:SECURE_END_DATE_HOUR_UTC}'
;

INSERT OVERWRITE TABLE si_core_quantum_events PARTITION (partition_date_utc, partition_date_hour_utc, visit__application_details__application_name)
SELECT *
FROM ${env:TMP_db}.si_core_quantum_events_tmp
;

DROP TABLE IF EXISTS ${env:TMP_db}.si_core_quantum_events_tmp;