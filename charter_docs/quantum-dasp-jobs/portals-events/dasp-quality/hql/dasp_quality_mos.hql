USE ${env:DASP_db};

SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.exec.parallel=true;
SET mapred.output.compress=true;
SET mapred.compress.map.output=true;
SET hive.exec.compress.intermediate=true;
SET hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.optimize.sort.dynamic.partition=false;
SET mapreduce.input.fileinputformat.split.maxsize=32000000;
SET hive.exec.reducers.bytes.per.reducer=128000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET orc.force.positional.evolution=true;
-- default logic is ignored and Tez tries to group splits into the specified number of groups. Change that parameter carefully.
--set tez.grouping.split-count=1200;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/epochtotimestamp-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_timestamp AS 'Epoch_Timestamp';

set hive.vectorized.execution.enabled = false;

SELECT "\n\nFor : dasp_core_events_oneday\n\n";
--create common temporary TABLES
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER} PURGE;
CREATE TABLE if not exists ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER} as
SELECT
  received__timestamp,
  cast(epoch_converter(received__timestamp,'America/Denver') as date) AS denver_date,
  visit__application_details__application_name AS application_name,
  visit__application_details__app_version AS application_version,
  visit__account__details__mso AS mso,
  --Update when NULL MSOs are resolved.
  --MAX(
  --CASE
  --  WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'L-CHTR'
  --  WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'L-TWC'
  --  WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'L-BHN'
  --  WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE') OR visit__account__details__mso IS NULL ) THEN 'MSO-MISSING'
  --  ELSE visit__account__details__mso
  --END) OVER (PARTITION BY visit__visit_id) visit_level_mso,
  MAX(visit__account__details__mso) OVER (PARTITION BY visit__visit_id) visit_level_mso,
  MAX(visit__account__enc_account_number) OVER (PARTITION BY visit__visit_id) visit_level_account_number,
  MAX(visit__account__enc_account_billing_id) OVER (PARTITION BY visit__visit_id) AS visit_level_billing_id,
  MAX(visit__account__enc_account_billing_division) OVER (PARTITION BY visit__visit_id) AS visit_level_billing_division,
  visit__account__enc_account_number as portals_unique_acct_key,
  visit__visit_id AS visit_id,
  visit__device__uuid,
  visit__device__browser__name AS browser_name,
  LOWER(visit__device__device_type) AS device_type,
  visit__connection__connection_type AS connection_type,
  visit__connection__network_status AS network_status,
  visit__location__enc_city AS location_city,
  visit__location__state AS location_state,
  visit__location__enc_zip_code AS location_zipcode,
  visit__device__model AS device_model,
  message__name,
  message__timestamp,
  message__sequence_number,
  message__feature__transaction_id AS transaction_id,
  message__feature__feature_name,
  message__feature__feature_step_name,
  message__feature__feature_step_changed,
  message__event_case_id,
  operation__success,
  operation__operation_type as operation_type,
  operation__billing__payment_method as payment_method,
  state__view__current_page__page_name AS page_name,
  state__view__current_page__page_sequence_number AS page_sequence_number,
  state__view__previous_page__page_name as previous_page_name,
  state__view__previous_page__page_sequence_number as previous_page_sequence_number,
  state__view__current_page__elements__standardized_name  as elements_standardized_name,
  state__view__current_Page__biller_Type as biller_type,
  IF(message__name = 'loginStop',visit__login__login_duration_ms,NULL)/1000 AS login_duration_sec,
  IF(message__name = 'pageViewPerformance', state__view__current_page__performance_timing__dom_content_loaded_event_end - state__view__current_page__performance_timing__navigation_start, NULL)/1000 AS cold_page_load_sec,
  IF(message__name = 'pageView', state__view__current_page__render_details__fully_rendered_ms, NULL)/1000 AS hot_page_load_sec,
  CASE WHEN
        (visit__application_details__application_name = 'MySpectrum' AND message__name = 'pageView' AND state__view__current_page__page_name IN ('paymentSuccess', 'paymentWithAutoPaySuccess', 'paySuccess', 'paySuccessAutoPay') AND state__view__previous_page__page_name IN ('makePayment', 'reviewCompletePayment'))
        OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStop' AND message__feature__feature_name IN ('oneTimeBillPayFlow', 'oneTimePayment') AND operation__success = TRUE AND ((message__feature__feature_step_changed = FALSE AND message__event_case_id <> 'SPECNET_selectAction_billPayStop_otp_exitBillPayFlow') OR (message__feature__feature_step_name IN('oneTimePaymentAppSuccess','oneTimePaymentAutoPayAppSuccess', 'paymentWithAutoPaySuccess', 'paymentSuccess') AND message__feature__feature_step_changed = TRUE)))
        OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStop' AND ((message__feature__feature_step_name IN( 'otpSuccess', 'otpSuccessAutoPay' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name IN ('oneTimeBillPayFlow', 'oneTimePayment') AND operation__success = TRUE AND ((message__feature__feature_step_changed = FALSE AND message__event_case_id <> 'SPECNET_selectAction_billPayStop_otp_exitBillPayFlow') OR (message__feature__feature_step_name IN('oneTimePaymentAppSuccess','oneTimePaymentAutoPayAppSuccess', 'paymentWithAutoPaySuccess', 'paymentSuccess') AND message__feature__feature_step_changed = TRUE)))))
        OR (visit__application_details__application_name = 'SpecMobile' AND message__event_case_id = 'specMobile_feature_oneTimePaymentComplete')
      THEN 'success_otp'
  END as success_otp,
  CASE WHEN
        (visit__application_details__application_name = 'MySpectrum' AND message__name = 'pageView' AND state__view__current_page__page_name IN ('paymentFailure', 'paymentWithAutoPayFailure', 'payUnsuccess','payUnsuccessAutoPay'))
        OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStop' AND message__feature__feature_step_changed = TRUE AND operation__success = FALSE AND ((message__feature__feature_name = 'oneTimePayment' AND message__feature__feature_step_name = 'paymentFailure') OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND message__feature__feature_step_name = 'oneTimePaymentError')))
        OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStop' AND message__feature__feature_step_changed = TRUE AND operation__success = FALSE AND ((message__feature__feature_name = 'oneTimePayment' AND message__feature__feature_step_name = 'paymentFailure') OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND message__feature__feature_step_name = 'oneTimePaymentError')))
        OR (visit__application_details__application_name = 'SpecMobile' AND message__event_case_id = 'specMobile_feature_oneTimePaymentFailed')
      THEN 'fail_otp'
  END as fail_otp,
  CASE WHEN
        (visit__application_details__application_name = 'MySpectrum' AND state__view__previous_page__page_name = 'autoPayManage' AND state__view__current_page__page_name IN ('autoPayCancelSuccess','autoPayCancellationSuccess'))
        OR (visit__application_details__application_name = 'MySpectrum' AND message__name = 'pageView' AND state__view__current_page__page_name IN( 'autoPayEnrollmentSuccess', 'paymentWithAutoPaySuccess', 'autoPaySuccess', 'paySuccessAutoPay', 'enrollmentSuccess') AND state__view__previous_page__page_name = 'autoPayEnroll')
        OR (visit__application_details__application_name = 'SpecNet' AND (message__event_case_id IN('SPECNET_billPay_billPayStop_autopayEnrolled_triggeredByApplicationSuccess', 'SPECNET_billPay_billPayStop_autopayEnrollmentWithPayment_triggeredByApplicationSuccess')))
        OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStop' AND operation__success AND message__feature__feature_name = 'autoPayEnrollment' AND message__feature__feature_step_name IN ('enrollmentSuccess','enrollmentWithPaymentSuccess'))
        OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStop' AND operation__success AND message__feature__feature_name = 'autoPayManagement' AND message__feature__feature_step_name IN ('cancellationSuccess','saveChangesSuccess'))
        OR (visit__application_details__application_name = 'SMB' AND ((message__name = 'featureStop' AND message__feature__feature_name IN ('autoPayEnroll', 'autoPayEnrollment') AND operation__success = TRUE) OR ((message__event_case_id IN('SPECNET_billPay_billPayStop_autopayEnrolled_triggeredByApplicationSuccess', 'SPECNET_billPay_billPayStop_autopayEnrollmentWithPayment_triggeredByApplicationSuccess')))))
        OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStop' AND operation__success AND message__feature__feature_name = 'autoPayEnrollment' AND message__feature__feature_step_name IN ('enrollmentSuccess','enrollmentWithPaymentSuccess'))
        OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStop' AND operation__success AND message__feature__feature_name = 'autoPayManagement' AND message__feature__feature_step_name IN ('cancellationSuccess','saveChangesSuccess'))
        OR (message__name = 'featureStop' AND message__feature__feature_name = 'autoPayEnrollment' AND visit__application_details__application_name IN ('SpecNet','SMB') AND message__event_case_id IN ('SPECNET_billPay_billPayStop_autopayEnrollmentWithPayment_triggeredByApplicationSuccess','SPECNET_billPay_billPayStop_autopayEnrolled_triggeredByApplicationSuccess'))
        OR (message__name = 'featureStop' AND message__feature__feature_name = 'autoPayManagement' AND visit__application_details__application_name IN ('SpecNet','SMB') AND message__event_case_id IN ('SPECNET_billPay_billPayStop_autopayChangesConfirmation_triggeredByApplicationSuccess','SPECNET_billPay_billPayStop_autopayEnrollmentCancelled_triggeredByApplicationSuccess'))
      THEN 'success_autopay'
  END as success_autopay,
  CASE WHEN
        (visit__application_details__application_name = 'MySpectrum' AND state__view__previous_page__page_name = 'autoPayManage' AND state__view__current_page__page_name IN ('autoPayCancelFail','autoPayCancellationFailure'))
        OR (visit__application_details__application_name = 'MySpectrum' AND message__name = 'pageView' AND (state__view__current_page__page_name = 'autoPayFail' OR (state__view__current_page__page_name IN ('enrollmentFailure','paymentWithAutoPayFailure','payUnsuccessAutoPay') AND state__view__previous_page__page_name = 'autoPayEnroll')))
        OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStop' AND message__feature__feature_name IN ('autoPayEnrollment', 'manageAutopay') AND operation__success = FALSE AND message__event_case_id NOT RLIKE '.*Cancelled.*')
        OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStop' AND operation__success AND message__feature__feature_name = 'autoPayEnrollment' AND message__feature__feature_step_name IN ('enrollmentFailure','enrollmentWithPaymentFailure'))
        OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStop' AND operation__success AND message__feature__feature_name = 'autoPayManagement' AND message__feature__feature_step_name IN ('cancellationFailure','saveChangesFailure'))
        OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStop' AND ((message__feature__feature_step_name IN ('apEnrollFailure', 'apEnrollFailureWithPayment') AND message__feature__feature_step_changed = true) OR (message__feature__feature_name IN ('autoPayEnrollment', 'manageAutopay') AND operation__success = FALSE AND message__event_case_id NOT RLIKE '.*Cancelled.*')))
        OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStop' AND NOT operation__success AND message__feature__feature_name = 'autoPayEnrollment' AND message__feature__feature_step_name IN ('enrollmentFailure','enrollmentWithPaymentFailure'))
        OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStop' AND NOT operation__success AND message__feature__feature_name = 'autoPayManagement' AND message__feature__feature_step_name IN ('cancellationFailure','saveChangesFailure'))
        OR (message__name = 'featureStop' AND message__feature__feature_name = 'autoPayEnrollment' AND visit__application_details__application_name IN ('SpecNet','SMB') AND message__event_case_id IN ('SPECNET_billPay_billPayStop_autopayEnrollmentErrorWithPayment_triggeredByApplicationError','SPECNET_billPay_billPayStop_autopayEnrollment_triggeredByApplicationError'))
        OR (message__name = 'featureStop' AND message__feature__feature_name = 'autoPayManagement' AND visit__application_details__application_name IN ('SpecNet','SMB') AND message__event_case_id IN ('SPECNET_billPay_billPayStop_autopayEnrollmentCancelled_triggeredByApplicationError','SPECNET_billPay_billPayStop_autopayPreferencesEdit_triggeredByApplicationError'))
      THEN 'failure_autopay'
  END as failure_autopay,
  CASE WHEN
        (visit__application_details__application_name = 'MySpectrum' AND message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'makePaymentButton')
        OR message__event_case_id = 'mySpectrum_selectAction_quickPay'
        OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStart' AND message__feature__feature_name IN  ('oneTimeBillPayFlow', 'oneTimePayment'))
        OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStart' AND message__feature__feature_name IN  ('oneTimeBillPayFlow', 'oneTimePayment'))
        OR (visit__application_details__application_name = 'SpecMobile' AND message__event_case_id = 'specMobile_feature_oneTimePaymentStart')
      THEN 'start_true_otp'
  END as start_true_otp,
  CASE WHEN
        (visit__application_details__application_name = 'MySpectrum' AND message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'autoPayManage')
        OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStart' AND message__feature__feature_step_name IN ('viewAutopayEnrollment', 'autoPayEnrollStart') AND message__feature__feature_step_changed = TRUE)
        OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStart' AND ((message__feature__feature_name IN ('autoPayEnroll', 'autoPayEnrollment' ) OR (message__feature__feature_step_name IN ('viewAutopayEnrollment', 'autoPayEnrollStart') AND message__feature__feature_step_changed = TRUE))))
        OR (visit__application_details__application_name IN ('SpecNet','SMB') AND message__name = 'featureStart' AND message__feature__feature_name IN ('autoPayManagement', 'autoPayEnrollment') AND message__feature__feature_step_name IN ('autoPayEnrollStart','autoPayManageStart','viewManageAutoPay') AND message__feature__feature_step_changed)
        OR (visit__application_details__application_name IN ('SpecNet','SMB') AND message__name = 'featureStart' AND message__feature__feature_name IN ('autoPayManagement', 'autoPayEnrollment') AND message__event_case_id IN ('SPECNET_billPay_billPayStart_autoPayEnroll', 'SPECNET_billPay_billPayStart_manageAutoPay'))
      THEN 'start_true_autopay'
  END as start_true_autopay,
  CASE WHEN
        (visit__application_details__application_name in ('SpecNet','SMB','MySpectrum')
        AND (message__feature__transaction_id IS NOT NULL AND message__feature__feature_name IN ('oneTimeBillPayFlow', 'oneTimePayment')))
       THEN 'start_false_otp'
  END as start_false_otp,
  CASE WHEN
        (visit__application_details__application_name in ('SpecNet','SMB','MySpectrum')
        AND (message__feature__transaction_id IS NOT NULL AND message__feature__feature_name IN ('autoPayEnroll', 'autoPayEnrollment','autoPayManagement', 'autoPayManage')))
      THEN 'start_false_autopay'
  END as start_false_autopay
FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
WHERE partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}'
  AND partition_date_hour_utc <  '${hiveconf:END_DATE_TZ}'
  AND visit__application_details__application_name IN ('SpecNet','SpecMobile','SMB','MySpectrum')
;

SELECT "\n\nFor : dasp_core_pageload_time_oneday\n\n";
drop table if exists ${env:TMP_db}.dasp_core_pageload_time_oneday_${env:CLUSTER} PURGE;
CREATE TABLE if not exists ${env:TMP_db}.dasp_core_pageload_time_oneday_${env:CLUSTER} as
SELECT
    application_name,
    application_version,
    mso,
    visit_id,
    page_name,
    message__timestamp,
    IF(page_sequence_number IS NULL, -1, page_sequence_number) AS page_sequence_number,
    IF(previous_page_sequence_number IS NULL, -1, previous_page_sequence_number) AS prev_page_sequence_number,
    cold_page_load_sec,
    hot_page_load_sec
FROM ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER}
WHERE message__name IN ('pageView','pageViewPerformance');


SELECT "\n\nFor 1: Application Entry\n\n";
--Application Entry Metric (SpecNet,SpecMobile,SMB,MySpectrum)
SET login_failure_buckets = array(1,2,3,4);
SET login_duration_sec_buckets = array(4,6,10,15);
SET login_page_load_sec_buckets = array(2,4,10,20);
SET login_weights_failures_then_duration_then_page_load = array(0.6,0.2,0.2);

SELECT "\n\nFor 1a: dasp_application_entry_events\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.dasp_application_entry_events_${env:CLUSTER} PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.dasp_application_entry_events_${env:CLUSTER} AS
SELECT
  cast(epoch_converter(received__timestamp,'America/Denver') as date) AS denver_date,
  t1.visit_id,
  t1.application_name,
  t1.application_version,
  t1.account_number,
  t1.billing_id,
  t1.billing_division,
  t1.mso,
  CASE WHEN (CAST(grouping__id AS INT) & 1) = 0 THEN t1.operation_type ELSE 'All Operation Types' END AS operation_type,
  CAST(grouping__id AS INT) AS visit_level_grouping_id,
  CAST(
  CONCAT(
    CASE WHEN grouping(t1.operation_type) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.mso) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.billing_division) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.billing_id) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.account_number) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.application_version) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.application_name) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.visit_id) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(cast(epoch_converter(received__timestamp,'America/Denver') as date)) = 0 THEN 1 ELSE 0 END
  ) AS BIGINT) AS custom_visit_level_grouping_id,
  COUNT(DISTINCT IF(t1.message__name = 'loginStart',message__sequence_number,NULL)) AS login_starts,
  COUNT(DISTINCT IF(t1.message__name = 'loginStop' AND operation__success = FALSE, message__sequence_number,NULL)) AS failed_logins,
  COUNT(DISTINCT IF(t1.message__name = 'loginStop' AND operation__success = TRUE, message__sequence_number,NULL)) AS successful_logins,
  AVG(DISTINCT login_duration_sec) AS login_duration_sec,
  (
    NVL(SUM(
      DISTINCT IF((t1.page_sequence_number = t2.page_sequence_number) OR (look_at_next_page AND t1.page_sequence_number = t2.prev_page_sequence_number), cold_page_load_sec, NULL)
    ),0) +
    NVL(SUM(
      DISTINCT IF((t1.page_sequence_number = t2.page_sequence_number) OR (look_at_next_page AND t1.page_sequence_number = t2.prev_page_sequence_number), hot_page_load_sec, NULL)
    ),0)
  )/(
    COUNT(DISTINCT IF(t1.page_sequence_number = t2.page_sequence_number,t2.page_sequence_number,NULL))
    + COUNT(DISTINCT IF(look_at_next_page AND t1.page_sequence_number = t2.prev_page_sequence_number,t2.prev_page_sequence_number,NULL))
  ) AS page_load_time
FROM(
  SELECT
    received__timestamp,
    application_name,
    application_version,
    visit_level_account_number account_number,
    visit_level_billing_id billing_id,
    visit_level_billing_division billing_division,
    visit_level_mso mso,
    page_name,
    IF(page_sequence_number IS NULL, -1, page_sequence_number) AS page_sequence_number,
    IF(
      message__name = 'loginStop' AND operation__success = TRUE
      AND (page_name IS NULL
      OR LOWER(page_name) RLIKE 'unauth'
      OR LOWER(page_name) RLIKE 'signin'
      OR LOWER(page_name) RLIKE 'login'
      OR LOWER(page_name) RLIKE 'reauth')
      ,TRUE
      ,FALSE
    ) AS look_at_next_page,
    visit_id,
    message__name,
    operation__success,
    operation_type,
    message__sequence_number,
    login_duration_sec
  FROM ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER}
  WHERE message__name IN ('loginStop','loginStart')
) AS t1
LEFT JOIN ${env:TMP_db}.dasp_core_pageload_time_oneday_${env:CLUSTER} AS t2
   ON t1.visit_id = t2.visit_id
  AND t1.application_version = t2.application_version
  AND t1.application_name = t2.application_name
WHERE ((t1.page_sequence_number = t2.page_sequence_number)
    OR (t1.page_sequence_number = t2.prev_page_sequence_number))
GROUP BY
  cast(epoch_converter(received__timestamp,'America/Denver') as date),
  t1.visit_id,
  t1.application_name,
  t1.application_version,
  t1.account_number,
  t1.billing_id,
  t1.billing_division,
  t1.mso,
  t1.operation_type
GROUPING SETS
(
  ( cast(epoch_converter(received__timestamp,'America/Denver') as date),
    t1.visit_id,
    t1.application_name,
    t1.application_version,
    t1.account_number,
    t1.billing_id,
    t1.billing_division,
    t1.mso),
  ( cast(epoch_converter(received__timestamp,'America/Denver') as date),
    t1.visit_id,
    t1.application_name,
    t1.application_version,
    t1.account_number,
    t1.billing_id,
    t1.billing_division,
    t1.mso,
    t1.operation_type)
)
;

SELECT "\n\nFor 1b: dasp_application_entry\n\n";
--Convert real numbers to score buckets
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_application_entry_${env:CLUSTER} PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.dasp_application_entry_${env:CLUSTER} AS
SELECT
  denver_date,
  visit_id,
  application_version,
  application_name,
  account_number,
  billing_id,
  billing_division,
  mso,
  operation_type,
  custom_visit_level_grouping_id,
  IF(successful_logins >=1, TRUE, FALSE) AS has_successful_login,
  successful_logins,
  failed_logins,
  login_duration_sec,
  page_load_time,
  CASE
    WHEN failed_logins <= ${hiveconf:login_failure_buckets}[0] THEN 8
    WHEN ${hiveconf:login_failure_buckets}[0] < failed_logins AND failed_logins <= ${hiveconf:login_failure_buckets}[1] THEN 5
    WHEN ${hiveconf:login_failure_buckets}[1] < failed_logins AND failed_logins <= ${hiveconf:login_failure_buckets}[2] THEN 3
    WHEN ${hiveconf:login_failure_buckets}[2] < failed_logins AND failed_logins <= ${hiveconf:login_failure_buckets}[3] THEN 2
    WHEN ${hiveconf:login_failure_buckets}[3] < failed_logins THEN 1
  END AS login_failure_bucket,
  CASE
    WHEN login_duration_sec <= ${hiveconf:login_duration_sec_buckets}[0] THEN 8
    WHEN ${hiveconf:login_duration_sec_buckets}[0] < login_duration_sec AND login_duration_sec <= ${hiveconf:login_duration_sec_buckets}[1] THEN 5
    WHEN ${hiveconf:login_duration_sec_buckets}[1] < login_duration_sec AND login_duration_sec <= ${hiveconf:login_duration_sec_buckets}[2] THEN 3
    WHEN ${hiveconf:login_duration_sec_buckets}[2] < login_duration_sec AND login_duration_sec <= ${hiveconf:login_duration_sec_buckets}[3] THEN 2
    WHEN ${hiveconf:login_duration_sec_buckets}[3] < login_duration_sec THEN 1
  END AS login_duration_bucket,
  CASE
    WHEN page_load_time <= ${hiveconf:login_page_load_sec_buckets}[0] THEN 8
    WHEN ${hiveconf:login_page_load_sec_buckets}[0] < page_load_time AND page_load_time <= ${hiveconf:login_page_load_sec_buckets}[1] THEN 5
    WHEN ${hiveconf:login_page_load_sec_buckets}[1] < page_load_time AND page_load_time <= ${hiveconf:login_page_load_sec_buckets}[2] THEN 3
    WHEN ${hiveconf:login_page_load_sec_buckets}[2] < page_load_time AND page_load_time <= ${hiveconf:login_page_load_sec_buckets}[3] THEN 2
    WHEN ${hiveconf:login_page_load_sec_buckets}[3] < page_load_time THEN 1
  END AS page_load_time_bucket
FROM ${env:TMP_db}.dasp_application_entry_events_${env:CLUSTER}
;

SELECT "\n\nFor 1c: asp_quality_kpi_dist_sum\n\n";
INSERT OVERWRITE TABLE asp_quality_kpi_dist PARTITION(denver_date)
select  grouping__id,
        LOWER(application_name) AS application_name,
        application_version,
        'portals_application_entry_sum' as metric_name,
        count(1) number_rows,
        count(visit_id) number_visits,
        count(distinct visit_id)/count(1) pct_uniq_visits,
        count(login_failure_bucket)/count(1) pct_failure,
        count(login_duration_bucket)/count(1) pct_duration,
        count(page_load_time_bucket)/COUNT(1) pct_time,
        0 as quality_score,
        0 as failure_bucket,
        0 as duration_bucket,
        0 as time_bucket,
        0 as all_true_yes,
        0 as success_true_yes,
        0 as failure_true_not,
        denver_date
  from ${env:TMP_db}.dasp_application_entry_${env:CLUSTER}
  where custom_visit_level_grouping_id = 11111111
group by denver_date,
         application_name,
         application_version
         grouping sets(
           (denver_date),
           (denver_date, application_name),
           (denver_date, application_name, application_version)
         )
;

SELECT "\n\nFor 1d: dasp_application_entry_visit\n\n";
drop table if EXISTS ${env:TMP_db}.dasp_application_entry_visit_${env:CLUSTER} PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.dasp_application_entry_visit_${env:CLUSTER} AS
SELECT
    application_name,
    application_version,
    account_number,
    billing_id,
    billing_division,
    mso,
    operation_type,
    custom_visit_level_grouping_id,
    denver_date,
    visit_id,
    has_successful_login,
    successful_logins,
    failed_logins,
    login_duration_sec,
    page_load_time,
    login_failure_bucket AS login_failure_bucket_raw,
    login_duration_bucket AS login_duration_bucket_raw,
    page_load_time_bucket AS page_load_time_bucket_raw,
    IF( has_successful_login,
      ( ${hiveconf:login_weights_failures_then_duration_then_page_load}[0] * login_failure_bucket +
        ${hiveconf:login_weights_failures_then_duration_then_page_load}[1] * login_duration_bucket +
        ${hiveconf:login_weights_failures_then_duration_then_page_load}[2] * page_load_time_bucket
      )/8,
      0
    ) AS login_performance,
    IF(has_successful_login,
       ${hiveconf:login_weights_failures_then_duration_then_page_load}[0] * login_failure_bucket/8,0) AS login_failure_bucket_filtered,
    IF(has_successful_login,
       ${hiveconf:login_weights_failures_then_duration_then_page_load}[1] * login_duration_bucket/8,0) AS login_duration_bucket_filtered,
    IF(has_successful_login,
       ${hiveconf:login_weights_failures_then_duration_then_page_load}[2] * page_load_time_bucket/8,0) AS page_load_time_bucket_filtered,
    ${hiveconf:login_weights_failures_then_duration_then_page_load}[0] * login_failure_bucket/8 AS login_failure_bucket,
    ${hiveconf:login_weights_failures_then_duration_then_page_load}[1] * login_duration_bucket/8 AS login_duration_bucket,
    ${hiveconf:login_weights_failures_then_duration_then_page_load}[2] * page_load_time_bucket/8 AS page_load_time_bucket,
    IF(has_successful_login, 1, 0) AS has_successful_login_true,
    IF(has_successful_login, 0, 1) AS has_successful_login_false
  FROM ${env:TMP_db}.dasp_application_entry_${env:CLUSTER}
  --where login_failure_bucket is not nULL
  --  and login_duration_bucket is not nULL
  --  and page_load_time_bucket is not null
   ;

SELECT "\n\nFor 1e: asp_quality_kpi_dist\n\n";
INSERT INTO TABLE asp_quality_kpi_dist PARTITION(denver_date)
select  grouping__id,
        LOWER(application_name) AS application_name,
        application_version,
        'portals_application_entry_dist' as metric_name,
        count(1) number_rows,
        count(visit_id) number_visits,
        count(distinct visit_id)/count(1) pct_uniq_visits,
        count(login_failure_bucket)/count(1) pct_failure,
        count(login_duration_bucket)/count(1) pct_duration,
        count(page_load_time_bucket)/COUNT(1) pct_time,
        login_performance quality_score,
        avg(login_failure_bucket) as failure_bucket,
        avg(login_duration_bucket) as duration_bucket,
        avg(page_load_time_bucket) as time_bucket,
        sum(has_successful_login_true) as all_true_yes,
        sum(has_successful_login_true) as success_true_yes,
        0 as failure_true_not,
        denver_date
 from ${env:TMP_db}.dasp_application_entry_visit_${env:CLUSTER}
 where custom_visit_level_grouping_id = 11111111
group by denver_date,
         application_name,
         application_version,
         login_performance
         grouping sets(
           (denver_date, login_performance),
           (denver_date, application_name, login_performance),
           (denver_date, application_name, application_version, login_performance)
         )
;

SELECT "\n\nFor 1f: asp_quality_kpi_mos\n\n";
--calculate final average score using the formula
INSERT OVERWRITE TABLE asp_quality_kpi_mos PARTITION(denver_date)
SELECT
  'total' as timeframe,
  grouping__id as grouping_id,
  LOWER(application_name) AS application_name,
  application_version,
  'portals_application_entry_mos' AS metric_name,
  AVG(login_performance) AS metric_value,
  AVG(login_duration_bucket_filtered)/AVG(login_performance) as duration_bucket_filtered_ratio,
  AVG(page_load_time_bucket_filtered)/AVG(login_performance) as page_load_time_bucket_filtered_ratio,
  AVG(login_failure_bucket_filtered)/AVG(login_performance) AS login_failure_bucket_filtered_ratio,
  AVG(login_duration_bucket) as duration_bucket,
  AVG(page_load_time_bucket) as page_load_time_bucket,
  AVG(login_failure_bucket) AS login_failure_bucket,
  AVG(login_performance)/(AVG(login_duration_bucket)+AVG(page_load_time_bucket)+AVG(login_failure_bucket)) as login_success_derived,
  SUM(has_successful_login_true)/SUM(has_successful_login_true+has_successful_login_false) as login_success,
  0 as otp_both_derived,
  0 as otp_both,
  0 as otp_success,
  0 as otp_failure_not,
  0 as autopay_all_derived,
  0 as autopay_all,
  0 as autopay_success,
  0 as autopay_failure_not,
  count(distinct visit_id) as number_uniq_visit,
  operation_type,
  NULL AS biller_type,
  NULL AS payment_method,
  custom_visit_level_grouping_id,
  CAST(
    CONCAT(
      CASE WHEN grouping(custom_visit_level_grouping_id) = 0 THEN 1 ELSE 0 END,
      CASE WHEN grouping(operation_type) = 0 THEN 1 ELSE 0 END,
      CASE WHEN grouping(application_version) = 0 THEN 1 ELSE 0 END,
      CASE WHEN grouping(application_name) = 0 THEN 1 ELSE 0 END,
      CASE WHEN grouping(denver_date) = 0 THEN 1 ELSE 0 END
    )
  AS BIGINT) AS custom_grouping_id,
  denver_date
FROM ${env:TMP_db}.dasp_application_entry_visit_${env:CLUSTER}
GROUP BY
  denver_date,
  application_name,
  application_version,
  operation_type,
  custom_visit_level_grouping_id
GROUPING SETS(
  (denver_date,operation_type,custom_visit_level_grouping_id), --11001
  (denver_date,application_name,operation_type,custom_visit_level_grouping_id), --11011
  (denver_date,application_name,application_version,operation_type,custom_visit_level_grouping_id) --11111
)
;


SELECT "\n\nFor 2: OTP\n\n";
--One Time Payment Metric (SpecNet,SMB,MySpectrum)
SET otp_allowed_failures = 1;
SET otp_transaction_duration_min_buckets = array(2,3,4,5);
SET otp_page_load_sec_buckets = array(2,4,10,20);
SET otp_weights_trans_then_page_load = array(0.5,0.5);

SELECT "\n\nFor 2a: dasp_otp_experience_events\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.dasp_otp_experience_events_${env:CLUSTER} PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.dasp_otp_experience_events_${env:CLUSTER} AS
WITH temp AS(
  SELECT DISTINCT
    MIN(t1.denver_date) OVER (PARTITION BY t1.visit_id) AS denver_date,
    t1.visit_id,
    t1.application_name,
    t1.application_version,
    t1.account_number,
    t1.billing_id,
    t1.billing_division,
    t1.mso,
    t2.biller_type,
    t2.payment_method,
    page_name_start,
    page_sequence_number_start,
    page_name_end,
    page_sequence_number_end,
    event,
    t1.message__timestamp AS message__timestamp_start,
    t2.message__timestamp AS message__timestamp_end,
    message__sequence_number,
    MAX(IF(`start` AND t1.message__timestamp < t2.message__timestamp, t1.message__timestamp, NULL)) OVER (PARTITION BY t1.visit_id,t2.message__timestamp) AS max_start_time,
    MIN(IF(NOT `start` AND t1.message__timestamp < t2.message__timestamp AND t1.transaction_id = t2.transaction_id, t1.message__timestamp,NULL)) OVER (PARTITION BY t1.visit_id, t2.transaction_id) AS transaction_start
  FROM (
    SELECT
      CASE
        WHEN start_true_otp is not null THEN TRUE
        WHEN start_false_otp is not null THEN FALSE
      END AS `start`,
      denver_date,
      visit_id,
      application_name,
      application_version,
      visit_level_account_number account_number,
      visit_level_billing_id billing_id,
      visit_level_billing_division billing_division,
      visit_level_mso mso,
      page_name AS page_name_start,
      transaction_id,
      page_sequence_number AS page_sequence_number_start,
      message__timestamp
    FROM ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER}
    WHERE start_true_otp is not null or start_false_otp is not null
  ) AS t1
  JOIN (
    SELECT
      denver_date,
      visit_id,
      application_name,
      application_version,
      mso,
      biller_type,
      payment_method,
      page_name AS page_name_end,
      transaction_id,
      page_sequence_number AS page_sequence_number_end,
      message__sequence_number,
      CASE
        WHEN success_otp is not null THEN 'success_otp'
        WHEN fail_otp is not null THEN 'fail_otp'
      end as event,
      message__timestamp
    FROM ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER}
    WHERE success_otp is not null or fail_otp is not null
  ) AS t2
  ON t1.visit_id = t2.visit_id
  AND t1.application_name = t2.application_name
  AND t1.application_version = t2.application_version
  --AND t1.mso = t2.mso
)
SELECT *,
  (message__timestamp_end - message__timestamp_start)/1000/60 AS transaction_min
FROM temp
WHERE(
  message__timestamp_start = max_start_time
  OR (max_start_time IS NULL AND message__timestamp_start = transaction_start)
)
;

SELECT "\n\nFor 2b: dasp_otp_experience_pages\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.dasp_otp_experience_pages_${env:CLUSTER} PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.dasp_otp_experience_pages_${env:CLUSTER} AS
SELECT
  denver_date,
  t1.application_name,
  t1.application_version,
  t2.account_number,
  t2.billing_id,
  t2.billing_division,
  t1.mso,
  CASE WHEN (CAST(grouping__id AS INT) & 2) = 0 THEN t2.biller_type ELSE 'All Biller Types' END AS biller_type,
  CASE WHEN (CAST(grouping__id AS INT) & 1) = 0 THEN t2.payment_method ELSE 'All Payment Methods' END AS payment_method,
  t1.visit_id,
  CAST(grouping__id AS INT) AS visit_level_grouping_id,
  CAST(
  CONCAT(
    CASE WHEN grouping(t2.payment_method) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t2.biller_type) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.mso) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t2.billing_division) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t2.billing_id) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t2.account_number) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.application_version) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.application_name) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.visit_id) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(denver_date) = 0 THEN 1 ELSE 0 END
  ) AS BIGINT) AS custom_visit_level_grouping_id,
  COUNT(DISTINCT IF(event='fail_otp',message__sequence_number,NULL)) AS number_of_failures,
  COUNT(DISTINCT IF(event='success_otp',message__sequence_number,NULL)) AS number_of_successes,
  AVG(DISTINCT transaction_min) AS avg_transaction_min,
  (
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message__timestamp <= t2.message__timestamp_end,cold_page_load_sec,NULL)
    ),0) +
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message__timestamp <= t2.message__timestamp_end,hot_page_load_sec,NULL)
    ),0)
  )/(
    COUNT(DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message__timestamp <= t2.message__timestamp_end, t1.page_sequence_number,NULL))
  ) AS page_load_time
FROM ${env:TMP_db}.dasp_core_pageload_time_oneday_${env:CLUSTER} AS t1
JOIN ${env:TMP_db}.dasp_otp_experience_events_${env:CLUSTER} AS t2
  ON t1.visit_id = t2.visit_id
 --AND t1.mso = t2.mso
 AND t1.application_name = t2.application_name
 AND t1.application_version = t2.application_version
GROUP BY
  denver_date,
  t1.visit_id,
  t1.application_name,
  t1.application_version,
  t2.account_number,
  t2.billing_id,
  t2.billing_division,
  t1.mso,
  t2.biller_type,
  t2.payment_method
GROUPING SETS
(
  ( denver_date,
    t1.application_name,
    t1.application_version,
    t2.account_number,
    t2.billing_id,
    t2.billing_division,
    t1.mso,
    t2.biller_type,
    t1.visit_id),
  ( denver_date,
    t1.application_name,
    t1.application_version,
    t2.account_number,
    t2.billing_id,
    t2.billing_division,
    t1.mso,
    t2.payment_method,
    t1.visit_id),
  ( denver_date,
    t1.application_name,
    t1.application_version,
    t2.account_number,
    t2.billing_id,
    t2.billing_division,
    t1.mso,
    t1.visit_id)
)
;

SELECT "\n\nFor 2c: dasp_otp_experience\n\n";
--convert real numbers to score buckets
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_otp_experience_${env:CLUSTER} PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.dasp_otp_experience_${env:CLUSTER} AS
SELECT
  denver_date,
  application_name,
  application_version,
  account_number,
  billing_id,
  billing_division,
  mso,
  biller_type,
  payment_method,
  custom_visit_level_grouping_id,
  visit_id,
  IF(number_of_successes >= 1, TRUE, FALSE) AS has_successful_otp,
  IF(number_of_failures > ${hiveconf:otp_allowed_failures},TRUE,FALSE) AS has_more_than_allowed_failured_otps,
  number_of_successes,
  number_of_failures,
  avg_transaction_min,
  page_load_time,
  CASE
    WHEN avg_transaction_min <= ${hiveconf:otp_transaction_duration_min_buckets}[0] THEN 8
    WHEN ${hiveconf:otp_transaction_duration_min_buckets}[0] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:otp_transaction_duration_min_buckets}[1] THEN 5
    WHEN ${hiveconf:otp_transaction_duration_min_buckets}[1] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:otp_transaction_duration_min_buckets}[2] THEN 3
    WHEN ${hiveconf:otp_transaction_duration_min_buckets}[2] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:otp_transaction_duration_min_buckets}[3] THEN 2
    WHEN ${hiveconf:otp_transaction_duration_min_buckets}[3] < avg_transaction_min THEN 1
  END AS transaction_duration_min_bucket,
  CASE
    WHEN page_load_time <= ${hiveconf:otp_page_load_sec_buckets}[0] THEN 8
    WHEN ${hiveconf:otp_page_load_sec_buckets}[0] < page_load_time AND page_load_time <= ${hiveconf:otp_page_load_sec_buckets}[1] THEN 5
    WHEN ${hiveconf:otp_page_load_sec_buckets}[1] < page_load_time AND page_load_time <= ${hiveconf:otp_page_load_sec_buckets}[2] THEN 3
    WHEN ${hiveconf:otp_page_load_sec_buckets}[2] < page_load_time AND page_load_time <= ${hiveconf:otp_page_load_sec_buckets}[3] THEN 2
    WHEN ${hiveconf:otp_page_load_sec_buckets}[3] < page_load_time THEN 1
END AS page_load_sec_bucket
FROM ${env:TMP_db}.dasp_otp_experience_pages_${env:CLUSTER}
;

SELECT "\n\nFor 2d: asp_quality_kpi_dist_sum\n\n";
INSERT INTO TABLE asp_quality_kpi_dist PARTITION(denver_date)
select  grouping__id,
        LOWER(application_name) AS application_name,
        application_version,
        'portals_one_time_payment_sum' as metric_name,
        count(1) number_rows,
        count(visit_id) number_visits,
        count(distinct visit_id)/count(1) pct_uniq_visits,
        0 as pct_failure,
        count(transaction_duration_min_bucket)/count(1) pct_duration,
        count(page_load_sec_bucket)/count(1) pct_time,
        0 as quality_score,
        0 as failure_bucket,
        0 as duration_bucket,
        0 as time_bucket,
        0 as all_true_yes,
        0 as success_true_yes,
        0 as failure_true_not,
        denver_date
 from ${env:TMP_db}.dasp_otp_experience_${env:CLUSTER}
 where custom_visit_level_grouping_id = 11111111
group by denver_date,
         application_name,
         application_version
         grouping sets(
           (denver_date),
           (denver_date, application_name),
           (denver_date, application_name, application_version)
        )
;

SELECT "\n\nFor 2e: dasp_otp_experience_visit\n\n";
drop table IF EXISTS ${env:TMP_db}.dasp_otp_experience_visit_${env:CLUSTER} PURGE;
CREATE TABLE if NOT EXISTS ${env:TMP_db}.dasp_otp_experience_visit_${env:CLUSTER} AS
  SELECT
  application_name,
  application_version,
  account_number,
  billing_id,
  billing_division,
  mso,
  biller_type,
  payment_method,
  custom_visit_level_grouping_id,
  denver_date,
  visit_id,
  has_successful_otp,
  has_more_than_allowed_failured_otps,
  number_of_successes,
  number_of_failures,
  avg_transaction_min,
  page_load_time,
  transaction_duration_min_bucket AS transaction_duration_min_bucket_raw,
  page_load_sec_bucket AS page_load_sec_bucket_raw,
  IF( has_successful_otp AND NOT has_more_than_allowed_failured_otps,
     (${hiveconf:otp_weights_trans_then_page_load}[0]*transaction_duration_min_bucket+${hiveconf:otp_weights_trans_then_page_load}[1]*page_load_sec_bucket)/8,
     0
  ) AS otp_performance,
  IF(has_successful_otp AND NOT has_more_than_allowed_failured_otps,
     ${hiveconf:otp_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8,0) AS duration_bucket_filtered,
  IF(has_successful_otp AND NOT has_more_than_allowed_failured_otps,
     ${hiveconf:otp_weights_trans_then_page_load}[1]*page_load_sec_bucket/8,0) AS page_load_time_bucket_filtered,
  ${hiveconf:otp_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8 AS duration_bucket,
  ${hiveconf:otp_weights_trans_then_page_load}[1]*page_load_sec_bucket/8 AS page_load_time_bucket,
  IF(has_successful_otp AND NOT has_more_than_allowed_failured_otps,1,0) AS otp_both_true_yes,
  IF(has_successful_otp AND NOT has_more_than_allowed_failured_otps,0,1) AS otp_both_true_no,
  IF(has_successful_otp,1,0) AS otp_success_true_yes,
  IF(has_successful_otp,0,1) AS otp_success_true_no,
  IF(NOT has_more_than_allowed_failured_otps,1,0) AS otp_failure_true_not,
  IF(NOT has_more_than_allowed_failured_otps,0,1) AS otp_failure_true_yes
  FROM ${env:TMP_db}.dasp_otp_experience_${env:CLUSTER}
  --where transaction_duration_min_bucket is not nULL
  --  and page_load_sec_bucket is not null
  ;

SELECT "\n\nFor 2f: asp_quality_kpi_dist\n\n";
INSERT INTO TABLE asp_quality_kpi_dist PARTITION(denver_date)
select  grouping__id,
        LOWER(application_name) AS application_name,
        application_version,
        'portals_one_time_payment_dist' as metric_name,
        count(1) number_rows,
        count(visit_id) number_visits,
        count(distinct visit_id)/count(1) pct_uniq_visits,
        0 as pct_failure,
        count(duration_bucket)/count(1) pct_duration,
        count(page_load_time_bucket)/count(1) pct_time,
        otp_performance quality_score,
        0 as failure_bucket,
        avg(duration_bucket) as duration_bucket,
        avg(page_load_time_bucket) as time_bucket,
        sum(otp_both_true_yes) as all_true_yes,
        sum(otp_success_true_yes) as success_true_yes,
        sum(otp_failure_true_not) as failure_true_not,
        denver_date
 from ${env:TMP_db}.dasp_otp_experience_visit_${env:CLUSTER}
 where custom_visit_level_grouping_id = 11111111
group by denver_date,
         application_name,
         application_version,
         otp_performance
         grouping sets(
           (denver_date, otp_performance),
           (denver_date, application_name, otp_performance),
           (denver_date, application_name, application_version, otp_performance)
         )
;

SELECT "\n\nFor 2g: asp_quality_kpi_mos\n\n";
--calculate final average score using the formula
--add the custom grouping id with the new dimensions, think if i need to add the
--visit level grouping id to the grouping sets here or its not needed
INSERT INTO asp_quality_kpi_mos PARTITION(denver_date)
SELECT
      'total' as timeframe,
      grouping__id as grouping_id,
      LOWER(application_name) AS application_name,
      application_version,
      'portals_one_time_payment_mos' AS metric_name,
      AVG(otp_performance) AS metric_value,
      AVG(duration_bucket_filtered)/AVG(otp_performance) as duration_bucket_filtered_ratio,
      AVG(page_load_time_bucket_filtered)/AVG(otp_performance) as page_load_time_bucket_filtered_ratio,
      0 as login_failure_bucket_filtered_ratio,
      AVG(duration_bucket) as duration_bucket,
      AVG(page_load_time_bucket) as page_load_time_bucket,
      0 as login_failure_bucket,
      0 as login_success_derived,
      0 as login_success,
      AVG(otp_performance)/(AVG(duration_bucket)+AVG(page_load_time_bucket)) as otp_both_derived,
      SUM(otp_both_true_yes)/SUM(otp_both_true_yes+otp_both_true_no) as otp_both,
      SUM(otp_success_true_yes)/SUM(otp_success_true_yes+otp_success_true_no) as otp_success,
      SUM(otp_failure_true_not)/SUM(otp_failure_true_not+otp_failure_true_yes) as otp_failure_not,
      0 as autopay_all_derived,
      0 as autopay_all,
      0 as autopay_success,
      0 as autopay_failure_not,
      count(distinct visit_id) as number_uniq_visit,
      NULL AS operation_type,
      biller_type,
      payment_method,
      custom_visit_level_grouping_id,
      CAST(CONCAT(
        CASE WHEN grouping(custom_visit_level_grouping_id) = 0 THEN 1 ELSE 0 END,
        CASE WHEN grouping(payment_method) = 0 THEN 1 ELSE 0 END,
        CASE WHEN grouping(biller_type) = 0 THEN 1 ELSE 0 END,
        CASE WHEN grouping(application_version) = 0 THEN 1 ELSE 0 END,
        CASE WHEN grouping(application_name) = 0 THEN 1 ELSE 0 END,
        CASE WHEN grouping(denver_date) = 0 THEN 1 ELSE 0 END
      ) AS BIGINT) AS custom_grouping_id,
      denver_date
FROM ${env:TMP_db}.dasp_otp_experience_visit_${env:CLUSTER}
GROUP BY
  denver_date,
  application_name,
  application_version,
  biller_type,
  payment_method,
  custom_visit_level_grouping_id
GROUPING SETS(
  (denver_date,biller_type,payment_method,custom_visit_level_grouping_id), --111001
  (denver_date,biller_type,payment_method,application_name,custom_visit_level_grouping_id), --111011
  (denver_date,biller_type,payment_method,application_name,application_version,custom_visit_level_grouping_id) --111111
)
;


SELECT "\n\nFor 3: Autopay\n\n";
--Autopay Performance Metric
SET allowed_enrollment_failures = 1;
SET allowed_management_failures = 1;
SET auto_transaction_duration_min_buckets = array(2,3,4,5);
SET auto_page_load_sec_buckets = array(2,4,10,20);
SET auto_weights_trans_then_page_load = array(0.5,0.5);

SELECT "\n\nFor 3a: dasp_autopay_experience_events\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.dasp_autopay_experience_events_${env:CLUSTER} PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.dasp_autopay_experience_events_${env:CLUSTER} AS
WITH temp AS(
  SELECT DISTINCT
    MIN(t1.denver_date) OVER (PARTITION BY t1.visit_id) AS denver_date,
    t1.visit_id,
    t1.application_name,
    t1.application_version,
    t1.account_number,
    t1.billing_id,
    t1.billing_division,
    t1.mso,
    t2.biller_type,
    t2.payment_method,
    page_sequence_number_start,
    page_sequence_number_end,
    IF(t1.type = 'MSA', t2.msa_type, t1.type) AS type,
    event,
    t1.message__timestamp AS message__timestamp_start,
    t2.message__timestamp AS message__timestamp_end,
    message__sequence_number,
    MAX(IF(`start` AND t1.message__timestamp < t2.message__timestamp, t1.message__timestamp, NULL)) OVER (PARTITION BY t1.visit_id,t2.message__timestamp) AS max_start_time,
    MIN(IF(NOT `start` AND t1.message__timestamp < t2.message__timestamp AND t1.transaction_id = t2.transaction_id, t1.message__timestamp,NULL)) OVER (PARTITION BY t1.visit_id, t2.transaction_id) AS transaction_start
  FROM(
    SELECT
      CASE
        WHEN start_true_autopay is not null THEN TRUE
        WHEN start_false_autopay is not null THEN FALSE
      END AS `start`,
      denver_date,
      CASE
        WHEN application_name = 'MySpectrum' THEN 'MSA'
        WHEN message__feature__feature_name = 'autoPayManagement' THEN 'manage'
        WHEN message__feature__feature_name IN ('autoPayEnroll', 'autoPayEnrollment') THEN 'enroll'
      END AS type,
      visit_id,
      application_name,
      application_version,
      visit_level_account_number account_number,
      visit_level_billing_id billing_id,
      visit_level_billing_division billing_division,
      visit_level_mso mso,
      page_name AS page_name_start,
      page_sequence_number AS page_sequence_number_start,
      transaction_id,
      message__timestamp
    FROM ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER}
    WHERE application_name IN ('SpecNet','SMB','MySpectrum')
      AND start_true_autopay is not null or start_false_autopay is not null
  ) AS t1
JOIN(
    SELECT
      denver_date,
      visit_id,
      CASE
        WHEN application_name = 'MySpectrum' THEN 'MSA'
        WHEN message__feature__feature_name IN ('autoPayManage', 'autoPayManagement') THEN 'manage'
        WHEN message__feature__feature_name IN ('autoPayEnroll', 'autoPayEnrollment')  THEN 'enroll'
      END AS type,
      CASE
        WHEN application_name = 'MySpectrum' AND previous_page_name = 'autoPayManage' THEN 'manage'
        WHEN application_name = 'MySpectrum' AND previous_page_name = 'autoPayEnroll' THEN 'enroll'
      END msa_type,
      application_name,
      application_version,
      transaction_id,
      mso,
      biller_type,
      payment_method,
      page_name AS page_name_end,
      page_sequence_number AS page_sequence_number_end,
      message__sequence_number,
      CASE
        WHEN success_autopay is not null THEN 'success_autopay'
        WHEN failure_autopay is not null THEN 'failure_autopay'
      END AS event,
      message__timestamp
    FROM ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER}
    WHERE application_name IN ('SpecNet','SMB','MySpectrum')
    AND success_autopay is not null or failure_autopay is not null
  ) AS t2
  ON t1.visit_id = t2.visit_id
  AND t1.type = t2.type
  AND t1.application_name = t2.application_name
  AND t1.application_version = t2.application_version
  --AND t1.mso = t2.mso
)
SELECT *,
(message__timestamp_end - message__timestamp_start)/1000/60 AS transaction_min
FROM temp
WHERE(
  message__timestamp_start = max_start_time
  OR (max_start_time IS NULL AND message__timestamp_start = transaction_start)
)
;

SELECT "\n\nFor 3b: dasp_autopay_experience_pages\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.dasp_autopay_experience_pages_${env:CLUSTER} PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.dasp_autopay_experience_pages_${env:CLUSTER} AS
SELECT
  denver_date,
  t1.application_name,
  t1.application_version,
  t2.account_number,
  t2.billing_id,
  t2.billing_division,
  t1.mso,
  CASE WHEN (CAST(grouping__id AS INT) & 2) = 0 THEN t2.biller_type ELSE 'All Biller Types' END AS biller_type,
  CASE WHEN (CAST(grouping__id AS INT) & 1) = 0 THEN t2.payment_method ELSE 'All Payment Methods' END AS payment_method,
  t1.visit_id,
  CAST(grouping__id AS INT) AS visit_level_grouping_id,
  CAST(
  CONCAT(
    CASE WHEN grouping(t2.payment_method) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t2.biller_type) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.mso) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t2.billing_division) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t2.billing_id) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t2.account_number) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.application_version) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.application_name) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(t1.visit_id) = 0 THEN 1 ELSE 0 END,
    CASE WHEN grouping(denver_date) = 0 THEN 1 ELSE 0 END
  ) AS BIGINT) AS custom_visit_level_grouping_id,
  COUNT(DISTINCT IF(type = 'manage' AND event='success_autopay',message__sequence_number,NULL)) AS number_of_successes_manage,
  COUNT(DISTINCT IF(type = 'enroll' AND event='success_autopay',message__sequence_number,NULL)) AS number_of_successes_enroll,
  COUNT(DISTINCT IF(type = 'manage' AND event='failure_autopay',message__sequence_number,NULL)) AS number_of_failures_manage,
  COUNT(DISTINCT IF(type = 'enroll' AND event='failure_autopay',message__sequence_number,NULL)) AS number_of_failures_enroll,
  COUNT(DISTINCT IF(event='success_autopay',message__sequence_number,NULL)) AS number_of_successes,
  COUNT(DISTINCT IF(event='failure_autopay',message__sequence_number,NULL)) AS number_of_failures,
  AVG(DISTINCT transaction_min) AS avg_transaction_min,
  (
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message__timestamp <= t2.message__timestamp_end,cold_page_load_sec,NULL)
    ),0) +
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message__timestamp <= t2.message__timestamp_end,hot_page_load_sec,NULL)
    ),0)
  )/(
    COUNT(DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message__timestamp <= t2.message__timestamp_end, t1.page_sequence_number,NULL))
  )AS page_load_time
FROM (SELECT * from ${env:TMP_db}.dasp_core_pageload_time_oneday_${env:CLUSTER} where application_name in ('SpecNet','SMB','MySpectrum')) AS t1
JOIN ${env:TMP_db}.dasp_autopay_experience_events_${env:CLUSTER} AS t2
  ON t1.visit_id = t2.visit_id
 --AND t1.mso = t2.mso
 AND t1.application_name = t2.application_name
 AND t1.application_version = t2.application_version
GROUP BY
  denver_date,
  t1.visit_id,
  t1.application_name,
  t1.application_version,
  t2.account_number,
  t2.billing_id,
  t2.billing_division,
  t1.mso,
  t2.biller_type,
  t2.payment_method
  GROUPING SETS
  (
    ( denver_date,
      t1.application_name,
      t1.application_version,
      t2.account_number,
      t2.billing_id,
      t2.billing_division,
      t1.mso,
      t2.biller_type,
      t1.visit_id),
    ( denver_date,
      t1.application_name,
      t1.application_version,
      t2.account_number,
      t2.billing_id,
      t2.billing_division,
      t1.mso,
      t2.payment_method,
      t1.visit_id),
    ( denver_date,
      t1.application_name,
      t1.application_version,
      t2.account_number,
      t2.billing_id,
      t2.billing_division,
      t1.mso,
      t1.visit_id)
  )
;

SELECT "\n\nFor 3c: dasp_autopay_experience\n\n";
--convert real numbers to score buckets
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_autopay_experience_${env:CLUSTER} PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.dasp_autopay_experience_${env:CLUSTER} AS
SELECT
  denver_date,
  application_name,
  application_version,
  account_number,
  billing_id,
  billing_division,
  mso,
  biller_type,
  payment_method,
  custom_visit_level_grouping_id,
  visit_id,
  number_of_successes,
  number_of_successes_enroll,
  number_of_successes_manage,
  number_of_failures,
  number_of_failures_enroll,
  number_of_failures_manage,
  avg_transaction_min,
  page_load_time,
  IF(number_of_successes >= 1, TRUE, FALSE) AS has_success,
  IF(number_of_failures_enroll > ${hiveconf:allowed_enrollment_failures}, TRUE, FALSE) AS has_more_than_allowed_enroll_failures,
  IF(number_of_failures_manage > ${hiveconf:allowed_management_failures}, TRUE, FALSE) AS has_more_than_allowed_manage_failures,
  CASE
    WHEN avg_transaction_min <= ${hiveconf:auto_transaction_duration_min_buckets}[0] THEN 8
    WHEN ${hiveconf:auto_transaction_duration_min_buckets}[0] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:auto_transaction_duration_min_buckets}[1] THEN 5
    WHEN ${hiveconf:auto_transaction_duration_min_buckets}[1] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:auto_transaction_duration_min_buckets}[2] THEN 3
    WHEN ${hiveconf:auto_transaction_duration_min_buckets}[2] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:auto_transaction_duration_min_buckets}[3] THEN 2
    WHEN ${hiveconf:auto_transaction_duration_min_buckets}[3] < avg_transaction_min THEN 1
  END AS transaction_duration_min_bucket,
  CASE
    WHEN page_load_time <= ${hiveconf:auto_page_load_sec_buckets}[0] THEN 8
    WHEN ${hiveconf:auto_page_load_sec_buckets}[0] < page_load_time AND page_load_time <= ${hiveconf:auto_page_load_sec_buckets}[1] THEN 5
    WHEN ${hiveconf:auto_page_load_sec_buckets}[1] < page_load_time AND page_load_time <= ${hiveconf:auto_page_load_sec_buckets}[2] THEN 3
    WHEN ${hiveconf:auto_page_load_sec_buckets}[2] < page_load_time AND page_load_time <= ${hiveconf:auto_page_load_sec_buckets}[3] THEN 2
    WHEN ${hiveconf:auto_page_load_sec_buckets}[3] < page_load_time THEN 1
  END AS page_load_sec_bucket
 FROM ${env:TMP_db}.dasp_autopay_experience_pages_${env:CLUSTER};

SELECT "\n\nFor 3d: asp_quality_kpi_dist_sum\n\n";
INSERT INTO TABLE asp_quality_kpi_dist PARTITION(denver_date)
select grouping__id,
       LOWER(application_name) AS application_name,
       application_version,
       'portals_autopay_sum' as metric_name,
       count(1) number_rows,
       count(visit_id) number_visits,
       count(distinct visit_id)/count(1) pct_uniq_visits,
       0 as pct_failure,
       count(transaction_duration_min_bucket)/count(1) pct_duration,
       count(page_load_sec_bucket)/count(1) pct_time,
       0 as quality_score,
       0 as failure_bucket,
       0 as duration_bucket,
       0 as time_bucket,
       0 as all_true_yes,
       0 as success_true_yes,
       0 as failure_true_not,
       denver_date
  from ${env:TMP_db}.dasp_autopay_experience_${env:CLUSTER}
  where custom_visit_level_grouping_id = 11111111
 group by denver_date,
        application_name,
        application_version
        grouping sets(
          (denver_date),
          (denver_date, application_name),
          (denver_date, application_name, application_version)
        )
;

SELECT "\n\nFor 3e: dasp_autopay_experience_visit\n\n";
drop table IF EXISTS ${env:TMP_db}.dasp_autopay_experience_visit_${env:CLUSTER} PURGE;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.dasp_autopay_experience_visit_${env:CLUSTER} AS
   SELECT
   application_name,
   application_version,
   account_number,
   billing_id,
   billing_division,
   mso,
   biller_type,
   payment_method,
   custom_visit_level_grouping_id,
   denver_date,
   visit_id,
   number_of_successes,
   number_of_successes_enroll,
   number_of_successes_manage,
   number_of_failures,
   number_of_failures_enroll,
   number_of_failures_manage,
   avg_transaction_min,
   page_load_time,
   has_success,
   has_more_than_allowed_enroll_failures,
   has_more_than_allowed_manage_failures,
   transaction_duration_min_bucket AS transaction_duration_min_bucket_raw,
   page_load_sec_bucket AS page_load_sec_bucket_raw,
   IF(
     has_success AND NOT has_more_than_allowed_enroll_failures AND NOT has_more_than_allowed_manage_failures,
     (${hiveconf:auto_weights_trans_then_page_load}[0]*transaction_duration_min_bucket+${hiveconf:auto_weights_trans_then_page_load}[1]*page_load_sec_bucket)/8,
     0
   ) AS autopay_performance,
   IF(has_success AND NOT has_more_than_allowed_enroll_failures AND NOT has_more_than_allowed_manage_failures,
     ${hiveconf:auto_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8,0) AS duration_bucket_filtered,
   IF(has_success AND NOT has_more_than_allowed_enroll_failures AND NOT has_more_than_allowed_manage_failures,
     ${hiveconf:auto_weights_trans_then_page_load}[1]*page_load_sec_bucket/8,0) AS page_load_time_bucket_filtered,
   ${hiveconf:auto_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8 AS duration_bucket,
   ${hiveconf:auto_weights_trans_then_page_load}[1]*page_load_sec_bucket/8 AS page_load_time_bucket,
   IF(has_success AND NOT has_more_than_allowed_enroll_failures AND NOT has_more_than_allowed_manage_failures,1,0) AS autopay_all_true_yes,
   IF(has_success AND NOT has_more_than_allowed_enroll_failures AND NOT has_more_than_allowed_manage_failures,0,1) AS autopay_all_true_no,
   IF(has_success,1,0) AS autopay_success_true_yes,
   IF(has_success,0,1) AS autopay_success_true_no,
   IF(NOT has_more_than_allowed_enroll_failures AND NOT has_more_than_allowed_manage_failures,1,0) AS autopay_failure_true_not,
   IF(NOT has_more_than_allowed_enroll_failures AND NOT has_more_than_allowed_manage_failures,0,1) AS autopay_failure_true_yes
   FROM ${env:TMP_db}.dasp_autopay_experience_${env:CLUSTER}
   --where page_load_sec_bucket is not nULL
   --and transaction_duration_min_bucket is not null
;

SELECT "\n\nFor 3f: asp_quality_kpi_dist\n\n";
INSERT INTO TABLE asp_quality_kpi_dist PARTITION(denver_date)
select grouping__id,
      LOWER(application_name) AS application_name,
      application_version,
      'portals_autopay_dist' as metric_name,
      count(1) number_rows,
      count(visit_id) number_visits,
      count(distinct visit_id)/count(1) pct_uniq_visits,
      0 as pct_failure,
      count(duration_bucket)/count(1) pct_duration,
      count(page_load_time_bucket)/count(1) pct_time,
      autopay_performance quality_score,
      0 as failure_bucket,
      avg(duration_bucket) as duration_bucket,
      avg(page_load_time_bucket) as time_bucket,
      sum(autopay_all_true_yes) as all_true_yes,
      sum(autopay_success_true_yes) as success_true_yes,
      sum(autopay_failure_true_not) as failure_true_not,
      denver_date
 from ${env:TMP_db}.dasp_autopay_experience_visit_${env:CLUSTER}
 where custom_visit_level_grouping_id = 11111111
group by denver_date,
       application_name,
       application_version,
       autopay_performance
       grouping sets(
         (denver_date, autopay_performance),
         (denver_date, application_name, autopay_performance),
         (denver_date, application_name, application_version, autopay_performance)
       )
;

SELECT "\n\nFor 3g: asp_quality_kpi_mos\n\n";
--calculate final average score using the formula
INSERT INTO asp_quality_kpi_mos PARTITION(denver_date)
SELECT
      'total' AS timeframe,
      grouping__id as grouping_id,
      LOWER(application_name) AS application_name,
      application_version,
      'portals_autopay_mos' AS metric_name,
      AVG(autopay_performance) AS metric_value,
      AVG(duration_bucket_filtered)/AVG(autopay_performance) as duration_bucket_filtered_ratio,
      AVG(page_load_time_bucket_filtered)/AVG(autopay_performance) as page_load_time_bucket_filtered_ratio,
      0 as login_failure_bucket_filtered_ratio,
      AVG(duration_bucket) as duration_bucket,
      AVG(page_load_time_bucket) as page_load_time_bucket,
      0 as login_failure_bucket,
      0 as login_success_derived,
      0 as login_success,
      0 as otp_both_derived,
      0 as otp_both,
      0 as otp_success,
      0 as otp_failure_not,
      AVG(autopay_performance)/(AVG(duration_bucket)+AVG(page_load_time_bucket)) as autopay_all_derived,
      SUM(autopay_all_true_yes)/SUM(autopay_all_true_yes+autopay_all_true_no) as autopay_all,
      SUM(autopay_success_true_yes)/SUM(autopay_success_true_yes+autopay_success_true_no) as autopay_success,
      SUM(autopay_failure_true_not)/SUM(autopay_failure_true_not+autopay_failure_true_yes) as autopay_failure_not,
      count(distinct visit_id) as number_uniq_visit,
      NULL AS operation_type,
      biller_type,
      payment_method,
      custom_visit_level_grouping_id,
      CAST(CONCAT(
        CASE WHEN grouping(custom_visit_level_grouping_id) = 0 THEN 1 ELSE 0 END,
        CASE WHEN grouping(payment_method) = 0 THEN 1 ELSE 0 END,
        CASE WHEN grouping(biller_type) = 0 THEN 1 ELSE 0 END,
        CASE WHEN grouping(application_version) = 0 THEN 1 ELSE 0 END,
        CASE WHEN grouping(application_name) = 0 THEN 1 ELSE 0 END,
        CASE WHEN grouping(denver_date) = 0 THEN 1 ELSE 0 END
      ) AS BIGINT) AS custom_grouping_id,
      denver_date
FROM ${env:TMP_db}.dasp_autopay_experience_visit_${env:CLUSTER}
GROUP BY
  denver_date,
  application_name,
  application_version,
  biller_type,
  payment_method,
  custom_visit_level_grouping_id
GROUPING SETS(
  (denver_date,biller_type,payment_method,custom_visit_level_grouping_id), --111001
  (denver_date,biller_type,payment_method,application_name,custom_visit_level_grouping_id), --111011
  (denver_date,biller_type,payment_method,application_name,application_version,custom_visit_level_grouping_id) --111111
)
;

CREATE TEMPORARY TABLE ${env:TMP_db}.dasp_quality_visit_agg_${env:CLUSTER} AS
SELECT * from asp_quality_visit_agg where denver_date='NOTHING';

--This should be a JOIN between tables. Update once NULL MSOs are removed.
WITH max_visits AS(
  SELECT
    denver_date,
    application_name,
    visit_id,
    MAX(start_true_otp) start_true_otp,
    MAX(success_otp) success_otp,
    MAX(fail_otp) fail_otp,
    MAX(start_true_autopay) start_true_autopay,
    MAX(success_autopay) success_autopay,
    MAX(failure_autopay) failure_autopay
  FROM ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER}
  GROUP BY
    denver_date,
    visit_id,
    application_name
)
INSERT OVERWRITE TABLE ${env:TMP_db}.dasp_quality_visit_agg_${env:CLUSTER}
SELECT
  ae.visit_id,
  ae.account_number,
  ae.billing_id,
  ae.billing_division,
  ae.application_name AS application_name,
  ae.application_version AS application_version,
  ae.mso AS mso,
  --APP Entry
  ae.login_performance AS app_entry_score,
  ae.login_failure_bucket_raw AS app_entry_login_failure_bucket,
  ae.login_duration_bucket_raw AS app_entry_login_duration_bucket,
  ae.page_load_time_bucket_raw AS app_entry_page_load_bucket,
  ae.login_failure_bucket_filtered AS app_entry_login_failure_score,
  ae.login_duration_bucket_filtered AS app_entry_login_duration_score,
  ae.page_load_time_bucket_filtered AS app_entry_page_load_score,
  ae.successful_logins AS app_entry_success_count,
  ae.failed_logins AS app_entry_fail_count,
  ae.login_duration_sec AS app_entry_avg_duration_sec,
  ae.page_load_time AS app_entry_avg_page_load_sec,
  ae.has_successful_login AS app_entry_has_success,
  --OTP
  CAST(NULL AS FLOAT) AS otp_score,
  CAST(NULL AS INT) AS otp_transaction_duration_bucket,
  CAST(NULL AS INT) AS otp_page_load_bucket,
  CAST(NULL AS FLOAT) AS otp_transaction_duration_score,
  CAST(NULL AS FLOAT) AS otp_page_load_score,
  CAST(NULL AS INT) AS otp_success_count,
  CAST(NULL AS INT) AS otp_fail_count,
  CAST(NULL AS FLOAT) AS otp_avg_transaction_min,
  CAST(NULL AS FLOAT) AS otp_avg_page_load_sec,
  CAST(NULL AS BOOLEAN) AS otp_has_success,
  CAST(NULL AS BOOLEAN) AS otp_more_than_allowed_fails,
  --AP
  CAST(NULL AS FLOAT) AS ap_score,
  CAST(NULL AS INT) AS ap_transaction_duration_bucket,
  CAST(NULL AS INT) AS ap_page_load_bucket,
  CAST(NULL AS FLOAT) AS ap_transaction_duration_score,
  CAST(NULL AS FLOAT) AS ap_page_load_score,
  CAST(NULL AS INT) AS ap_success_count,
  CAST(NULL AS INT) AS ap_fail_count,
  CAST(NULL AS INT) AS ap_enroll_success_count,
  CAST(NULL AS INT) AS ap_manage_success_count,
  CAST(NULL AS INT) AS ap_enroll_fail_count,
  CAST(NULL AS INT) AS ap_manage_fail_count,
  CAST(NULL AS FLOAT) AS ap_avg_transaction_min,
  CAST(NULL AS FLOAT) AS ap_avg_page_load_sec,
  CAST(NULL AS BOOLEAN) AS ap_has_success,
  CAST(NULL AS BOOLEAN) AS ap_enroll_more_than_allowed_fails,
  CAST(NULL AS BOOLEAN) AS ap_manage_more_than_allowed_fails,
  mv.is_otp_abandon,
  mv.is_ap_abandon,
  ae.denver_date AS denver_date
FROM ${env:TMP_db}.dasp_application_entry_visit_${env:CLUSTER} ae
FULL OUTER JOIN
(
  SELECT
    visit_id,
    denver_date,
    IF(start_true_otp IS NOT NULL AND success_otp IS NULL AND fail_otp IS NULL,TRUE,FALSE) is_otp_abandon,
    IF(start_true_autopay IS NOT NULL AND success_autopay IS NULL AND failure_autopay IS NULL,TRUE,FALSE) is_ap_abandon
  FROM max_visits
) mv ON ae.visit_id = mv.visit_id
     AND ae.denver_date = mv.denver_date
WHERE custom_visit_level_grouping_id = 11111111
UNION ALL
SELECT
  otp.visit_id,
  otp.account_number,
  otp.billing_id,
  otp.billing_division,
  otp.application_name,
  otp.application_version,
  otp.mso,
  --APP Entry
  CAST(NULL AS FLOAT) AS app_entry_score,
  CAST(NULL AS INT) AS app_entry_login_failure_bucket,
  CAST(NULL AS INT) AS app_entry_login_duration_bucket,
  CAST(NULL AS INT) AS app_entry_page_load_bucket,
  CAST(NULL AS FLOAT) AS app_entry_login_failure_score,
  CAST(NULL AS FLOAT) AS app_entry_login_duration_score,
  CAST(NULL AS FLOAT) AS app_entry_page_load_score,
  CAST(NULL AS INT) AS app_entry_success_count,
  CAST(NULL AS INT) AS app_entry_fail_count,
  CAST(NULL AS FLOAT) AS app_entry_avg_duration_sec,
  CAST(NULL AS FLOAT) AS app_entry_avg_page_load_sec,
  CAST(NULL AS BOOLEAN) AS app_entry_has_success,
  --OTP
  otp.otp_performance AS otp_score,
  otp.transaction_duration_min_bucket_raw AS otp_transaction_duration_bucket,
  otp.page_load_sec_bucket_raw AS otp_page_load_bucket,
  otp.duration_bucket_filtered AS otp_transaction_duration_score,
  otp.page_load_time_bucket_filtered AS otp_page_load_score,
  otp.number_of_successes AS otp_success_count,
  otp.number_of_failures AS otp_fail_count,
  otp.avg_transaction_min AS otp_avg_transaction_min,
  otp.page_load_time AS otp_avg_page_load_sec,
  otp.has_successful_otp AS otp_has_success,
  otp.has_more_than_allowed_failured_otps AS otp_more_than_allowed_fails,
  --AP
  CAST(NULL AS FLOAT) AS ap_score,
  CAST(NULL AS INT) AS ap_transaction_duration_bucket,
  CAST(NULL AS INT) AS ap_page_load_bucket,
  CAST(NULL AS FLOAT) AS ap_transaction_duration_score,
  CAST(NULL AS FLOAT) AS ap_page_load_score,
  CAST(NULL AS INT) AS ap_success_count,
  CAST(NULL AS INT) AS ap_fail_count,
  CAST(NULL AS INT) AS ap_enroll_success_count,
  CAST(NULL AS INT) AS ap_manage_success_count,
  CAST(NULL AS INT) AS ap_enroll_fail_count,
  CAST(NULL AS INT) AS ap_manage_fail_count,
  CAST(NULL AS FLOAT) AS ap_avg_transaction_min,
  CAST(NULL AS FLOAT) AS ap_avg_page_load_sec,
  CAST(NULL AS BOOLEAN) AS ap_has_success,
  CAST(NULL AS BOOLEAN) AS ap_enroll_more_than_allowed_fails,
  CAST(NULL AS BOOLEAN) AS ap_manage_more_than_allowed_fails,
  CAST(NULL AS BOOLEAN) AS is_otp_abandon,
  CAST(NULL AS BOOLEAN) AS is_ap_abandon,
  otp.denver_date AS denver_date
FROM ${env:TMP_db}.dasp_otp_experience_visit_${env:CLUSTER} otp
WHERE custom_visit_level_grouping_id = 11111111
UNION ALL
SELECT
  ap.visit_id,
  ap.account_number,
  ap.billing_id,
  ap.billing_division,
  ap.application_name,
  ap.application_version,
  ap.mso,
  --APP Entry
  CAST(NULL AS FLOAT) AS app_entry_score,
  CAST(NULL AS INT) AS app_entry_login_failure_bucket,
  CAST(NULL AS INT) AS app_entry_login_duration_bucket,
  CAST(NULL AS INT) AS app_entry_page_load_bucket,
  CAST(NULL AS FLOAT) AS app_entry_login_failure_score,
  CAST(NULL AS FLOAT) AS app_entry_login_duration_score,
  CAST(NULL AS FLOAT) AS app_entry_page_load_score,
  CAST(NULL AS INT) AS app_entry_success_count,
  CAST(NULL AS INT) AS app_entry_fail_count,
  CAST(NULL AS FLOAT) AS app_entry_avg_duration_sec,
  CAST(NULL AS FLOAT) AS app_entry_avg_page_load_sec,
  CAST(NULL AS BOOLEAN) AS app_entry_has_success,
  --OTP
  CAST(NULL AS FLOAT) AS otp_score,
  CAST(NULL AS INT) AS otp_transaction_duration_bucket,
  CAST(NULL AS INT) AS otp_page_load_bucket,
  CAST(NULL AS FLOAT) AS otp_transaction_duration_score,
  CAST(NULL AS FLOAT) AS otp_page_load_score,
  CAST(NULL AS INT) AS otp_success_count,
  CAST(NULL AS INT) AS otp_fail_count,
  CAST(NULL AS FLOAT) AS otp_avg_transaction_min,
  CAST(NULL AS FLOAT) AS otp_avg_page_load_sec,
  CAST(NULL AS BOOLEAN) AS otp_has_success,
  CAST(NULL AS BOOLEAN) AS otp_more_than_allowed_fails,
  --AP
  ap.autopay_performance AS ap_score,
  ap.transaction_duration_min_bucket_raw AS ap_transaction_duration_bucket,
  ap.page_load_sec_bucket_raw AS ap_page_load_bucket,
  ap.duration_bucket_filtered AS ap_transaction_duration_score,
  ap.page_load_time_bucket_filtered AS ap_page_load_score,
  ap.number_of_successes AS ap_success_count,
  ap.number_of_failures AS ap_fail_count,
  ap.number_of_successes_enroll AS ap_enroll_success_count,
  ap.number_of_successes_manage AS ap_manage_success_count,
  ap.number_of_failures_enroll AS ap_enroll_fail_count,
  ap.number_of_failures_manage AS ap_manage_fail_count,
  ap.avg_transaction_min AS ap_avg_transaction_min,
  ap.page_load_time AS ap_avg_page_load_sec,
  ap.has_success AS ap_has_success,
  ap.has_more_than_allowed_enroll_failures AS ap_enroll_more_than_allowed_fails,
  ap.has_more_than_allowed_manage_failures AS ap_manage_more_than_allowed_fails,
  CAST(NULL AS BOOLEAN) AS is_otp_abandon,
  CAST(NULL AS BOOLEAN) AS is_ap_abandon,
  ap.denver_date AS denver_date
FROM ${env:TMP_db}.dasp_autopay_experience_visit_${env:CLUSTER} ap
WHERE custom_visit_level_grouping_id = 11111111
;

INSERT OVERWRITE TABLE asp_quality_visit_agg PARTITION(denver_date)
SELECT * FROM ${env:TMP_db}.dasp_quality_visit_agg_${env:CLUSTER};

CREATE TEMPORARY TABLE ${env:TMP_db}.dasp_quality_bucket_distribution_${env:CLUSTER} AS
SELECT
  application_name,
  CASE WHEN (grouping_id & 8) = 0 THEN application_version ELSE 'All Application Versions' END AS application_version,
  score_type,
  component_name,
  component_bucket,
  visit_count,
  SUM(visit_count) OVER (PARTITION BY denver_date, application_name, application_version, score_type, component_name) AS total_visits_component,
  visit_count
  /
  SUM(visit_count) OVER (PARTITION BY denver_date, application_name, application_version, score_type, component_name) AS percent_in_bucket,
  grouping_id,
  denver_date
FROM(
  SELECT
    application_name,
    application_version,
    score_type,
    component_name,
    component_bucket,
    COUNT(DISTINCT visit_id) AS visit_count,
    CAST(grouping__id AS INT) grouping_id,
    denver_date
  FROM
  (
    SELECT
      visit_id,
      application_name,
      application_version,
      'App Entry' AS score_type,
      MAP
      (
        'Login Failure', app_entry_login_failure_bucket,
        'Login Duration', app_entry_login_duration_bucket,
        'Page Load', app_entry_page_load_bucket,
        'Login Success', IF(app_entry_success_count>=1,1,0)
      ) components_map,
      denver_date
    FROM asp_quality_visit_agg
    WHERE denver_date = '${hiveconf:START_DATE}'
    AND app_entry_score IS NOT NULL
    AND app_entry_login_failure_bucket IS NOT NULL
    AND app_entry_login_duration_bucket IS NOT NULL
    AND app_entry_page_load_bucket IS NOT NULL
  ) metrics_sub
  LATERAL VIEW EXPLODE(components_map) components_map AS component_name, component_bucket
  GROUP BY
    denver_date,
    application_name,
    application_version,
    score_type,
    component_name,
    component_bucket
  GROUPING SETS
  (
    (denver_date,application_name,application_version,score_type,component_name,component_bucket),
    (denver_date,application_name,score_type,component_name,component_bucket)
  )
) result
UNION ALL
SELECT
  application_name,
  CASE WHEN (grouping_id & 8) = 0 THEN application_version ELSE 'All Application Versions' END AS application_version,
  score_type,
  component_name,
  component_bucket,
  visit_count,
  SUM(visit_count) OVER (PARTITION BY denver_date, application_name, application_version, score_type, component_name) AS total_visits_component,
  visit_count
  /
  SUM(visit_count) OVER (PARTITION BY denver_date, application_name, application_version, score_type, component_name) AS percent_in_bucket,
  grouping_id,
  denver_date
FROM(
  SELECT
    application_name,
    application_version,
    score_type,
    component_name,
    component_bucket,
    COUNT(DISTINCT visit_id) AS visit_count,
    CAST(grouping__id AS INT) grouping_id,
    denver_date
  FROM
  (
    SELECT
      visit_id,
      application_name,
      application_version,
      'One Time Payment' AS score_type,
      MAP
      (
        'Transaction Duration', otp_transaction_duration_bucket,
        'Page Load', otp_page_load_bucket,
        'OTP Failure', IF(otp_fail_count>1,0,1),
        'OTP Success', IF(otp_success_count>=1,1,0)
      ) components_map,
      denver_date
    FROM asp_quality_visit_agg
    WHERE denver_date = '${hiveconf:START_DATE}'
    AND otp_score IS NOT NULL
    AND otp_transaction_duration_bucket IS NOT NULL
    AND otp_page_load_bucket IS NOT NULL
  ) metrics_sub
  LATERAL VIEW EXPLODE(components_map) components_map AS component_name, component_bucket
  GROUP BY
    denver_date,
    application_name,
    application_version,
    score_type,
    component_name,
    component_bucket
  GROUPING SETS
  (
    (denver_date,application_name,application_version,score_type,component_name,component_bucket),
    (denver_date,application_name,score_type,component_name,component_bucket)
  )
) result
UNION ALL
SELECT
  application_name,
  CASE WHEN (grouping_id & 8) = 0 THEN application_version ELSE 'All Application Versions' END AS application_version,
  score_type,
  component_name,
  component_bucket,
  visit_count,
  SUM(visit_count) OVER (PARTITION BY denver_date, application_name, application_version, score_type, component_name) AS total_visits_component,
  visit_count
  /
  SUM(visit_count) OVER (PARTITION BY denver_date, application_name, application_version, score_type, component_name) AS percent_in_bucket,
  grouping_id,
  denver_date
FROM(
  SELECT
    application_name,
    application_version,
    score_type,
    component_name,
    component_bucket,
    COUNT(DISTINCT visit_id) AS visit_count,
    CAST(grouping__id AS INT) grouping_id,
    denver_date
  FROM
  (
    SELECT
      visit_id,
      application_name,
      application_version,
      'Autopay' AS score_type,
      MAP
      (
        'Transaction Duration', ap_transaction_duration_bucket,
        'Page Load', ap_page_load_bucket,
        'AP Failure', IF(ap_fail_count>1,0,1),
        'AP Success', IF(ap_success_count>=1,1,0)
      ) components_map,
      denver_date
    FROM asp_quality_visit_agg
    WHERE denver_date = '${hiveconf:START_DATE}'
    AND ap_score IS NOT NULL
    AND ap_transaction_duration_bucket IS NOT NULL
    AND ap_page_load_bucket IS NOT NULL
  ) metrics_sub
  LATERAL VIEW EXPLODE(components_map) components_map AS component_name, component_bucket
  GROUP BY
    denver_date,
    application_name,
    application_version,
    score_type,
    component_name,
    component_bucket
  GROUPING SETS
  (
    (denver_date,application_name,application_version,score_type,component_name,component_bucket),
    (denver_date,application_name,score_type,component_name,component_bucket)
  )
) result
;

INSERT OVERWRITE TABLE asp_quality_bucket_distribution PARTITION(denver_date)
SELECt * FROM ${env:TMP_db}.dasp_quality_bucket_distribution_${env:CLUSTER};

DROP TABLE IF EXISTS ${env:TMP_db}.dasp_core_pageload_time_oneday_${env:CLUSTER}   PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_application_entry_events_${env:CLUSTER}    PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_application_entry_${env:CLUSTER}           PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_application_entry_visit_${env:CLUSTER}     PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_otp_experience_events_${env:CLUSTER}       PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_otp_experience_pages_${env:CLUSTER}        PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_otp_experience_${env:CLUSTER}              PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_otp_experience_visit_${env:CLUSTER}        PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_autopay_experience_events_${env:CLUSTER}   PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_autopay_experience_pages_${env:CLUSTER}    PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_autopay_experience_${env:CLUSTER}          PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_autopay_experience_visit_${env:CLUSTER}    PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_quality_visit_agg_${env:CLUSTER}           PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.dasp_quality_bucket_distribution_${env:CLUSTER} PURGE;
