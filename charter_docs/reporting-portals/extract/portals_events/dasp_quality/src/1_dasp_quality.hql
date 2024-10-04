USE ${env:ENVIRONMENT};

SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

----Get all HH app name lvl metrics from portals set agg except for SpecMobile
--- insert OVERWRITE first time to clear partition
INSERT OVERWRITE TABLE asp_quality_kpi PARTITION(denver_date)
SELECT
  'total' as timeframe,
  application_name,
  metric_name,
  metric_value,
  denver_date
FROM
(
  SELECT
    application_name,
    denver_date,
    MAP(
      'portals_all_equipment_reset_flow_successes_hh', SUM(portals_all_equipment_reset_flow_successes),
      'portals_all_equipment_reset_flow_failures_hh', SUM(portals_all_equipment_reset_flow_failures),
      'portals_one_time_payment_successes_hh', SUM(portals_one_time_payment_successes),
      'portals_one_time_payment_failures_hh', SUM(portals_one_time_payment_failures),
      'portals_site_unique_hh', SUM(portals_site_unique)
    ) tmp_map
  FROM
    (
      SELECT
        portals_unique_acct_key,
        application_name,
        denver_date,
        IF(SUM(portals_all_equipment_reset_flow_successes) > 0, 1, 0) AS portals_all_equipment_reset_flow_successes,
        IF(SUM(portals_all_equipment_reset_flow_failures) > 0, 1, 0) AS portals_all_equipment_reset_flow_failures,
        IF(SUM(portals_one_time_payment_successes) > 0, 1, 0) AS portals_one_time_payment_successes,
        IF(SUM(portals_one_time_payment_failures) > 0, 1, 0) AS portals_one_time_payment_failures,
        IF(SUM(portals_site_unique) > 0, 1, 0) AS portals_site_unique
      FROM
        prod.venona_metric_agg_portals
      WHERE denver_date >= '${env:START_DATE}'
        AND denver_date <  '${env:END_DATE}'
      GROUP BY
        portals_unique_acct_key,
        application_name,
        denver_date
    ) sub
  GROUP BY
    application_name,
    denver_date
) map_query
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
;

----Active HHs (All)
INSERT INTO asp_quality_kpi PARTITION(denver_date)
SELECT
  'total' as timeframe,
  'All Applications' application_name,
  metric_name,
  metric_value,
  denver_date
FROM
(
  SELECT
    denver_date,
    MAP(
      'portals_site_unique_hh', SUM(portals_site_unique)
    ) tmp_map
  FROM
    (
      SELECT
        portals_unique_acct_key,
        denver_date,
        IF(SUM(portals_site_unique) > 0, 1, 0) AS portals_site_unique
      FROM
        prod.venona_metric_agg_portals
      WHERE denver_date >= '${env:START_DATE}'
        AND denver_date <  '${env:END_DATE}'
      GROUP BY
        portals_unique_acct_key,
        denver_date
    ) sub
    GROUP BY
      denver_date
) map_query
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
;

--Device log in for all except SpecMobile
INSERT INTO asp_quality_kpi PARTITION(denver_date)
SELECT
  'total' as timeframe,
  application_name,
  metric_name,
  metric_value,
  denver_date
FROM
(
  SELECT
    application_name,
    denver_date,
    MAP(
      'portals_login_attempts_devices', SUM(portals_login_attempts),
      'portals_login_failures_devices', SUM(portals_login_failures),
      'portals_site_unique_auth_devices', SUM(portals_site_unique_auth)
    ) tmp_map
  FROM
    (
      SELECT
        device_id,
        application_name,
        denver_date,
        IF(SUM(portals_login_attempts) > 0, 1, 0) AS portals_login_attempts,
        IF(SUM(portals_login_failures) > 0, 1, 0) AS portals_login_failures,
        IF(SUM(portals_site_unique_auth) > 0, 1, 0) AS portals_site_unique_auth
      FROM
          prod.venona_metric_agg_portals
      WHERE denver_date >= '${env:START_DATE}'
        AND denver_date <  '${env:END_DATE}'
      GROUP BY
        device_id,
        application_name,
        denver_date
    ) sub
  GROUP BY
    application_name,
    denver_date
) map_query
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
;

--Active HHs (SpecMobile)
INSERT INTO asp_quality_kpi PARTITION(denver_date)
SELECT
  'total' timeframe,
  LOWER(application_name) AS application_name,
  metric_name,
  metric_value,
  denver_date
FROM(
  SELECT
    denver_date,
    application_name,
    MAP(
      count(distinct portals_unique_acct_key), 'portals_site_unique_hh'
    ) tmp_map
  FROM
    (
      SELECT DISTINCT
        visit__account__enc_account_number portals_unique_acct_key,
        visit__application_details__application_name application_name,
        SUBSTRING(prod.epoch_timestamp(received__timestamp, 'America/Denver'), 0, 10) denver_date
      FROM
        prod.core_quantum_events
      WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
        AND partition_date_hour_utc < '${env:END_DATE_TZ}'
      AND visit__application_details__application_name in ('SpecMobile')
    ) A
  GROUP BY
    denver_date,
    application_name
) sub
LATERAL VIEW EXPLODE(tmp_map) tmp_map AS metric_value, metric_name
;

--Devices Login (SpecMobile)
INSERT INTO asp_quality_kpi PARTITION(denver_date)
SELECT
  'total' timeframe,
  LOWER(application_name) AS application_name,
  metric_name,
  metric_value,
  denver_date
FROM(
  SELECT
    SUBSTRING(prod.epoch_timestamp(received__timestamp, 'America/Denver'), 0, 10) AS denver_date,
    visit__application_details__application_name application_name,
    MAP(
      COUNT(DISTINCT(if(message__name = 'loginStop' and operation__success = false,visit__device__uuid,null))), 'portals_login_failures_devices',
      COUNT(DISTINCT(if(message__name = 'loginStart',visit__device__uuid,null))), 'portals_login_attempts_devices'
    ) as tmp_map
  FROM
    prod.core_quantum_events
  WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
    AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
  AND visit__application_details__application_name in ('SpecMobile')
  GROUP BY
    SUBSTRING(prod.epoch_timestamp(received__timestamp, 'America/Denver'), 0, 10),
    visit__application_details__application_name
) sub
LATERAL VIEW EXPLODE(tmp_map) tmp_map AS metric_value, metric_name
;

--Application Entry Metric (SpecNet,SpecMobile,SMB,MySpectrum)
SET login_failure_buckets = array(1,2,3,4);
SET login_duration_sec_buckets = array(4,6,10,15);
SET login_page_load_sec_buckets = array(2,4,10,20);
SET login_weights_failures_then_duration_then_page_load = array(0.6,0.2,0.2);

DROP TABLE IF EXISTS ${env:TMP_db}.kmb_dasp_application_entry_events;
CREATE TEMPORARY TABLE ${env:TMP_db}.kmb_dasp_application_entry_events AS
SELECT
  prod.epoch_converter(MIN(received__timestamp),'America/Denver') AS denver_date,
  t1.visit_id,
  t1.application_name,
  t1.application_type,
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
    visit__application_details__application_name AS application_name,
    visit__application_details__application_type AS application_type,
    state__view__current_page__page_name AS page_name,
    IF(state__view__current_page__page_sequence_number IS NULL, -1, state__view__current_page__page_sequence_number) AS page_sequence_number,
    IF(
      message__name = 'loginStop' AND operation__success = TRUE
      AND (state__view__current_page__page_name IS NULL
      OR LOWER(state__view__current_page__page_name) RLIKE 'unauth'
      OR LOWER(state__view__current_page__page_name) RLIKE 'signin'
      OR LOWER(state__view__current_page__page_name) RLIKE 'login'
      OR LOWER(state__view__current_page__page_name) RLIKE 'reauth')
      ,TRUE
      ,FALSE
    ) AS look_at_next_page,
    visit__visit_id AS visit_id,
    message__name,
    operation__success,
    message__sequence_number,
    IF(message__name = 'loginStop',visit__login__login_duration_ms,NULL)/1000 AS login_duration_sec
  FROM prod.core_quantum_events
  WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
    AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
    AND visit__application_details__application_name IN ('SpecNet','SpecMobile','SMB','MySpectrum')
    AND message__name IN ('loginStop','loginStart')
) AS t1
LEFT JOIN (
  SELECT
    state__view__current_page__page_name AS page_name,
    IF(state__view__current_page__page_sequence_number IS NULL, -1, state__view__current_page__page_sequence_number) AS page_sequence_number,
    IF(state__view__previous_page__page_sequence_number IS NULL, -1, state__view__previous_page__page_sequence_number) AS prev_page_sequence_number,
    visit__visit_id AS visit_id,
    visit__application_details__application_name AS application_name,
    visit__application_details__application_type AS application_type,
    IF(message__name = 'pageViewPerformance', state__view__current_page__performance_timing__dom_content_loaded_event_end - state__view__current_page__performance_timing__navigation_start, NULL)/1000 AS cold_page_load_sec,
    IF(message__name = 'pageView', state__view__current_page__render_details__fully_rendered_ms, NULL)/1000 AS hot_page_load_sec
  FROM prod.core_quantum_events
  WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
    AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
    AND visit__application_details__application_name IN ('SpecNet','SpecMobile','SMB','MySpectrum')
    AND message__name IN ('pageView','pageViewPerformance')
) AS t2
ON
  t1.visit_id = t2.visit_id
  AND t1.application_type = t2.application_type
  AND t1.application_name = t2.application_name
WHERE (
  (t1.page_sequence_number = t2.page_sequence_number)
  OR (t1.page_sequence_number = t2.prev_page_sequence_number)
)
GROUP BY
  t1.visit_id,
  t1.application_name,
  t1.application_type;

DROP TABLE IF EXISTS ${env:TMP_db}.kmb_dasp_application_entry;
CREATE TEMPORARY TABLE ${env:TMP_db}.kmb_dasp_application_entry AS
SELECT
  denver_date,
  visit_id,
  application_type,
  application_name,
  IF(successful_logins >=1, TRUE, FALSE) AS has_successful_login,
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
FROM ${env:TMP_db}.kmb_dasp_application_entry_events
;

INSERT INTO asp_quality_kpi PARTITION(denver_date)
SELECT
  'total' as timeframe,
  LOWER(application_name) AS application_name,
  'portals_application_entry_mos' AS metric_name,
  AVG(login_performance) AS metric_value,
  denver_date
FROM (
  SELECT
    application_name,
    denver_date,
    visit_id,
    IF(
      has_successful_login,
      (
        ${hiveconf:login_weights_failures_then_duration_then_page_load}[0] * login_failure_bucket +
        ${hiveconf:login_weights_failures_then_duration_then_page_load}[1] * login_duration_bucket +
        ${hiveconf:login_weights_failures_then_duration_then_page_load}[2] * page_load_time_bucket
      )/8,
      0
    ) AS login_performance
  FROM ${env:TMP_db}.kmb_dasp_application_entry
) AS t1
GROUP BY
application_name,
denver_date
UNION ALL
SELECT
  'total' as timeframe,
  LOWER(application_name) AS application_name,
  'portals_application_entry_mos_visits' AS metric_name,
  COUNT(DISTINCT visit_id) AS metric_value,
  denver_date
FROM (
  SELECT
    application_name,
    denver_date,
    visit_id,
    IF(
      has_successful_login,
      (
        ${hiveconf:login_weights_failures_then_duration_then_page_load}[0] * login_failure_bucket +
        ${hiveconf:login_weights_failures_then_duration_then_page_load}[1] * login_duration_bucket +
        ${hiveconf:login_weights_failures_then_duration_then_page_load}[2] * page_load_time_bucket
      )/8,
      0
    ) AS login_performance
  FROM ${env:TMP_db}.kmb_dasp_application_entry
) AS t1
GROUP BY
application_name,
denver_date
;


--One Time Payment Metric (SpecNet,SMB,MySpectrum)
SET otp_allowed_failures = 1;
SET otp_transaction_duration_min_buckets = array(2,3,4,5);
SET otp_page_load_sec_buckets = array(2,4,10,20);
SET otp_weights_trans_then_page_load = array(0.5,0.5);

DROP TABLE IF EXISTS ${env:TMP_db}.kmb_dasp_otp_experience_events;
CREATE TEMPORARY TABLE ${env:TMP_db}.kmb_dasp_otp_experience_events AS
WITH temp AS(
  SELECT DISTINCT
    MIN(t1.denver_date) OVER (PARTITION BY t1.visit_id) AS denver_date,
    t1.visit_id,
    t1.application_name,
    t1.application_type,
    t1.mso,
    page_name_start,
    page_sequence_number_start,
    page_name_end,
    page_sequence_number_end,
    event,
    t1.message__timestamp AS message__timestamp_start,
    t2.message__timestamp AS message__timestamp_end,
    message__sequence_number,
    MAX(IF(start AND t1.message__timestamp < t2.message__timestamp, t1.message__timestamp, NULL)) OVER (PARTITION BY t1.visit_id,t2.message__timestamp) AS max_start_time,
    MIN(IF(NOT start AND t1.message__timestamp < t2.message__timestamp AND t1.transaction_id = t2.transaction_id, t1.message__timestamp,NULL)) OVER (PARTITION BY t1.visit_id, t2.transaction_id) AS transaction_start
  FROM (
    SELECT
      TRUE AS start,
      prod.epoch_converter(received__timestamp,'America/Denver') AS denver_date,
      visit__visit_id AS visit_id,
      visit__application_details__application_name AS application_name,
      visit__application_details__application_type AS application_type,
      visit__account__details__mso AS mso,
      state__view__current_page__page_name AS page_name_start,
      message__feature__transaction_id AS transaction_id,
      state__view__current_page__page_sequence_number AS page_sequence_number_start,
      message__timestamp
    FROM prod.core_quantum_events
    WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
      AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
      AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum')
      AND(
        (visit__application_details__application_name = 'MySpectrum' AND message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'makePaymentButton')
        OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStart' AND message__feature__feature_name IN  ('oneTimeBillPayFlow', 'oneTimePayment'))
        OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStart' AND message__feature__feature_name IN  ('oneTimeBillPayFlow', 'oneTimePayment'))
      )
    UNION ALL
    SELECT
      FALSE AS start,
      prod.epoch_converter(received__timestamp,'America/Denver') AS denver_date,
      visit__visit_id AS visit_id,
      visit__application_details__application_name AS application_name,
      visit__application_details__application_type AS application_type,
      visit__account__details__mso AS mso,
      state__view__current_page__page_name AS page_name_start,
      message__feature__transaction_id AS transaction_id,
      state__view__current_page__page_sequence_number AS page_sequence_number_start,
      message__timestamp
    FROM prod.core_quantum_events
    WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
      AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
      AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum')
      AND(
        (message__feature__transaction_id IS NOT NULL AND message__feature__feature_name IN ('oneTimeBillPayFlow', 'oneTimePayment'))
      )
  ) AS t1
  JOIN (
    SELECT
      prod.epoch_converter(received__timestamp,'America/Denver') AS denver_date,
      visit__visit_id AS visit_id,
      visit__application_details__application_name AS application_name,
      visit__application_details__application_type AS application_type,
      visit__account__details__mso AS mso,
      state__view__current_page__page_name AS page_name_end,
      message__feature__transaction_id AS transaction_id,
      state__view__current_page__page_sequence_number AS page_sequence_number_end,
      message__sequence_number,
      CASE
        WHEN
            (visit__application_details__application_name = 'MySpectrum' AND message__name = 'pageView' AND state__view__current_page__page_name IN ('paymentSuccess', 'paymentWithAutoPaySuccess', 'paySuccess', 'paySuccessAutoPay') AND state__view__previous_page__page_name IN ('makePayment', 'reviewCompletePayment'))
            OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStop' AND message__feature__feature_name IN ('oneTimeBillPayFlow', 'oneTimePayment') AND operation__success = TRUE AND ((message__feature__feature_step_changed = FALSE AND message__event_case_id <> 'SPECNET_selectAction_billPayStop_otp_exitBillPayFlow') OR (message__feature__feature_step_name IN('oneTimePaymentAppSuccess','oneTimePaymentAutoPayAppSuccess', 'paymentWithAutoPaySuccess', 'paymentSuccess') AND message__feature__feature_step_changed = TRUE)))
            OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStop' AND ((message__feature__feature_step_name IN( 'otpSuccess', 'otpSuccessAutoPay' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name IN ('oneTimeBillPayFlow', 'oneTimePayment') AND operation__success = TRUE AND ((message__feature__feature_step_changed = FALSE AND message__event_case_id <> 'SPECNET_selectAction_billPayStop_otp_exitBillPayFlow') OR (message__feature__feature_step_name IN('oneTimePaymentAppSuccess','oneTimePaymentAutoPayAppSuccess', 'paymentWithAutoPaySuccess', 'paymentSuccess') AND message__feature__feature_step_changed = TRUE)))))
          THEN 'otp_success'
        WHEN
            (visit__application_details__application_name = 'MySpectrum' AND message__name = 'pageView' AND state__view__current_page__page_name IN ('paymentFailure', 'paymentWithAutoPayFailure', 'payUnsuccess', 'payUnsuccessAutoPay'))
            OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStop' AND message__feature__feature_step_changed = TRUE AND operation__success = FALSE AND ((message__feature__feature_name = 'oneTimePayment' AND message__feature__feature_step_name = 'paymentFailure') OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND message__feature__feature_step_name = 'oneTimePaymentError')))
            OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStop' AND message__feature__feature_step_changed = TRUE AND operation__success = FALSE AND ((message__feature__feature_name = 'oneTimePayment' AND message__feature__feature_step_name = 'paymentFailure') OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND message__feature__feature_step_name = 'oneTimePaymentError')))
          THEN 'otp_fail'
      END AS event,
      message__timestamp
    FROM prod.core_quantum_events
    WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
      AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
    AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum')
  ) AS t2
  ON t1.visit_id = t2.visit_id
  AND t1.application_name = t2.application_name
  WHERE
    event IS NOT NULL
)
SELECT *,
  (message__timestamp_end - message__timestamp_start)/1000/60 AS transaction_min
FROM temp
WHERE(
  message__timestamp_start = max_start_time
  OR (max_start_time IS NULL AND message__timestamp_start = transaction_start)
)
;

DROP TABLE IF EXISTS ${env:TMP_db}.kmb_dasp_otp_experience_pages;
CREATE TEMPORARY TABLE ${env:TMP_db}.kmb_dasp_otp_experience_pages AS
SELECT
  denver_date,
  t1.application_name,
  application_type,
  t1.mso,
  t1.visit_id,
  COUNT(DISTINCT IF(event='otp_fail',message__sequence_number,NULL)) AS number_of_failures,
  COUNT(DISTINCT IF(event='otp_success',message__sequence_number,NULL)) AS number_of_successes,
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
FROM (
  SELECT
    visit__visit_id AS visit_id,
    visit__account__details__mso AS mso,
    state__view__current_page__page_name AS page_name,
    visit__application_details__application_name AS application_name,
    IF(state__view__current_page__page_sequence_number IS NULL, -1, state__view__current_page__page_sequence_number) AS page_sequence_number,
    IF(message__name = 'pageViewPerformance', state__view__current_page__performance_timing__dom_content_loaded_event_end - state__view__current_page__performance_timing__navigation_start, NULL)/1000 AS cold_page_load_sec,
    IF(message__name = 'pageView', state__view__current_page__render_details__fully_rendered_ms, NULL)/1000 AS hot_page_load_sec,
    message__timestamp
  FROM prod.core_quantum_events
  WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
    AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
    AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum')
    AND message__name IN ('pageView','pageViewPerformance')
) AS t1
JOIN ${env:TMP_db}.kmb_dasp_otp_experience_events AS t2
ON t1.visit_id = t2.visit_id
AND t1.application_name = t2.application_name
GROUP BY
  denver_date,
  t1.application_name,
  application_type,
  t1.mso,
  t1.visit_id
;

DROP TABLE IF EXISTS ${env:TMP_db}.kmb_dasp_otp_experience;
CREATE TEMPORARY TABLE ${env:TMP_db}.kmb_dasp_otp_experience AS
SELECT
  denver_date,
  application_name,
  application_type,
  mso,
  visit_id,
  IF(number_of_successes >= 1, TRUE, FALSE) AS has_successful_otp,
  IF(number_of_failures > ${hiveconf:otp_allowed_failures},TRUE,FALSE) AS has_more_than_allowed_failured_otps,
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
FROM ${env:TMP_db}.kmb_dasp_otp_experience_pages;



INSERT INTO asp_quality_kpi PARTITION(denver_date)
SELECT
'total' as timeframe,
LOWER(application_name) AS application_name,
'portals_one_time_payment_mos' AS metric_name,
AVG(otp_performance) AS metric_value,
denver_date
FROM (
  SELECT
  application_name,
  denver_date,
  visit_id,
  IF(
    has_successful_otp AND NOT has_more_than_allowed_failured_otps,
    (${hiveconf:otp_weights_trans_then_page_load}[0]*transaction_duration_min_bucket+${hiveconf:otp_weights_trans_then_page_load}[1]*page_load_sec_bucket)/8,
    0
  ) AS otp_performance
  FROM ${env:TMP_db}.kmb_dasp_otp_experience
) AS t1
GROUP BY
denver_date,
application_name
UNION ALL
SELECT
'total' as timeframe,
LOWER(application_name) AS application_name,
'portals_one_time_payment_mos_visits' AS metric_name,
COUNT(DISTINCT visit_id) AS metric_value,
denver_date
FROM (
  SELECT
  application_name,
  denver_date,
  visit_id,
  IF(
    has_successful_otp AND NOT has_more_than_allowed_failured_otps,
    (${hiveconf:otp_weights_trans_then_page_load}[0]*transaction_duration_min_bucket+${hiveconf:otp_weights_trans_then_page_load}[1]*page_load_sec_bucket)/8,
    0
  ) AS otp_performance
  FROM ${env:TMP_db}.kmb_dasp_otp_experience
) AS t1
GROUP BY
denver_date,
application_name;


--Autopay Performance Metric
SET allowed_enrollment_failures = 1;
SET allowed_management_failures = 1;
SET auto_transaction_duration_min_buckets = array(2,3,4,5);
SET auto_page_load_sec_buckets = array(2,4,10,20);
SET auto_weights_trans_then_page_load = array(0.5,0.5);

DROP TABLE IF EXISTS ${env:TMP_db}.kmb_dasp_autopay_experience_events;
CREATE TABLE ${env:TMP_db}.kmb_dasp_autopay_experience_events AS
WITH temp AS(
  SELECT DISTINCT
    MIN(t1.denver_date) OVER (PARTITION BY t1.visit_id) AS denver_date,
    t1.visit_id,
    t1.application_name,
    t1.application_type,
    t1.mso,
    page_sequence_number_start,
    page_sequence_number_end,
    IF(t1.type = 'MSA', t2.msa_type, t1.type) AS type,
    event,
    t1.message__timestamp AS message__timestamp_start,
    t2.message__timestamp AS message__timestamp_end,
    message__sequence_number,
    MAX(IF(start AND t1.message__timestamp < t2.message__timestamp, t1.message__timestamp, NULL)) OVER (PARTITION BY t1.visit_id,t2.message__timestamp) AS max_start_time,
    MIN(IF(NOT start AND t1.message__timestamp < t2.message__timestamp AND t1.transaction_id = t2.transaction_id, t1.message__timestamp,NULL)) OVER (PARTITION BY t1.visit_id, t2.transaction_id) AS transaction_start
  FROM( --start
    SELECT
      TRUE AS start,
      prod.epoch_converter(received__timestamp,'America/Denver') AS denver_date,
      CASE
        WHEN visit__application_details__application_name = 'MySpectrum' THEN 'MSA'
        WHEN message__feature__feature_name = 'autoPayManagement' THEN 'manage'
        WHEN message__feature__feature_name IN ('autoPayEnroll', 'autoPayEnrollment') THEN 'enroll'
      END AS type,
      visit__visit_id AS visit_id,
      visit__application_details__application_name AS application_name,
      visit__application_details__application_type AS application_type,
      visit__account__details__mso AS mso,
      state__view__current_page__page_name AS page_name_start,
      state__view__current_page__page_sequence_number AS page_sequence_number_start,
      message__feature__transaction_id AS transaction_id,
      message__timestamp
    FROM prod.core_quantum_events
    WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
      AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
    AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum')
      AND (
        (visit__application_details__application_name = 'MySpectrum' AND message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'autoPayManage')
        OR (visit__application_details__application_name = 'SpecNet' AND message__name = 'featureStart' AND message__feature__feature_step_name IN ('viewAutopayEnrollment', 'autoPayEnrollStart') AND message__feature__feature_step_changed = TRUE)
        OR (visit__application_details__application_name = 'SMB' AND message__name = 'featureStart' AND ((message__feature__feature_name IN ('autoPayEnroll', 'autoPayEnrollment' ) OR (message__feature__feature_step_name IN ('viewAutopayEnrollment', 'autoPayEnrollStart') AND message__feature__feature_step_changed = TRUE))))
        OR (visit__application_details__application_name IN ('SpecNet','SMB') AND message__name = 'featureStart' AND message__feature__feature_name IN ('autoPayManagement', 'autoPayEnrollment') AND message__feature__feature_step_name IN ('autoPayEnrollStart','autoPayManageStart','viewManageAutoPay') AND message__feature__feature_step_changed)
        OR (visit__application_details__application_name IN ('SpecNet','SMB') AND message__name = 'featureStart' AND message__feature__feature_name IN ('autoPayManagement', 'autoPayEnrollment') AND message__event_case_id IN ('SPECNET_billPay_billPayStart_autoPayEnroll', 'SPECNET_billPay_billPayStart_manageAutoPay'))
      )
    UNION ALL
    SELECT
      FALSE AS start,
      prod.epoch_converter(received__timestamp,'America/Denver') AS denver_date,
      CASE
        WHEN visit__application_details__application_name = 'MySpectrum' THEN 'MSA'
        WHEN message__feature__feature_name = 'autoPayManagement' THEN 'manage'
        WHEN message__feature__feature_name IN ('autoPayEnroll', 'autoPayEnrollment') THEN 'enroll'
      END AS type,
      visit__visit_id AS visit_id,
      visit__application_details__application_name AS application_name,
      visit__application_details__application_type AS application_type,
      visit__account__details__mso AS mso,
      state__view__current_page__page_name AS page_name_start,
      state__view__current_page__page_sequence_number AS page_sequence_number_start,
      message__feature__transaction_id AS transaction_id,
      message__timestamp
    FROM prod.core_quantum_events
    WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
      AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
      AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum')
      AND (
        (message__feature__transaction_id IS NOT NULL AND message__feature__feature_name IN ('autoPayEnroll', 'autoPayEnrollment','autoPayManagement', 'autoPayManage'))
      )
  ) AS t1
JOIN(
    SELECT
      prod.epoch_converter(received__timestamp,'America/Denver') AS denver_date,
      visit__visit_id AS visit_id,
      CASE
        WHEN visit__application_details__application_name = 'MySpectrum' THEN 'MSA'
        WHEN message__feature__feature_name IN ('autoPayManage', 'autoPayManagement') THEN 'manage'
        WHEN message__feature__feature_name IN ('autoPayEnroll', 'autoPayEnrollment')  THEN 'enroll'
      END AS type,
      CASE
        WHEN visit__application_details__application_name = 'MySpectrum' AND state__view__previous_page__page_name = 'autoPayManage' THEN 'manage'
        WHEN visit__application_details__application_name = 'MySpectrum' AND state__view__previous_page__page_name = 'autoPayEnroll' THEN 'enroll'
      END msa_type,
      visit__application_details__application_name AS application_name,
      visit__application_details__application_type AS application_type,
      message__feature__transaction_id AS transaction_id,
      visit__account__details__mso AS mso,
      state__view__current_page__page_name AS page_name_end,
      state__view__current_page__page_sequence_number AS page_sequence_number_end,
      message__sequence_number,
      CASE
        WHEN
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
          THEN 'success'
        WHEN
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
          THEN 'failure'
      END AS event,
      message__timestamp
    FROM prod.core_quantum_events
    WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
      AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
      AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum')
  ) AS t2
  ON t1.visit_id = t2.visit_id
  AND t1.type = t2.type
  AND t1.application_name = t2.application_name
  WHERE event IS NOT NULL
)
SELECT *,
(message__timestamp_end - message__timestamp_start)/1000/60 AS transaction_min
FROM temp
WHERE(
  message__timestamp_start = max_start_time
  OR (max_start_time IS NULL AND message__timestamp_start = transaction_start)
)
;

DROP TABLE IF EXISTS ${env:TMP_db}.kmb_dasp_autopay_experience_pages;
CREATE TEMPORARY TABLE ${env:TMP_db}.kmb_dasp_autopay_experience_pages AS
SELECT
  denver_date,
  t1.application_name,
  application_type,
  t1.mso,
  t1.visit_id,
  COUNT(DISTINCT IF(type = 'manage' AND event='failure',message__sequence_number,NULL)) AS number_of_failures_manage,
  COUNT(DISTINCT IF(type = 'enroll' AND event='failure',message__sequence_number,NULL)) AS number_of_failures_enroll,
  COUNT(DISTINCT IF(event='success',message__sequence_number,NULL)) AS number_of_successes,
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
FROM (
  SELECT
    visit__visit_id AS visit_id,
    visit__account__details__mso AS mso,
    state__view__current_page__page_sequence_number AS page_sequence_number,
    visit__application_details__application_name AS application_name,
    IF(message__name = 'pageViewPerformance', state__view__current_page__performance_timing__dom_content_loaded_event_end - state__view__current_page__performance_timing__navigation_start, NULL)/1000 AS cold_page_load_sec,
    IF(message__name = 'pageView', state__view__current_page__render_details__fully_rendered_ms, NULL)/1000 AS hot_page_load_sec,
    message__timestamp
  FROM prod.core_quantum_events
  WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
    AND partition_date_hour_utc <  '${env:END_DATE_TZ}'
    AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum')
    AND message__name IN ('pageView','pageViewPerformance')
) AS t1
JOIN ${env:TMP_db}.kmb_dasp_autopay_experience_events AS t2
ON t1.visit_id = t2.visit_id
AND t1.application_name = t2.application_name
GROUP BY
  denver_date,
  t1.application_name,
  application_type,
  t1.mso,
  t1.visit_id
;

DROP TABLE IF EXISTS ${env:TMP_db}.kmb_dasp_autopay_experience;
CREATE TEMPORARY TABLE ${env:TMP_db}.kmb_dasp_autopay_experience AS
SELECT
  denver_date,
  application_name,
  application_type,
  mso,
  visit_id,
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
FROM ${env:TMP_db}.kmb_dasp_autopay_experience_pages;

INSERT INTO asp_quality_kpi PARTITION(denver_date)
SELECT
  'total' AS timeframe,
  LOWER(application_name) AS application_name,
  'portals_autopay_mos' AS metric_name,
  AVG(autopay_performance) AS metric_value,
  denver_date
FROM (
  SELECT
  application_name,
  denver_date,
  visit_id,
  IF(
    has_success AND NOT has_more_than_allowed_enroll_failures AND NOT has_more_than_allowed_manage_failures,
    (${hiveconf:auto_weights_trans_then_page_load}[0]*transaction_duration_min_bucket+${hiveconf:auto_weights_trans_then_page_load}[1]*page_load_sec_bucket)/8,
    0
  ) AS autopay_performance
  FROM ${env:TMP_db}.kmb_dasp_autopay_experience
) AS t1
GROUP BY
denver_date,
application_name
UNION ALL
SELECT
  'total' AS timeframe,
  LOWER(application_name) AS application_name,
  'portals_autopay_mos_visits' AS metric_name,
  COUNT(DISTINCT visit_id) AS metric_value,
  denver_date
  FROM (
    SELECT
    application_name,
    denver_date,
    visit_id,
    IF(
      has_success AND NOT has_more_than_allowed_enroll_failures AND NOT has_more_than_allowed_manage_failures,
      (${hiveconf:auto_weights_trans_then_page_load}[0]*transaction_duration_min_bucket+${hiveconf:auto_weights_trans_then_page_load}[1]*page_load_sec_bucket)/8,
      0
    ) AS autopay_performance
    FROM ${env:TMP_db}.kmb_dasp_autopay_experience
  ) AS t1
GROUP BY
denver_date,
application_name;
