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

SET hive.merge.tezfiles=false;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/epochtotimestamp-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_timestamp AS 'Epoch_Timestamp';

SET cya_allowed_failures = 1;
SET cya_transaction_duration_min_buckets = array(2,3,4,5);
SET cya_page_load_sec_buckets = array(2,4,10,20);
SET cya_weights_trans_then_page_load = array(0.5,0.5);

---------------------
-------CONFIRM-------
---------------------

DROP TABLE IF EXISTS ${env:TMP_db}.idm_core_events_base;
CREATE TABLE ${env:TMP_db}.idm_core_events_base AS
SELECT
  received__timestamp,
  cast(epoch_converter(received__timestamp,'America/Denver') as date) AS denver_date,
  visit__application_details__application_name AS application_name,
  visit__application_details__application_type AS application_type,
  visit__application_details__app_version AS application_version,
  visit__visit_id AS visit_id,
  visit__account__enc_account_number AS account_number,
  visit__account__enc_account_billing_id AS billing_id,
  visit__account__enc_account_billing_division AS billing_division,
  visit__account__details__mso AS mso,
  message__name AS message_name,
  message__timestamp AS message_timestamp,
  message__sequence_number AS message_sequence_number,
  operation__success AS operation__success,
  operation__operation_type as operation_type,
  state__view__current_page__page_name AS page_name,
  state__view__current_page__page_sequence_number AS page_sequence_number,
  state__view__previous_page__page_sequence_number AS previous_page_sequence_number,
  state__view__previous_page__page_name AS previous_page_name,
  IF(message__name = 'loginStop',visit__login__login_duration_ms,NULL)/1000 AS login_duration_sec,
  IF(message__name = 'pageViewPerformance', state__view__current_page__performance_timing__dom_content_loaded_event_end - state__view__current_page__performance_timing__navigation_start, NULL)/1000 AS cold_page_load_sec,
  IF(message__name = 'pageView', state__view__current_page__render_details__fully_rendered_ms, NULL)/1000 AS hot_page_load_sec,
  message__feature__transaction_id AS transaction_id,
  CASE
    WHEN message__name = 'pageView' AND state__view__current_page__page_name = 'confirmYourAccount'
    THEN 'start'
    WHEN (message__name = 'pageView' AND state__view__current_page__page_name = 'verifyYourIdentity' AND state__view__previous_page__page_name = 'confirmYourAccount')
    THEN 'success'
    WHEN (state__view__current_page__page_name = 'confirmYourAccount'
         AND application__error__error_message IN ("The info you entered doesn\'t match our records. Please try again.",'SafeValue must use [property]=binding: The info you entered matches an inactive account. For more assistance, please contact <a href="https://www.spectrum.net/contact-us">Spectrum Support</a> (see http://g.co/ng/security#xss)'))
         OR (message__name = 'pageView' AND state__view__current_page__page_name = 'inviteExpired')
    THEN 'fail'
  END AS cya_status,
  CASE
    WHEN message__name = 'pageView' AND state__view__current_page__page_name IN ('checkYourInfo','username')
    THEN 'start'
    WHEN message__name = 'loginStop' AND operation__success AND message__event_case_Id = 'IdentityManagement_Manual_Login_Success'
    THEN 'success'
    WHEN (state__view__current_page__page_name = 'enterYourPassword' AND application__error__error_message IN ('This username is already in use. Please enter a different one.', "We\'re sorry, something didn\'t work quite right. Please try again."))
         OR (message__name = 'pageView' AND state__view__current_page__page_name = 'inviteExpired')
    THEN 'fail'
  END AS cun_status,
  CASE
    WHEN message__name = 'pageView' AND state__view__current_page__page_name IN ('resetYourPassword','forcedPasswordReset')
    THEN 'start'
    WHEN message__name = 'loginStop' AND operation__success AND message__event_case_Id = 'IdentityManagement_Manual_Login_Success'
    THEN 'success'
    WHEN (state__view__current_page__page_name = 'resetYourPassword' AND application__error__error_message IN ('Unexpected event loginStart received in state authenticating. Analytics partially disabled until the next tune',"Please enter a password that\'s different from your previously used passwords."))
         OR (message__name = 'pageView' AND state__view__current_page__page_name = 'inviteExpired')
    THEN 'fail'
  END AS rpw_status,
  CASE
    WHEN message__name = 'pageView' AND state__view__current_page__page_name = 'verifyYourIdentity'
    THEN 'start'
    WHEN message__name = 'pageView'
         AND state__view__previous_page__page_name = 'verificationCode'
         AND state__view__current_page__page_name IN ('resetYourPassword','checkYourInfo','username')
    THEN 'success'
    WHEN (state__view__current_page__page_name IN ('verifyYourIdentity','verificationCode') AND application__error__error_message IN ("The info you entered doesn\’t match our records. Please try again.","You\'ve exceeded the maximum number of attempts. Please try again."))
         OR (message__name = 'pageView' AND state__view__current_page__page_name = 'inviteExpired')
    THEN 'fail'
  END AS vyi_status
FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
WHERE partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}'
  AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}'
  AND visit__application_details__application_name = 'IDManagement'
;

DROP TABLE IF EXISTS ${env:TMP_db}.idm_cya_experience_events;
CREATE TEMPORARY TABLE ${env:TMP_db}.idm_cya_experience_events AS
WITH temp AS(
  SELECT DISTINCT
    t1.visit_id,
    t1.application_name,
    t1.application_version,
    t1.application_type,
    t1.mso,
    page_name_start,
    page_sequence_number_start,
    page_name_end,
    page_sequence_number_end,
    event,
    t1.message_timestamp AS message_timestamp_start,
    t2.message_timestamp AS message_timestamp_end,
    message_sequence_number,
    MAX(IF(`start` AND t1.message_timestamp < t2.message_timestamp, t1.message_timestamp, NULL)) OVER (PARTITION BY t1.visit_id,t2.message_timestamp) AS max_start_time,
    t1.denver_date
  FROM (
    SELECT
      TRUE AS `start`,
      visit_id,
      application_name,
      application_type,
      application_version,
      mso,
      page_name AS page_name_start,
      page_sequence_number AS page_sequence_number_start,
      message_timestamp,
      denver_date
    FROM ${env:TMP_db}.idm_core_events_base
    WHERE cya_status = 'start'
  ) AS t1
  JOIN (
    SELECT
      visit_id,
      application_name,
      application_type,
      mso,
      page_name AS page_name_end,
      page_sequence_number AS page_sequence_number_end,
      message_sequence_number,
      CASE
        WHEN cya_status = 'success' THEN 'cya_success'
        WHEN cya_status = 'fail' THEN 'cya_fail'
      END AS event,
      message_timestamp,
      denver_date
    FROM ${env:TMP_db}.idm_core_events_base
    WHERE cya_status IN ('success', 'fail')
  ) AS t2
  ON t1.visit_id = t2.visit_id
  AND t1.application_name = t2.application_name
)
SELECT *,
  (message_timestamp_end - message_timestamp_start)/1000/60 AS transaction_min
FROM temp
WHERE message_timestamp_start = max_start_time
;

DROP TABLE IF EXISTS ${env:TMP_db}.idm_cya_experience_pages;
CREATE TEMPORARY TABLE ${env:TMP_db}.idm_cya_experience_pages AS
SELECT
  t1.application_name,
  t2.application_version,
  t2.application_type,
  t1.visit_id,
  COUNT(DISTINCT IF(event='cya_fail',message_sequence_number,NULL)) AS number_of_failures,
  COUNT(DISTINCT IF(event='cya_success',message_sequence_number,NULL)) AS number_of_successes,
  AVG(DISTINCT transaction_min) AS avg_transaction_min,
  (
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end,cold_page_load_sec,NULL)
    ),0) +
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end,hot_page_load_sec,NULL)
    ),0)
  )/(
    COUNT(DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end, t1.page_sequence_number,NULL))
  ) AS page_load_time,
  MIN(t2.denver_date) denver_date
FROM (
  SELECT
    visit_id,
    page_name,
    application_name,
    IF(page_sequence_number IS NULL, -1, page_sequence_number) AS page_sequence_number,
    cold_page_load_sec,
    hot_page_load_sec,
    message_timestamp,
    denver_date
  FROM ${env:TMP_db}.idm_core_events_base
  WHERE message_name IN ('pageView','pageViewPerformance')
) AS t1
JOIN ${env:TMP_db}.idm_cya_experience_events AS t2
ON t1.visit_id = t2.visit_id
AND t1.application_name = t2.application_name
GROUP BY
  t1.application_name,
  t2.application_version,
  t2.application_type,
  t1.visit_id
HAVING MIN(t2.denver_date) = '${hiveconf:START_DATE}'
 ;

DROP TABLE IF EXISTS ${env:TMP_db}.idm_cya_experience;
CREATE TABLE `${env:TMP_db}.idm_cya_experience`(
  `application_name` string,
  `application_version` string,
  `application_type` string,
  `visit_id` string,
  `has_successful_cya` boolean,
  `has_more_than_allowed_failures_cyas` boolean,
  `number_of_successes` bigint,
  `number_of_failures` bigint,
  `page_load_time` double,
  `avg_transaction_min` double,
  `transaction_duration_min_bucket` int,
  `page_load_sec_bucket` int,
  `score` double,
  `cya_performance` double,
  `duration_bucket_filtered` double,
  `page_load_time_bucket_filtered` double,
  `duration_bucket` double,
  `page_load_time_bucket` double,
  `cya_both_true_yes` int,
  `cya_both_true_no` int,
  `cya_success_true_yes` int,
  `cya_success_true_no` int,
  `cya_failure_true_not` int,
  `cya_failure_true_yes` int,
  `denver_date` date)
;

WITH cte_idm_cya_experience AS (
  SELECT
    application_name,
    application_version,
    application_type,
    visit_id,
    IF(number_of_successes >= 1, TRUE, FALSE) AS has_successful_cya,
    IF(number_of_failures > ${hiveconf:cya_allowed_failures},TRUE,FALSE) AS has_more_than_allowed_failures_cyas,
    number_of_successes,
    number_of_failures,
    page_load_time,
    avg_transaction_min,
    CASE
      WHEN avg_transaction_min <= ${hiveconf:cya_transaction_duration_min_buckets}[0] THEN 8
      WHEN ${hiveconf:cya_transaction_duration_min_buckets}[0] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:cya_transaction_duration_min_buckets}[1] THEN 5
      WHEN ${hiveconf:cya_transaction_duration_min_buckets}[1] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:cya_transaction_duration_min_buckets}[2] THEN 3
      WHEN ${hiveconf:cya_transaction_duration_min_buckets}[2] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:cya_transaction_duration_min_buckets}[3] THEN 2
      WHEN ${hiveconf:cya_transaction_duration_min_buckets}[3] < avg_transaction_min THEN 1
    END AS transaction_duration_min_bucket,
    CASE
      WHEN page_load_time <= ${hiveconf:cya_page_load_sec_buckets}[0] THEN 8
      WHEN ${hiveconf:cya_page_load_sec_buckets}[0] < page_load_time AND page_load_time <= ${hiveconf:cya_page_load_sec_buckets}[1] THEN 5
      WHEN ${hiveconf:cya_page_load_sec_buckets}[1] < page_load_time AND page_load_time <= ${hiveconf:cya_page_load_sec_buckets}[2] THEN 3
      WHEN ${hiveconf:cya_page_load_sec_buckets}[2] < page_load_time AND page_load_time <= ${hiveconf:cya_page_load_sec_buckets}[3] THEN 2
      WHEN ${hiveconf:cya_page_load_sec_buckets}[3] < page_load_time THEN 1
  END AS page_load_sec_bucket,
  denver_date
  FROM ${env:TMP_db}.idm_cya_experience_pages
)
INSERT INTO TABLE ${env:TMP_db}.idm_cya_experience
SELECT
  application_name,
  application_version,
  application_type,
  visit_id,
  has_successful_cya,
  has_more_than_allowed_failures_cyas,
  number_of_successes,
  number_of_failures,
  page_load_time,
  avg_transaction_min,
  transaction_duration_min_bucket,
  page_load_sec_bucket,
  ((1/2*transaction_duration_min_bucket + 1/2*page_load_sec_bucket)/8) * if(has_successful_cya = true,1,0) * if(has_more_than_allowed_failures_cyas = false,1,0) as score,
  IF(
    has_successful_cya AND NOT has_more_than_allowed_failures_cyas,
    (${hiveconf:cya_weights_trans_then_page_load}[0]*transaction_duration_min_bucket+${hiveconf:cya_weights_trans_then_page_load}[1]*page_load_sec_bucket)/8,
    0
  ) AS cya_performance,
  IF(has_successful_cya AND NOT has_more_than_allowed_failures_cyas,
   ${hiveconf:cya_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8,0) AS duration_bucket_filtered,
  IF(has_successful_cya AND NOT has_more_than_allowed_failures_cyas,
   ${hiveconf:cya_weights_trans_then_page_load}[1]*page_load_sec_bucket/8,0) AS page_load_time_bucket_filtered,
  ${hiveconf:cya_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8 AS duration_bucket,
  ${hiveconf:cya_weights_trans_then_page_load}[1]*page_load_sec_bucket/8 AS page_load_time_bucket,
  IF(has_successful_cya AND NOT has_more_than_allowed_failures_cyas,1,0) AS cya_both_true_yes,
  IF(has_successful_cya AND NOT has_more_than_allowed_failures_cyas,0,1) AS cya_both_true_no,
  IF(has_successful_cya,1,0) AS cya_success_true_yes,
  IF(has_successful_cya,0,1) AS cya_success_true_no,
  IF(NOT has_more_than_allowed_failures_cyas,1,0) AS cya_failure_true_not,
  IF(NOT has_more_than_allowed_failures_cyas,0,1) AS cya_failure_true_yes,
  denver_date
FROM cte_idm_cya_experience
;

SELECT "\n\nFor 1: CYA Experience Calculated MOS\n\n";
INSERT OVERWRITE TABLE idm_quality_kpi_mos PARTITION (denver_date)
SELECT
  'Total' as timeframe,
  application_name,
  application_type,
  'idm_cya_mos' AS metric_name,
  AVG(score) AS score,
  AVG(cya_performance) AS metric_value,
  AVG(duration_bucket_filtered)/AVG(cya_performance) as duration_bucket_filtered_ratio,
  AVG(page_load_time_bucket_filtered)/AVG(cya_performance) as page_load_time_bucket_filtered_ratio,
  AVG(duration_bucket) as duration_bucket,
  AVG(page_load_time_bucket) as page_load_time_bucket,
  AVG(cya_performance)/(AVG(duration_bucket)+AVG(page_load_time_bucket)) as cya_both_derived,
  SUM(cya_both_true_yes)/SUM(cya_both_true_yes+cya_both_true_no) as cya_both,
  SUM(cya_success_true_yes)/SUM(cya_success_true_yes+cya_success_true_no) as cya_success,
  SUM(cya_failure_true_not)/SUM(cya_failure_true_not+cya_failure_true_yes) as cya_failure_not,
  0 AS vyi_both_derived,
  0 AS vyi_both,
  0 AS vyi_success,
  0 AS vyi_failure_not,
  0 AS cun_both_derived,
  0 AS cun_both,
  0 AS cun_success,
  0 AS cun_failure_not,
  0 AS rpw_both_derived,
  0 AS rpw_both,
  0 AS rpw_success,
  0 AS rpw_failure_not,
  'Confirm' AS flow,
  denver_date
FROM ${env:TMP_db}.idm_cya_experience
  GROUP BY
  application_name,
  application_type,
  denver_date
UNION ALL
SELECT
  'Total' as timeframe,
  application_name,
  application_type,
  'idm_cya_mos_visits' AS metric_name,
  AVG(score) AS score,
  COUNT(DISTINCT visit_id) AS metric_value,
  0 as duration_bucket_filtered_ratio,
  0 as page_load_time_bucket_filtered_ratio,
  0 as duration_bucket,
  0 as page_load_time_bucket,
  SUM(1) as cya_both_derived,
  SUM(cya_both_true_yes) as cya_both,
  SUM(cya_success_true_yes) as cya_success,
  SUM(cya_failure_true_not) as cya_failure_not,
  0 AS vyi_both_derived,
  0 AS vyi_both,
  0 AS vyi_success,
  0 AS vyi_failure_not,
  0 AS cun_both_derived,
  0 AS cun_both,
  0 AS cun_success,
  0 AS cun_failure_not,
  0 AS rpw_both_derived,
  0 AS rpw_both,
  0 AS rpw_success,
  0 AS rpw_failure_not,
  'Confirm' AS flow,
  denver_date
FROM ${env:TMP_db}.idm_cya_experience
  GROUP BY
  application_name,
  application_type,
  denver_date
;

---------------------
--------CREATE-------
---------------------
SET cun_allowed_failures = 1;
SET cun_transaction_duration_min_buckets = array(2,3,4,5);
SET cun_page_load_sec_buckets = array(2,4,10,20);
SET cun_weights_trans_then_page_load = array(0.5,0.5);


DROP TABLE IF EXISTS ${env:TMP_db}.idm_cun_experience_events PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.idm_cun_experience_events AS
WITH temp AS(
  SELECT DISTINCT
    t1.visit_id,
    t1.application_name,
    t1.application_version,
    t1.application_type,
    t1.mso,
    page_name_start,
    page_sequence_number_start,
    page_name_end,
    page_sequence_number_end,
    event,
    t1.message_timestamp AS message_timestamp_start,
    t2.message_timestamp AS message_timestamp_end,
    message_sequence_number,
    MAX(IF(`start` AND t1.message_timestamp < t2.message_timestamp, t1.message_timestamp, NULL)) OVER (PARTITION BY t1.visit_id,t2.message_timestamp) AS max_start_time,
    t1.denver_date
  FROM (
    SELECT
      TRUE AS `start`,
      visit_id,
      application_name,
      application_type,
      application_version,
      mso,
      page_name AS page_name_start,
      page_sequence_number AS page_sequence_number_start,
      message_timestamp,
      denver_date
    FROM ${env:TMP_db}.idm_core_events_base
    WHERE cun_status = 'start'
  ) AS t1
  JOIN (
    SELECT
      visit_id,
      application_name,
      application_type,
      mso,
      page_name AS page_name_end,
      page_sequence_number AS page_sequence_number_end,
      message_sequence_number,
      CASE
        WHEN cun_status = 'success' THEN 'cun_success'
        WHEN cun_status = 'fail' THEN 'cun_fail'
      END AS event,
      message_timestamp,
      denver_date
    FROM ${env:TMP_db}.idm_core_events_base
    WHERE cun_status IN ('success', 'fail')
  ) AS t2
  ON t1.visit_id = t2.visit_id
  AND t1.application_name = t2.application_name
)
SELECT *,
  (message_timestamp_end - message_timestamp_start)/1000/60 AS transaction_min
FROM temp
WHERE message_timestamp_start = max_start_time
;

DROP TABLE IF EXISTS ${env:TMP_db}.idm_cun_experience_pages;
CREATE TEMPORARY TABLE ${env:TMP_db}.idm_cun_experience_pages AS
SELECT
  t1.application_name,
  t2.application_version,
  t2.application_type,
  t1.visit_id,
  COUNT(DISTINCT IF(event='cun_fail',message_sequence_number,NULL)) AS number_of_failures,
  COUNT(DISTINCT IF(event='cun_success',message_sequence_number,NULL)) AS number_of_successes,
  AVG(DISTINCT transaction_min) AS avg_transaction_min,
  (
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end,cold_page_load_sec,NULL)
    ),0) +
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end,hot_page_load_sec,NULL)
    ),0)
  )/(
    COUNT(DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end, t1.page_sequence_number,NULL))
  ) AS page_load_time,
  MIN(t2.denver_date) denver_date
FROM (
  SELECT
    visit_id,
    page_name,
    application_name,
    IF(page_sequence_number IS NULL, -1, page_sequence_number) AS page_sequence_number,
    cold_page_load_sec,
    hot_page_load_sec,
    message_timestamp,
    denver_date
  FROM ${env:TMP_db}.idm_core_events_base
  WHERE message_name IN ('pageView','pageViewPerformance')
) AS t1
JOIN ${env:TMP_db}.idm_cun_experience_events AS t2
ON t1.visit_id = t2.visit_id
AND t1.application_name = t2.application_name
GROUP BY
  t1.application_name,
  t2.application_version,
  t2.application_type,
  t1.visit_id
HAVING MIN(t2.denver_date) = '${hiveconf:START_DATE}'
 ;

DROP TABLE IF EXISTS ${env:TMP_db}.idm_cun_experience;
CREATE TABLE `${env:TMP_db}.idm_cun_experience`(
  `application_name` string,
  `application_version` string,
  `application_type` string,
  `visit_id` string,
  `has_successful_cun` boolean,
  `has_more_than_allowed_failures_cuns` boolean,
  `number_of_successes` bigint,
  `number_of_failures` bigint,
  `page_load_time` double,
  `avg_transaction_min` double,
  `transaction_duration_min_bucket` int,
  `page_load_sec_bucket` int,
  `score` double,
  `cun_performance` double,
  `duration_bucket_filtered` double,
  `page_load_time_bucket_filtered` double,
  `duration_bucket` double,
  `page_load_time_bucket` double,
  `cun_both_true_yes` int,
  `cun_both_true_no` int,
  `cun_success_true_yes` int,
  `cun_success_true_no` int,
  `cun_failure_true_not` int,
  `cun_failure_true_yes` int,
  `denver_date` date)
;

WITH cte_idm_cun_experience AS (
  SELECT
    application_name,
    application_version,
    application_type,
    visit_id,
    IF(number_of_successes >= 1, TRUE, FALSE) AS has_successful_cun,
    IF(number_of_failures > ${hiveconf:cun_allowed_failures},TRUE,FALSE) AS has_more_than_allowed_failures_cuns,
    number_of_successes,
    number_of_failures,
    page_load_time,
    avg_transaction_min,
    CASE
      WHEN avg_transaction_min <= ${hiveconf:cun_transaction_duration_min_buckets}[0] THEN 8
      WHEN ${hiveconf:cun_transaction_duration_min_buckets}[0] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:cun_transaction_duration_min_buckets}[1] THEN 5
      WHEN ${hiveconf:cun_transaction_duration_min_buckets}[1] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:cun_transaction_duration_min_buckets}[2] THEN 3
      WHEN ${hiveconf:cun_transaction_duration_min_buckets}[2] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:cun_transaction_duration_min_buckets}[3] THEN 2
      WHEN ${hiveconf:cun_transaction_duration_min_buckets}[3] < avg_transaction_min THEN 1
    END AS transaction_duration_min_bucket,
    CASE
      WHEN page_load_time <= ${hiveconf:cun_page_load_sec_buckets}[0] THEN 8
      WHEN ${hiveconf:cun_page_load_sec_buckets}[0] < page_load_time AND page_load_time <= ${hiveconf:cun_page_load_sec_buckets}[1] THEN 5
      WHEN ${hiveconf:cun_page_load_sec_buckets}[1] < page_load_time AND page_load_time <= ${hiveconf:cun_page_load_sec_buckets}[2] THEN 3
      WHEN ${hiveconf:cun_page_load_sec_buckets}[2] < page_load_time AND page_load_time <= ${hiveconf:cun_page_load_sec_buckets}[3] THEN 2
      WHEN ${hiveconf:cun_page_load_sec_buckets}[3] < page_load_time THEN 1
  END AS page_load_sec_bucket,
  denver_date
  FROM ${env:TMP_db}.idm_cun_experience_pages
)
INSERT INTO TABLE ${env:TMP_db}.idm_cun_experience
SELECT
  application_name,
  application_version,
  application_type,
  visit_id,
  has_successful_cun,
  has_more_than_allowed_failures_cuns,
  number_of_successes,
  number_of_failures,
  page_load_time,
  avg_transaction_min,
  transaction_duration_min_bucket,
  page_load_sec_bucket,
  ((1/2*transaction_duration_min_bucket + 1/2*page_load_sec_bucket)/8) * if(has_successful_cun = true,1,0) * if(has_more_than_allowed_failures_cuns = false,1,0) as score,
  IF(
    has_successful_cun AND NOT has_more_than_allowed_failures_cuns,
    (${hiveconf:cun_weights_trans_then_page_load}[0]*transaction_duration_min_bucket+${hiveconf:cun_weights_trans_then_page_load}[1]*page_load_sec_bucket)/8,
    0
  ) AS cun_performance,
  IF(has_successful_cun AND NOT has_more_than_allowed_failures_cuns,
   ${hiveconf:cun_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8,0) AS duration_bucket_filtered,
  IF(has_successful_cun AND NOT has_more_than_allowed_failures_cuns,
   ${hiveconf:cun_weights_trans_then_page_load}[1]*page_load_sec_bucket/8,0) AS page_load_time_bucket_filtered,
  ${hiveconf:cun_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8 AS duration_bucket,
  ${hiveconf:cun_weights_trans_then_page_load}[1]*page_load_sec_bucket/8 AS page_load_time_bucket,
  IF(has_successful_cun AND NOT has_more_than_allowed_failures_cuns,1,0) AS cun_both_true_yes,
  IF(has_successful_cun AND NOT has_more_than_allowed_failures_cuns,0,1) AS cun_both_true_no,
  IF(has_successful_cun,1,0) AS cun_success_true_yes,
  IF(has_successful_cun,0,1) AS cun_success_true_no,
  IF(NOT has_more_than_allowed_failures_cuns,1,0) AS cun_failure_true_not,
  IF(NOT has_more_than_allowed_failures_cuns,0,1) AS cun_failure_true_yes,
  denver_date
FROM cte_idm_cun_experience
;

SELECT "\n\nFor 1: CUN Experience Calculated MOS\n\n";
INSERT INTO TABLE idm_quality_kpi_mos PARTITION (denver_date)
SELECT
  'Total' as timeframe,
  application_name,
  application_type,
  'idm_cun_mos' AS metric_name,
  AVG(score) AS score,
  AVG(cun_performance) AS metric_value,
  AVG(duration_bucket_filtered)/AVG(cun_performance) as duration_bucket_filtered_ratio,
  AVG(page_load_time_bucket_filtered)/AVG(cun_performance) as page_load_time_bucket_filtered_ratio,
  AVG(duration_bucket) as duration_bucket,
  AVG(page_load_time_bucket) as page_load_time_bucket,
  0 AS cya_both_derived,
  0 AS cya_both,
  0 AS cya_success,
  0 AS cya_failure_not,
  0 AS vyi_both_derived,
  0 AS vyi_both,
  0 AS vyi_success,
  0 AS vyi_failure_not,
  AVG(cun_performance)/(AVG(duration_bucket)+AVG(page_load_time_bucket)) as cun_both_derived,
  SUM(cun_both_true_yes)/SUM(cun_both_true_yes+cun_both_true_no) as cun_both,
  SUM(cun_success_true_yes)/SUM(cun_success_true_yes+cun_success_true_no) as cun_success,
  SUM(cun_failure_true_not)/SUM(cun_failure_true_not+cun_failure_true_yes) as cun_failure_not,
  0 AS rpw_both_derived,
  0 AS rpw_both,
  0 AS rpw_success,
  0 AS rpw_failure_not,
  'Create' AS flow,
  denver_date
FROM ${env:TMP_db}.idm_cun_experience
  GROUP BY
  application_name,
  application_type,
  denver_date
UNION ALL
SELECT
  'Total' as timeframe,
  application_name,
  application_type,
  'idm_cun_mos_visits' AS metric_name,
  AVG(score) AS score,
  COUNT(DISTINCT visit_id) AS metric_value,
  0 as duration_bucket_filtered_ratio,
  0 as page_load_time_bucket_filtered_ratio,
  0 as duration_bucket,
  0 as page_load_time_bucket,
  0 AS cya_both_derived,
  0 AS cya_both,
  0 AS cya_success,
  0 AS cya_failure_not,
  0 AS vyi_both_derived,
  0 AS vyi_both,
  0 AS vyi_success,
  0 AS vyi_failure_not,
  SUM(1) as cun_both_derived,
  SUM(cun_both_true_yes) as cun_both,
  SUM(cun_success_true_yes) as cun_success,
  SUM(cun_failure_true_not) as cun_failure_not,
  0 AS rpw_both_derived,
  0 AS rpw_both,
  0 AS rpw_success,
  0 AS rpw_failure_not,
  'Create' AS flow,
  denver_date
FROM ${env:TMP_db}.idm_cun_experience
  GROUP BY
  application_name,
  application_type,
  denver_date
;

---------------------
--------RESET--------
---------------------
SET rpw_allowed_failures = 1;
SET rpw_transaction_duration_min_buckets = array(2,3,4,5);
SET rpw_page_load_sec_buckets = array(2,4,10,20);
SET rpw_weights_trans_then_page_load = array(0.5,0.5);

DROP TABLE IF EXISTS ${env:TMP_db}.idm_rpw_experience_events;
CREATE TEMPORARY TABLE ${env:TMP_db}.idm_rpw_experience_events AS
WITH temp AS(
  SELECT DISTINCT
    t1.visit_id,
    t1.application_name,
    t1.application_version,
    t1.application_type,
    t1.mso,
    page_name_start,
    page_sequence_number_start,
    page_name_end,
    page_sequence_number_end,
    event,
    t1.message_timestamp AS message_timestamp_start,
    t2.message_timestamp AS message_timestamp_end,
    message_sequence_number,
    MAX(IF(`start` AND t1.message_timestamp < t2.message_timestamp, t1.message_timestamp, NULL)) OVER (PARTITION BY t1.visit_id,t2.message_timestamp) AS max_start_time,
    t1.denver_date
  FROM (
    SELECT
      TRUE AS `start`,
      visit_id,
      application_name,
      application_type,
      application_version,
      mso,
      page_name AS page_name_start,
      page_sequence_number AS page_sequence_number_start,
      message_timestamp,
      denver_date
    FROM ${env:TMP_db}.idm_core_events_base
    WHERE rpw_status = 'start'
  ) AS t1
  JOIN (
    SELECT
      visit_id,
      application_name,
      application_type,
      mso,
      page_name AS page_name_end,
      page_sequence_number AS page_sequence_number_end,
      message_sequence_number,
      CASE
        WHEN rpw_status = 'success' THEN 'rpw_success'
        WHEN rpw_status = 'fail' THEN 'rpw_fail'
      END AS event,
      message_timestamp,
      denver_date
    FROM ${env:TMP_db}.idm_core_events_base
    WHERE rpw_status IN ('success', 'fail')
  ) AS t2
  ON t1.visit_id = t2.visit_id
  AND t1.application_name = t2.application_name
)
SELECT *,
  (message_timestamp_end - message_timestamp_start)/1000/60 AS transaction_min
FROM temp
WHERE message_timestamp_start = max_start_time
;

DROP TABLE IF EXISTS ${env:TMP_db}.idm_rpw_experience_pages;
CREATE TEMPORARY TABLE ${env:TMP_db}.idm_rpw_experience_pages AS
SELECT
  t1.application_name,
  t2.application_version,
  t2.application_type,
  t1.visit_id,
  COUNT(DISTINCT IF(event='rpw_fail',message_sequence_number,NULL)) AS number_of_failures,
  COUNT(DISTINCT IF(event='rpw_success',message_sequence_number,NULL)) AS number_of_successes,
  AVG(DISTINCT transaction_min) AS avg_transaction_min,
  (
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end,cold_page_load_sec,NULL)
    ),0) +
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end,hot_page_load_sec,NULL)
    ),0)
  )/(
    COUNT(DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end, t1.page_sequence_number,NULL))
  ) AS page_load_time,
  MIN(t2.denver_date) denver_date
FROM (
  SELECT
    visit_id,
    page_name,
    application_name,
    IF(page_sequence_number IS NULL, -1, page_sequence_number) AS page_sequence_number,
    cold_page_load_sec,
    hot_page_load_sec,
    message_timestamp,
    denver_date
  FROM ${env:TMP_db}.idm_core_events_base
  WHERE message_name IN ('pageView','pageViewPerformance')
) AS t1
JOIN ${env:TMP_db}.idm_rpw_experience_events AS t2
ON t1.visit_id = t2.visit_id
AND t1.application_name = t2.application_name
GROUP BY
  t1.application_name,
  t2.application_version,
  t2.application_type,
  t1.visit_id
HAVING MIN(t2.denver_date) = '${hiveconf:START_DATE}'
 ;

DROP TABLE IF EXISTS ${env:TMP_db}.idm_rpw_experience;
CREATE TABLE `${env:TMP_db}.idm_rpw_experience`(
  `application_name` string,
  `application_version` string,
  `application_type` string,
  `visit_id` string,
  `has_successful_rpw` boolean,
  `has_more_than_allowed_failures_rpws` boolean,
  `number_of_successes` bigint,
  `number_of_failures` bigint,
  `page_load_time` double,
  `avg_transaction_min` double,
  `transaction_duration_min_bucket` int,
  `page_load_sec_bucket` int,
  `score` double,
  `rpw_performance` double,
  `duration_bucket_filtered` double,
  `page_load_time_bucket_filtered` double,
  `duration_bucket` double,
  `page_load_time_bucket` double,
  `rpw_both_true_yes` int,
  `rpw_both_true_no` int,
  `rpw_success_true_yes` int,
  `rpw_success_true_no` int,
  `rpw_failure_true_not` int,
  `rpw_failure_true_yes` int,
  `denver_date` date)
;

WITH cte_idm_rpw_experience AS (
  SELECT
    application_name,
    application_version,
    application_type,
    visit_id,
    IF(number_of_successes >= 1, TRUE, FALSE) AS has_successful_rpw,
    IF(number_of_failures > ${hiveconf:rpw_allowed_failures},TRUE,FALSE) AS has_more_than_allowed_failures_rpws,
    number_of_successes,
    number_of_failures,
    page_load_time,
    avg_transaction_min,
    CASE
      WHEN avg_transaction_min <= ${hiveconf:rpw_transaction_duration_min_buckets}[0] THEN 8
      WHEN ${hiveconf:rpw_transaction_duration_min_buckets}[0] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:rpw_transaction_duration_min_buckets}[1] THEN 5
      WHEN ${hiveconf:rpw_transaction_duration_min_buckets}[1] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:rpw_transaction_duration_min_buckets}[2] THEN 3
      WHEN ${hiveconf:rpw_transaction_duration_min_buckets}[2] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:rpw_transaction_duration_min_buckets}[3] THEN 2
      WHEN ${hiveconf:rpw_transaction_duration_min_buckets}[3] < avg_transaction_min THEN 1
    END AS transaction_duration_min_bucket,
    CASE
      WHEN page_load_time <= ${hiveconf:rpw_page_load_sec_buckets}[0] THEN 8
      WHEN ${hiveconf:rpw_page_load_sec_buckets}[0] < page_load_time AND page_load_time <= ${hiveconf:rpw_page_load_sec_buckets}[1] THEN 5
      WHEN ${hiveconf:rpw_page_load_sec_buckets}[1] < page_load_time AND page_load_time <= ${hiveconf:rpw_page_load_sec_buckets}[2] THEN 3
      WHEN ${hiveconf:rpw_page_load_sec_buckets}[2] < page_load_time AND page_load_time <= ${hiveconf:rpw_page_load_sec_buckets}[3] THEN 2
      WHEN ${hiveconf:rpw_page_load_sec_buckets}[3] < page_load_time THEN 1
  END AS page_load_sec_bucket,
  denver_date
  FROM ${env:TMP_db}.idm_rpw_experience_pages
)
INSERT INTO TABLE ${env:TMP_db}.idm_rpw_experience
SELECT
  application_name,
  application_version,
  application_type,
  visit_id,
  has_successful_rpw,
  has_more_than_allowed_failures_rpws,
  number_of_successes,
  number_of_failures,
  page_load_time,
  avg_transaction_min,
  transaction_duration_min_bucket,
  page_load_sec_bucket,
  ((1/2*transaction_duration_min_bucket + 1/2*page_load_sec_bucket)/8) * if(has_successful_rpw = true,1,0) * if(has_more_than_allowed_failures_rpws = false,1,0) as score,
  IF(
    has_successful_rpw AND NOT has_more_than_allowed_failures_rpws,
    (${hiveconf:rpw_weights_trans_then_page_load}[0]*transaction_duration_min_bucket+${hiveconf:rpw_weights_trans_then_page_load}[1]*page_load_sec_bucket)/8,
    0
  ) AS rpw_performance,
  IF(has_successful_rpw AND NOT has_more_than_allowed_failures_rpws,
   ${hiveconf:rpw_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8,0) AS duration_bucket_filtered,
  IF(has_successful_rpw AND NOT has_more_than_allowed_failures_rpws,
   ${hiveconf:rpw_weights_trans_then_page_load}[1]*page_load_sec_bucket/8,0) AS page_load_time_bucket_filtered,
  ${hiveconf:rpw_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8 AS duration_bucket,
  ${hiveconf:rpw_weights_trans_then_page_load}[1]*page_load_sec_bucket/8 AS page_load_time_bucket,
  IF(has_successful_rpw AND NOT has_more_than_allowed_failures_rpws,1,0) AS rpw_both_true_yes,
  IF(has_successful_rpw AND NOT has_more_than_allowed_failures_rpws,0,1) AS rpw_both_true_no,
  IF(has_successful_rpw,1,0) AS rpw_success_true_yes,
  IF(has_successful_rpw,0,1) AS rpw_success_true_no,
  IF(NOT has_more_than_allowed_failures_rpws,1,0) AS rpw_failure_true_not,
  IF(NOT has_more_than_allowed_failures_rpws,0,1) AS rpw_failure_true_yes,
  denver_date
FROM cte_idm_rpw_experience
;

SELECT "\n\nFor 1: RPW Experience Calculated MOS\n\n";
INSERT INTO TABLE idm_quality_kpi_mos PARTITION (denver_date)
SELECT
  'Total' as timeframe,
  application_name,
  application_type,
  'idm_rpw_mos' AS metric_name,
  AVG(score) AS score,
  AVG(rpw_performance) AS metric_value,
  AVG(duration_bucket_filtered)/AVG(rpw_performance) as duration_bucket_filtered_ratio,
  AVG(page_load_time_bucket_filtered)/AVG(rpw_performance) as page_load_time_bucket_filtered_ratio,
  AVG(duration_bucket) as duration_bucket,
  AVG(page_load_time_bucket) as page_load_time_bucket,
  0 AS cya_both_derived,
  0 AS cya_both,
  0 AS cya_success,
  0 AS cya_failure_not,
  0 AS vyi_both_derived,
  0 AS vyi_both,
  0 AS vyi_success,
  0 AS vyi_failure_not,
  0 AS cun_both_derived,
  0 AS cun_both,
  0 AS cun_success,
  0 AS cun_failure_not,
  AVG(rpw_performance)/(AVG(duration_bucket)+AVG(page_load_time_bucket)) as rpw_both_derived,
  SUM(rpw_both_true_yes)/SUM(rpw_both_true_yes+rpw_both_true_no) as rpw_both,
  SUM(rpw_success_true_yes)/SUM(rpw_success_true_yes+rpw_success_true_no) as rpw_success,
  SUM(rpw_failure_true_not)/SUM(rpw_failure_true_not+rpw_failure_true_yes) as rpw_failure_not,
  'Reset' AS flow,
  denver_date
FROM ${env:TMP_db}.idm_rpw_experience
  GROUP BY
  application_name,
  application_type,
  denver_date
UNION ALL
SELECT
  'Total' as timeframe,
  application_name,
  application_type,
  'idm_rpw_mos_visits' AS metric_name,
  AVG(score) AS score,
  COUNT(DISTINCT visit_id) AS metric_value,
  0 as duration_bucket_filtered_ratio,
  0 as page_load_time_bucket_filtered_ratio,
  0 as duration_bucket,
  0 as page_load_time_bucket,
  0 AS cya_both_derived,
  0 AS cya_both,
  0 AS cya_success,
  0 AS cya_failure_not,
  0 AS vyi_both_derived,
  0 AS vyi_both,
  0 AS vyi_success,
  0 AS vyi_failure_not,
  0 AS cun_both_derived,
  0 AS cun_both,
  0 AS cun_success,
  0 AS cun_failure_not,
  SUM(1) as rpw_both_derived,
  SUM(rpw_both_true_yes) as rpw_both,
  SUM(rpw_success_true_yes) as rpw_success,
  SUM(rpw_failure_true_not) as rpw_failure_not,
  'Reset' AS flow,
  denver_date
FROM ${env:TMP_db}.idm_rpw_experience
  GROUP BY
  application_name,
  application_type,
  denver_date
;

---------------------
--------VERIFY-------
---------------------
SET vyi_allowed_failures = 1;
SET vyi_transaction_duration_min_buckets = array(2,3,4,5);
SET vyi_page_load_sec_buckets = array(2,4,10,20);
SET vyi_weights_trans_then_page_load = array(0.5,0.5);


DROP TABLE IF EXISTS ${env:TMP_db}.idm_vyi_experience_events;
CREATE TEMPORARY TABLE ${env:TMP_db}.idm_vyi_experience_events AS
WITH temp AS(
  SELECT DISTINCT
    t1.visit_id,
    t1.application_name,
    t1.application_version,
    t1.application_type,
    t1.mso,
    page_name_start,
    page_sequence_number_start,
    page_name_end,
    page_sequence_number_end,
    event,
    t1.message_timestamp AS message_timestamp_start,
    t2.message_timestamp AS message_timestamp_end,
    message_sequence_number,
    MAX(IF(`start` AND t1.message_timestamp < t2.message_timestamp, t1.message_timestamp, NULL)) OVER (PARTITION BY t1.visit_id,t2.message_timestamp) AS max_start_time,
    t1.denver_date
  FROM (
    SELECT
      TRUE AS `start`,
      visit_id,
      application_name,
      application_type,
      application_version,
      mso,
      page_name AS page_name_start,
      page_sequence_number AS page_sequence_number_start,
      message_timestamp,
      denver_date
    FROM ${env:TMP_db}.idm_core_events_base
    WHERE vyi_status = 'start'
  ) AS t1
  JOIN (
    SELECT
      visit_id,
      application_name,
      application_type,
      mso,
      page_name AS page_name_end,
      page_sequence_number AS page_sequence_number_end,
      message_sequence_number,
      CASE
        WHEN vyi_status = 'success' THEN 'vyi_success'
        WHEN vyi_status = 'fail' THEN 'vyi_fail'
      END AS event,
      message_timestamp,
      denver_date
    FROM ${env:TMP_db}.idm_core_events_base
    WHERE vyi_status IN ('success', 'fail')
  ) AS t2
  ON t1.visit_id = t2.visit_id
  AND t1.application_name = t2.application_name
)
SELECT *,
  (message_timestamp_end - message_timestamp_start)/1000/60 AS transaction_min
FROM temp
WHERE message_timestamp_start = max_start_time
;

DROP TABLE IF EXISTS ${env:TMP_db}.idm_vyi_experience_pages;
CREATE TEMPORARY TABLE ${env:TMP_db}.idm_vyi_experience_pages AS
SELECT
  t1.application_name,
  t2.application_version,
  t2.application_type,
  t1.visit_id,
  COUNT(DISTINCT IF(event='vyi_fail',message_sequence_number,NULL)) AS number_of_failures,
  COUNT(DISTINCT IF(event='vyi_success',message_sequence_number,NULL)) AS number_of_successes,
  AVG(DISTINCT transaction_min) AS avg_transaction_min,
  (
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end,cold_page_load_sec,NULL)
    ),0) +
    NVL(SUM(
      DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end,hot_page_load_sec,NULL)
    ),0)
  )/(
    COUNT(DISTINCT IF(t2.page_sequence_number_start <= t1.page_sequence_number AND t1.page_sequence_number <= t2.page_sequence_number_end AND t1.message_timestamp <= t2.message_timestamp_end, t1.page_sequence_number,NULL))
  ) AS page_load_time,
  MIN(t2.denver_date) denver_date
FROM (
  SELECT
    visit_id,
    page_name,
    application_name,
    IF(page_sequence_number IS NULL, -1, page_sequence_number) AS page_sequence_number,
    cold_page_load_sec,
    hot_page_load_sec,
    message_timestamp,
    denver_date
  FROM ${env:TMP_db}.idm_core_events_base
  WHERE message_name IN ('pageView','pageViewPerformance')
) AS t1
JOIN ${env:TMP_db}.idm_vyi_experience_events AS t2
ON t1.visit_id = t2.visit_id
AND t1.application_name = t2.application_name
GROUP BY
  t1.application_name,
  t2.application_version,
  t2.application_type,
  t1.visit_id
HAVING MIN(t2.denver_date) = '${hiveconf:START_DATE}'
 ;

DROP TABLE IF EXISTS ${env:TMP_db}.idm_vyi_experience;
CREATE TABLE `${env:TMP_db}.idm_vyi_experience`(
  `application_name` string,
  `application_version` string,
  `application_type` string,
  `visit_id` string,
  `has_successful_vyi` boolean,
  `has_more_than_allowed_failures_vyis` boolean,
  `number_of_successes` bigint,
  `number_of_failures` bigint,
  `page_load_time` double,
  `avg_transaction_min` double,
  `transaction_duration_min_bucket` int,
  `page_load_sec_bucket` int,
  `score` double,
  `vyi_performance` double,
  `duration_bucket_filtered` double,
  `page_load_time_bucket_filtered` double,
  `duration_bucket` double,
  `page_load_time_bucket` double,
  `vyi_both_true_yes` int,
  `vyi_both_true_no` int,
  `vyi_success_true_yes` int,
  `vyi_success_true_no` int,
  `vyi_failure_true_not` int,
  `vyi_failure_true_yes` int,
  `denver_date` date)
;

WITH cte_idm_vyi_experience AS (
  SELECT
    application_name,
    application_version,
    application_type,
    visit_id,
    IF(number_of_successes >= 1, TRUE, FALSE) AS has_successful_vyi,
    IF(number_of_failures > ${hiveconf:vyi_allowed_failures},TRUE,FALSE) AS has_more_than_allowed_failures_vyis,
    number_of_successes,
    number_of_failures,
    page_load_time,
    avg_transaction_min,
    CASE
      WHEN avg_transaction_min <= ${hiveconf:vyi_transaction_duration_min_buckets}[0] THEN 8
      WHEN ${hiveconf:vyi_transaction_duration_min_buckets}[0] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:vyi_transaction_duration_min_buckets}[1] THEN 5
      WHEN ${hiveconf:vyi_transaction_duration_min_buckets}[1] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:vyi_transaction_duration_min_buckets}[2] THEN 3
      WHEN ${hiveconf:vyi_transaction_duration_min_buckets}[2] < avg_transaction_min AND avg_transaction_min <= ${hiveconf:vyi_transaction_duration_min_buckets}[3] THEN 2
      WHEN ${hiveconf:vyi_transaction_duration_min_buckets}[3] < avg_transaction_min THEN 1
    END AS transaction_duration_min_bucket,
    CASE
      WHEN page_load_time <= ${hiveconf:vyi_page_load_sec_buckets}[0] THEN 8
      WHEN ${hiveconf:vyi_page_load_sec_buckets}[0] < page_load_time AND page_load_time <= ${hiveconf:vyi_page_load_sec_buckets}[1] THEN 5
      WHEN ${hiveconf:vyi_page_load_sec_buckets}[1] < page_load_time AND page_load_time <= ${hiveconf:vyi_page_load_sec_buckets}[2] THEN 3
      WHEN ${hiveconf:vyi_page_load_sec_buckets}[2] < page_load_time AND page_load_time <= ${hiveconf:vyi_page_load_sec_buckets}[3] THEN 2
      WHEN ${hiveconf:vyi_page_load_sec_buckets}[3] < page_load_time THEN 1
  END AS page_load_sec_bucket,
  denver_date
  FROM ${env:TMP_db}.idm_vyi_experience_pages
)
INSERT INTO TABLE ${env:TMP_db}.idm_vyi_experience
SELECT
  application_name,
  application_version,
  application_type,
  visit_id,
  has_successful_vyi,
  has_more_than_allowed_failures_vyis,
  number_of_successes,
  number_of_failures,
  page_load_time,
  avg_transaction_min,
  transaction_duration_min_bucket,
  page_load_sec_bucket,
  ((1/2*transaction_duration_min_bucket + 1/2*page_load_sec_bucket)/8) * if(has_successful_vyi = true,1,0) * if(has_more_than_allowed_failures_vyis = false,1,0) as score,
  IF(
    has_successful_vyi AND NOT has_more_than_allowed_failures_vyis,
    (${hiveconf:vyi_weights_trans_then_page_load}[0]*transaction_duration_min_bucket+${hiveconf:vyi_weights_trans_then_page_load}[1]*page_load_sec_bucket)/8,
    0
  ) AS vyi_performance,
  IF(has_successful_vyi AND NOT has_more_than_allowed_failures_vyis,
   ${hiveconf:vyi_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8,0) AS duration_bucket_filtered,
  IF(has_successful_vyi AND NOT has_more_than_allowed_failures_vyis,
   ${hiveconf:vyi_weights_trans_then_page_load}[1]*page_load_sec_bucket/8,0) AS page_load_time_bucket_filtered,
  ${hiveconf:vyi_weights_trans_then_page_load}[0]*transaction_duration_min_bucket/8 AS duration_bucket,
  ${hiveconf:vyi_weights_trans_then_page_load}[1]*page_load_sec_bucket/8 AS page_load_time_bucket,
  IF(has_successful_vyi AND NOT has_more_than_allowed_failures_vyis,1,0) AS vyi_both_true_yes,
  IF(has_successful_vyi AND NOT has_more_than_allowed_failures_vyis,0,1) AS vyi_both_true_no,
  IF(has_successful_vyi,1,0) AS vyi_success_true_yes,
  IF(has_successful_vyi,0,1) AS vyi_success_true_no,
  IF(NOT has_more_than_allowed_failures_vyis,1,0) AS vyi_failure_true_not,
  IF(NOT has_more_than_allowed_failures_vyis,0,1) AS vyi_failure_true_yes,
  denver_date
FROM cte_idm_vyi_experience
;

SELECT "\n\nFor 1: VYI Experience Calculated MOS\n\n";
INSERT INTO TABLE idm_quality_kpi_mos PARTITION (denver_date)
SELECT
  'Total' as timeframe,
  application_name,
  application_type,
  'idm_vyi_mos' AS metric_name,
  AVG(score) AS score,
  AVG(vyi_performance) AS metric_value,
  AVG(duration_bucket_filtered)/AVG(vyi_performance) as duration_bucket_filtered_ratio,
  AVG(page_load_time_bucket_filtered)/AVG(vyi_performance) as page_load_time_bucket_filtered_ratio,
  AVG(duration_bucket) as duration_bucket,
  AVG(page_load_time_bucket) as page_load_time_bucket,
  0 AS cya_both_derived,
  0 AS cya_both,
  0 AS cya_success,
  0 AS cya_failure_not,
  AVG(vyi_performance)/(AVG(duration_bucket)+AVG(page_load_time_bucket)) as vyi_both_derived,
  SUM(vyi_both_true_yes)/SUM(vyi_both_true_yes+vyi_both_true_no) as vyi_both,
  SUM(vyi_success_true_yes)/SUM(vyi_success_true_yes+vyi_success_true_no) as vyi_success,
  SUM(vyi_failure_true_not)/SUM(vyi_failure_true_not+vyi_failure_true_yes) as vyi_failure_not,
  0 AS cun_both_derived,
  0 AS cun_both,
  0 AS cun_success,
  0 AS cun_failure_not,
  0 AS rpw_both_derived,
  0 AS rpw_both,
  0 AS rpw_success,
  0 AS rpw_failure_not,
  'Verify' AS flow,
  denver_date
FROM ${env:TMP_db}.idm_vyi_experience
  GROUP BY
  application_name,
  application_type,
  denver_date
UNION ALL
SELECT
  'Total' as timeframe,
  application_name,
  application_type,
  'idm_vyi_mos_visits' AS metric_name,
  AVG(score) AS score,
  COUNT(DISTINCT visit_id) AS metric_value,
  0 as duration_bucket_filtered_ratio,
  0 as page_load_time_bucket_filtered_ratio,
  0 as duration_bucket,
  0 as page_load_time_bucket,
  0 AS cya_both_derived,
  0 AS cya_both,
  0 AS cya_success,
  0 AS cya_failure_not,
  SUM(1) as vyi_both_derived,
  SUM(vyi_both_true_yes) as vyi_both,
  SUM(vyi_success_true_yes) as vyi_success,
  SUM(vyi_failure_true_not) as vyi_failure_not,
  0 AS cun_both_derived,
  0 AS cun_both,
  0 AS cun_success,
  0 AS cun_failure_not,
  0 AS rpw_both_derived,
  0 AS rpw_both,
  0 AS rpw_success,
  0 AS rpw_failure_not,
  'Verify' AS flow,
  denver_date
FROM ${env:TMP_db}.idm_vyi_experience
  GROUP BY
  application_name,
  application_type,
  denver_date
;

INSERT OVERWRITE TABLE idm_quality_visit_agg PARTITION (denver_date)
SELECT
  COALESCE(cya.visit_id,cun.visit_id,rpw.visit_id,vyi.visit_id) AS visit_id,

  MAX(COALESCE(cya.application_name,cun.application_name,rpw.application_name,vyi.application_name)) application_name,
  MAX(COALESCE(cya.application_version,cun.application_version,rpw.application_version,vyi.application_version)) application_version,
  MAX(COALESCE(cya.application_type,cun.application_type,rpw.application_type,vyi.application_type)) application_type,
  MAX(cya_performance) AS confirm_score,
  MAX(cya.transaction_duration_min_bucket) AS confirm_transaction_duration_bucket,
  MAX(cya.page_load_sec_bucket) AS confirm_page_load_bucket,
  MAX(IF(cya.has_more_than_allowed_failures_cyas,0,1)) confirm_fail_bucket,
  MAX(IF(cya.has_successful_cya,1,0)) confirm_success_bucket,
  MAX(${hiveconf:cya_weights_trans_then_page_load}[0]*cya.transaction_duration_min_bucket/8) AS confirm_transaction_duration_bucket_score,
  MAX(${hiveconf:cya_weights_trans_then_page_load}[1]*cya.page_load_sec_bucket/8) AS confirm_page_load_bucket_score,
  MAX(cya.number_of_successes) AS confirm_success_count,
  MAX(cya.number_of_failures) AS confirm_fail_count,
  MAX(cya.avg_transaction_min) AS confirm_avg_transaction_min,
  MAX(cya.page_load_time) AS confirm_avg_page_load_sec,
  MAX(cya.has_successful_cya) AS confirm_has_success,
  MAX(cya.has_more_than_allowed_failures_cyas) AS confirm_more_than_allowed_fails,

  MAX(cun_performance) AS create_score,
  MAX(cun.transaction_duration_min_bucket) AS create_transaction_duration_bucket,
  MAX(cun.page_load_sec_bucket) AS create_page_load_bucket,
  MAX(IF(cun.has_more_than_allowed_failures_cuns,0,1)) create_fail_bucket,
  MAX(IF(cun.has_successful_cun,1,0)) create_success_bucket,
  MAX(${hiveconf:cun_weights_trans_then_page_load}[0]*cun.transaction_duration_min_bucket/8) AS create_transaction_duration_bucket_score,
  MAX(${hiveconf:cun_weights_trans_then_page_load}[1]*cun.page_load_sec_bucket/8) AS create_page_load_bucket_score,
  MAX(cun.number_of_successes) AS create_success_count,
  MAX(cun.number_of_failures) AS create_fail_count,
  MAX(cun.avg_transaction_min) AS create_avg_transaction_min,
  MAX(cun.page_load_time) AS create_avg_page_load_sec,
  MAX(cun.has_successful_cun) AS create_has_success,
  MAX(cun.has_more_than_allowed_failures_cuns) AS create_more_than_allowed_fails,

  MAX(rpw_performance) AS reset_score,
  MAX(rpw.transaction_duration_min_bucket) AS reset_transaction_duration_bucket,
  MAX(rpw.page_load_sec_bucket) AS reset_page_load_bucket,
  MAX(IF(rpw.has_more_than_allowed_failures_rpws,0,1)) reset_fail_bucket,
  MAX(IF(rpw.has_successful_rpw,1,0)) reset_success_bucket,
  MAX(${hiveconf:rpw_weights_trans_then_page_load}[0]*rpw.transaction_duration_min_bucket/8) AS reset_transaction_duration_bucket_score,
  MAX(${hiveconf:rpw_weights_trans_then_page_load}[1]*rpw.page_load_sec_bucket/8) AS reset_page_load_bucket_score,
  MAX(rpw.number_of_successes) AS reset_success_count,
  MAX(rpw.number_of_failures) AS reset_fail_count,
  MAX(rpw.avg_transaction_min) AS reset_avg_transaction_min,
  MAX(rpw.page_load_time) AS reset_avg_page_load_sec,
  MAX(rpw.has_successful_rpw) AS reset_has_success,
  MAX(rpw.has_more_than_allowed_failures_rpws) AS reset_more_than_allowed_fails,

  MAX(vyi_performance) AS verify_score,
  MAX(vyi.transaction_duration_min_bucket) AS verify_transaction_duration_bucket,
  MAX(vyi.page_load_sec_bucket) AS verify_page_load_bucket,
  MAX(IF(vyi.has_more_than_allowed_failures_vyis,0,1)) verify_fail_bucket,
  MAX(IF(vyi.has_successful_vyi,1,0)) verify_success_bucket,
  MAX(${hiveconf:vyi_weights_trans_then_page_load}[0]*vyi.transaction_duration_min_bucket/8) AS verify_transaction_duration_bucket_score,
  MAX(${hiveconf:vyi_weights_trans_then_page_load}[1]*vyi.page_load_sec_bucket/8) AS verify_page_load_bucket_score,
  MAX(vyi.number_of_successes) AS verify_success_count,
  MAX(vyi.number_of_failures) AS verify_fail_count,
  MAX(vyi.avg_transaction_min) AS verify_avg_transaction_min,
  MAX(vyi.page_load_time) AS verify_avg_page_load_sec,
  MAX(vyi.has_successful_vyi) AS verify_has_success,
  MAX(vyi.has_more_than_allowed_failures_vyis) AS verify_more_than_allowed_fails,
  COALESCE(cya.denver_date,cun.denver_date,rpw.denver_date,vyi.denver_date) denver_date
FROM ${env:TMP_db}.idm_cya_experience cya
FULL OUTER JOIN ${env:TMP_db}.idm_cun_experience cun ON cya.visit_id = cun.visit_id
                                                     AND cya.denver_date = cun.denver_date
FULL OUTER JOIN ${env:TMP_db}.idm_vyi_experience vyi ON cya.visit_id = vyi.visit_id
                                                     AND cya.denver_date = vyi.denver_date
FULL OUTER JOIN ${env:TMP_db}.idm_rpw_experience rpw ON cya.visit_id = rpw.visit_id
                                                     AND cya.denver_date = rpw.denver_date
WHERE COALESCE(cya.denver_date,cun.denver_date,rpw.denver_date,vyi.denver_date) IS NOT NULL
GROUP BY
  COALESCE(cya.visit_id,cun.visit_id,rpw.visit_id,vyi.visit_id),
  COALESCE(cya.denver_date,cun.denver_date,rpw.denver_date,vyi.denver_date)
;

SET hive.merge.tezfiles=true;

INSERT OVERWRITE TABLE idm_quality_kpi_mos PARTITION (denver_date)
select *
from idm_quality_kpi_mos
where denver_date in (select distinct denver_date from ${env:TMP_db}.idm_core_events_base);

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.idm_core_events_base       PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_cun_experience         PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_cun_experience_events  PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_cun_experience_pages   PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_cya_experience         PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_cya_experience_events  PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_cya_experience_pages   PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_rpw_experience         PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_rpw_experience_events  PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_rpw_experience_pages   PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_vyi_experience         PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_vyi_experience_events  PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.idm_vyi_experience_pages   PURGE;
