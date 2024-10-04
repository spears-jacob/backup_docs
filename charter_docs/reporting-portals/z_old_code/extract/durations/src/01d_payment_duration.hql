USE ${env:ENVIRONMENT}
;

------------------------------------------------------------
--asp_duration_raw
--STEP ONE:
--Populate raw duration table with all page views in payment workflows from asp_v_venona_portals
------------------------------------------------------------

------------------------------------------------------------
--PART ONE:
--Populate with production suite data
------------------------------------------------------------

SELECT '***** BEGIN STEP ONE: asp_metrics_payment_duration_raw ******'
;

INSERT OVERWRITE TABLE asp_duration_raw PARTITION (date_denver,source_table)
SELECT
  visit__visit_id as visit_id,
  COALESCE(state__view__current_page__elements__standardized_name, message__feature__feature_name) AS page_name,
  received__timestamp as ts_ms,
  message__name as message_name,
  message__feature__feature_name as message_feature_name,
  message__feature__feature_step_name as message_feature_step_name,
  operation__success,
  prod.epoch_converter(received__timestamp) as date_denver,
  CASE
    WHEN visit__application_details__application_name = 'MySpectrum' THEN 'asp_v_venona_events_portals_msa'
    WHEN LOWER(visit__application_details__application_name) = 'specnet' THEN 'asp_v_venona_events_portals_specnet'
    WHEN LOWER(visit__application_details__application_name) =  'smb' THEN 'asp_v_venona_events_portals_smb'
    ELSE 'UNKNOWN'
  ENd AS source_table
FROM asp_v_venona_events_portals
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc <  '${env:END_DATE_TZ}' )
   AND message__name In('selectAction','featureStop')
;


SELECT '***** END PART ONE: Populate with production suite data ******'
;

------------------------------------------------------------
--PART TWO:
--Populate with dev suite data
------------------------------------------------------------
INSERT OVERWRITE TABLE asp_duration_raw PARTITION (date_denver,source_table)
SELECT
  visit__visit_id as visit_id,
  COALESCE(state__view__current_page__page_name, message__feature__feature_name) AS page_name,
  received__timestamp as ts_ms,
  message__name as message_name,
  message__feature__feature_name as message_feature_name,
  message__feature__feature_step_name as message_feature_step_name,
  operation__success,
  prod.epoch_converter(received__timestamp) as date_denver,
  CASE
    WHEN visit__application_details__application_name = 'MySpectrum' THEN 'asp_v_venona_staging_portals_msa'
    WHEN LOWER(visit__application_details__application_name) = 'specnet' THEN 'asp_v_venona_staging_portals_specnet'
    WHEN LOWER(visit__application_details__application_name) =  'smb' THEN 'asp_v_venona_staging_portals_smb'
    ELSE 'UNKNOWN'
  ENd AS source_table
FROM asp_v_venona_staging_portals
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc <  '${env:END_DATE_TZ}' )
   AND message__name In('selectAction','featureStop')
;

SELECT '***** END PART TWO: Populate with dev suite data ******'
;

SELECT '***** END STEP ONE: asp_metrics_payment_duration_raw ******'
;

------------------------------------------------------------
--asp_metrics_payment_duration_order
--STEP TWO:
--Populate raw lag and lead values for page view order
------------------------------------------------------------

SELECT '***** BEGIN STEP TWO: asp_metrics_payment_duration_order ******'
;

INSERT OVERWRITE TABLE asp_duration_order PARTITION (date_denver,source_table)
SELECT
  visit_id,
  page_name,
  message_name,
  message_feature_name,
  message_feature_step_name,
  operation__success,
  ts_ms,
  lead(page_name)
    OVER (PARTITION BY visit_id
      ORDER BY ts_ms)
    AS next_page,
  lead(ts_ms)
    OVER (PARTITION BY visit_id
      ORDER BY ts_ms)
    AS next_timestamp,
  date_denver,
  source_table
FROM asp_duration_raw
WHERE
   (date_denver BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
;

SELECT '***** END STEP TWO: asp_metrics_payment_duration_order ******'
;

------------------------------------------------------------
--asp_metrics_payment_duration_agg
--STEP THREE:
--Populate agg table with all page time durations
------------------------------------------------------------

SELECT '***** BEGIN STEP THREE: asp_metrics_payment_duration_agg ******'
;

INSERT OVERWRITE TABLE asp_metrics_payment_duration_agg PARTITION (partition_date,source_table)
SELECT
  visit_id,
  case
      WHEN message_name = 'selectAction' AND page_name RLIKE 'pay-bill.onetime.payment-amount.*' then 'New_OT_1'
      when message_name = 'selectAction' AND page_name RLIKE 'pay-bill.onetime.payment-date.continue.*' then 'New_OT_2'
      when message_name = 'selectAction' AND page_name RLIKE 'pay-bill.onetime.payment-method.\(credit|checking|savings\)' then 'New_OT_3'
      when message_name = 'selectAction' AND page_name RLIKE 'pay-bill.onetime.payment-review.\(make-payment|pay-and-enroll\)' then 'New_OT_4'
      when message_name = 'featureStop'  AND message_feature_name = 'oneTimeBillPayFlow'
                                          AND operation__success IS NOT NULL
                                                     then 'New_OT_5_Confirm'
      when message_name = 'selectAction' AND page_name RLIKE 'pay-bill.enroll-in-autopay-\(no|with\)-balance-\(checking|savings|credit\)' then 'New_AP_1'
      when message_name = 'featureStop'  AND message_feature_name IN ('autoPayEnrollment','manageAutoPay')
                                         AND operation__success IS NOT NULL
                                         AND message_feature_step_name <> 'autopayChangesConfirmation'
                                                     then 'New_AP_2_Confirm'
      ELSE '#'
   end as payment_step_current,
  page_name as current_page,
  case
      when next_page RLIKE 'pay-bill.onetime.payment-amount.*' then 'New_OT_1'
      when next_page RLIKE 'pay-bill.onetime.payment-date.continue.*' then 'New_OT_2'
      when next_page RLIKE 'pay-bill.onetime.payment-method.\(credit|checking|savings\)' then 'New_OT_3'
      when next_page RLIKE 'pay-bill.onetime.payment-review.\(make-payment|pay-and-enroll\)' then 'New_OT_4'
      when next_page in ('oneTimeBillPayFlow') then 'New_OT_5_Confirm' -- approximate, as does not include operation__success criterion
      when next_page RLIKE 'pay-bill.enroll-in-autopay-\(no|with\)-balance-\(checking|savings|credit\)' then 'New_AP_1'
      when next_page IN ('autoPayEnrollment','manageAutoPay') then 'New_AP_2_Confirm' -- approximate, as does not include operation__success and message feature step name criteria
      ELSE '#'
   end as payment_step_next,
  next_page as next_page,
  next_timestamp - ts_ms AS duration,  -- get the amount of time spent on a page by subtracting the start time of their next page from the current page start time,
  message_feature_step_name,
  date_denver,
  source_table
FROM asp_duration_order
  WHERE
     (date_denver BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
     AND source_table IN ('asp_v_venona_events_portals_specnet',
                          'asp_v_venona_staging_portals_specnet')
;


SELECT '***** END STEP THREE: asp_metrics_payment_duration_agg ******'
;

------------------------------------------------------------

SELECT '***** Cleaning up tables for use ******'
;

drop table if exists ${env:LKP_db}.payment_stats_avg PURGE;
drop table if exists ${env:LKP_db}.payment_stats_05 PURGE;
drop table if exists ${env:LKP_db}.payment_stats_25 PURGE;
drop table if exists ${env:LKP_db}.payment_stats_50 PURGE;
drop table if exists ${env:LKP_db}.payment_stats_75 PURGE;
drop table if exists ${env:LKP_db}.payment_stats_95 PURGE;
drop table if exists ${env:LKP_db}.payment_stats_stdev PURGE;
drop table if exists ${env:LKP_db}.payment_stats_min PURGE;
drop table if exists ${env:LKP_db}.payment_stats_max PURGE;
drop table if exists ${env:LKP_db}.payment_stats_total PURGE;
drop table if exists ${env:LKP_db}.payment_stats_count PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.payment_stats_avg
( payment_step_current string,
  payment_step_next string,
  value decimal(18,3),
  payment_flow string,
  partition_date string,
  metric string,
  source_table string
);

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.payment_stats_05 LIKE ${env:LKP_db}.payment_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.payment_stats_25 LIKE ${env:LKP_db}.payment_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.payment_stats_50 LIKE ${env:LKP_db}.payment_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.payment_stats_75 LIKE ${env:LKP_db}.payment_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.payment_stats_95 LIKE ${env:LKP_db}.payment_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.payment_stats_stdev LIKE ${env:LKP_db}.payment_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.payment_stats_min LIKE ${env:LKP_db}.payment_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.payment_stats_max LIKE ${env:LKP_db}.payment_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.payment_stats_total LIKE ${env:LKP_db}.payment_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.payment_stats_count LIKE ${env:LKP_db}.payment_stats_avg;


SELECT '***** Calculating Stats tables for steps ******'
;

FROM
  (
      SELECT
        partition_date,
        payment_step_current,
        payment_step_next,
        CASE
            WHEN payment_step_current RLIKE 'Old_OT.*' THEN 'old_ot_flow'
            WHEN payment_step_current RLIKE 'Old_AP.*' THEN 'old_ap_flow'
            WHEN payment_step_current RLIKE 'New_OT.*' THEN 'new_ot_flow'
            WHEN payment_step_current RLIKE 'New_AP.*' THEN 'new_ap_flow'
            WHEN payment_step_next RLIKE 'Old_OT.*' THEN 'old_ot_flow_entry'
            WHEN payment_step_next RLIKE 'Old_AP.*' THEN 'old_ap_flow_entry'
            WHEN payment_step_next RLIKE 'New_OT.*' THEN 'new_ot_flow_entry'
            WHEN payment_step_next RLIKE 'New_AP.*' THEN 'new_ap_flow_entry'
            ELSE 'Not Identified Flow'
        END AS payment_flow,
        AVG(duration) AS avg_to_next_step,
        PERCENTILE(duration, array(0.05, 0.25, 0.5, 0.75, 0.95)) AS percentiles_to_next_step,
        stddev_pop(duration) AS stdev_to_next_step,
        MIN(duration) AS min_to_next_step,
        MAX(duration) AS max_to_next_step,
        SUM(duration) AS total_to_next_step,
        count(*) AS count_to_next_step,
        source_table
      FROM asp_metrics_payment_duration_agg
      WHERE
         (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
      GROUP BY
        partition_date,
        payment_step_current,
        payment_step_next,
        source_table
  ) stats_prep
  INSERT OVERWRITE TABLE ${env:LKP_db}.payment_stats_avg
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      avg_to_next_step,
      payment_flow,
      partition_date,
      'Average' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE ${env:LKP_db}.payment_stats_05
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      percentiles_to_next_step[0],
      payment_flow,
      partition_date,
      '.05 percentile' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE ${env:LKP_db}.payment_stats_25
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      percentiles_to_next_step[1],
      payment_flow,
      partition_date,
      '.25 percentile' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE ${env:LKP_db}.payment_stats_50
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      percentiles_to_next_step[2],
      payment_flow,
      partition_date,
      '.5 percentile' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE ${env:LKP_db}.payment_stats_75
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      percentiles_to_next_step[3],
      payment_flow,
      partition_date,
      '.75 percentile' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE ${env:LKP_db}.payment_stats_95
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      percentiles_to_next_step[4],
      payment_flow,
      partition_date,
      '.95 percentile' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE ${env:LKP_db}.payment_stats_stdev
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      stdev_to_next_step,
      payment_flow,
      partition_date,
      'stdev' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE ${env:LKP_db}.payment_stats_min
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      min_to_next_step,
      payment_flow,
      partition_date,
      'min' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE ${env:LKP_db}.payment_stats_max
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      max_to_next_step,
      payment_flow,
      partition_date,
      'max' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE ${env:LKP_db}.payment_stats_total
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      total_to_next_step,
      payment_flow,
      partition_date,
      'total' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE ${env:LKP_db}.payment_stats_count
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      count_to_next_step,
      payment_flow,
      partition_date,
      'count' as metric,-- metric,
      source_table
;

INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM ${env:LKP_db}.payment_stats_avg
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM ${env:LKP_db}.payment_stats_05
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM ${env:LKP_db}.payment_stats_25
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM ${env:LKP_db}.payment_stats_50
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM ${env:LKP_db}.payment_stats_75
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM ${env:LKP_db}.payment_stats_95
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM ${env:LKP_db}.payment_stats_stdev
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM ${env:LKP_db}.payment_stats_min
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM ${env:LKP_db}.payment_stats_max
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM ${env:LKP_db}.payment_stats_total
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM ${env:LKP_db}.payment_stats_count
;

SELECT '***** END STEP FOUR ******'
;
