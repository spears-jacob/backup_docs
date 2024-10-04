USE ${env:ENVIRONMENT}
;

------------------------------------------------------------
--asp_metrics_payment_duration_raw
--STEP ONE:
--Populate raw duration table with all page views in payment workflows from net_events
------------------------------------------------------------

------------------------------------------------------------
--PART ONE:
--Populate with production suite data
------------------------------------------------------------

SELECT '***** BEGIN STEP ONE: asp_metrics_payment_duration_raw ******'
;

INSERT OVERWRITE TABLE asp_metrics_payment_duration_raw PARTITION (partition_date,source_table)
SELECT
  visit__visit_id,
  state__view__current_page__name,
  message__timestamp,
  message__category,
  visit__settings["post_prop73"] AS browser_size_breakpoint,
  visit__settings["post_prop11"] AS visit__settings,
  partition_date,
  'net_events' AS source_table
FROM asp_v_net_events
WHERE
   (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
  AND message__category = 'Page View'
;

SELECT '***** END PART ONE: Populate with production suite data ******'
;


------------------------------------------------------------
--PART TWO:
--Populate with dev suite data
------------------------------------------------------------

INSERT OVERWRITE TABLE asp_metrics_payment_duration_raw PARTITION (partition_date,source_table)
SELECT
  visit__visit_id,
  state__view__current_page__name,
  message__timestamp,
  message__category,
  visit__settings["post_prop73"] AS browser_size_breakpoint,
  visit__settings["post_prop11"] AS visit__settings,
  partition_date_utc AS partition_date,
  'net_dev_events' AS source_table
FROM asp_v_net_dev_events
WHERE
    (epoch_datehour(cast(message__timestamp*1000 as bigint),'UTC') >= ("${env:START_DATE_TZ}") AND epoch_datehour(cast(message__timestamp*1000 as bigint),'UTC') < ("${env:END_DATE_TZ}"))
  AND message__category = 'Page View'
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

INSERT OVERWRITE TABLE asp_metrics_payment_duration_order PARTITION (partition_date,source_table)
SELECT
  visit__visit_id,
  state__view__current_page__name,
  message__category,
  message__timestamp,
  lead(state__view__current_page__name)
    OVER (PARTITION BY visit__visit_id
      ORDER BY message__timestamp)
    AS next_page,
  lead(message__timestamp)
    OVER (PARTITION BY visit__visit_id
      ORDER BY message__timestamp)
    AS next_timestamp,
  browser_size_breakpoint,
  visit__settings,
  partition_date,
  source_table
FROM asp_metrics_payment_duration_raw
WHERE
   (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
GROUP BY
  partition_date,
  visit__visit_id,
  message__category,
  state__view__current_page__name,
  message__timestamp,
  source_table,
  browser_size_breakpoint,
  visit__settings
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
  visit__visit_id,
  case
      when state__view__current_page__name in ('pay-bill.onetime-payment-date') then 'New_OT_2'
      when state__view__current_page__name in ('pay-bill.onetime-payment-method') then 'New_OT_3'
      when state__view__current_page__name in ('pay-bill.onetime-review-autopay-option', 'pay-bill.onetime-review-no-autopay-option') then 'New_OT_4'
      when state__view__current_page__name in ('pay-bill.onetime-confirmation', 'pay-bill.onetime-confirmation-with-autopay-enrollment') then 'New_OT_5_Confirm'
      when state__view__current_page__name in ('pay-bill.autopay-enrollment') then 'New_AP_1'
      when state__view__current_page__name in ('pay-bill.autopay-enrollment-confirmation') then 'New_AP_2_Confirm'
      when state__view__current_page__name in ('OneTime-noAutoPay', 'OneTime-wAutoPay') then 'Old_OT_1'
      when state__view__current_page__name in ('OneTime-noAutoPay-Credit-Review', 'OneTime-noAutoPay-Checking-Review', 'OneTime-noAutoPay-Savings-Review', 'OneTime-wAutoPay-Credit-Review', 'OneTime-wAutoPay-Checking-Review', 'OneTime-wAutoPay-Savings-Review') then 'Old_OT_2'
      when state__view__current_page__name in ('OneTime-noAutoPay-Credit-Confirm', 'OneTime-noAutoPay-Checking-Confirm', 'OneTime-noAutoPay-Savings-Confirm', 'OneTime-wAutoPay-Credit-Confirm', 'OneTime-wAutoPay-Checking-Confirm', 'OneTime-wAutoPay-Savings-Confirm') then 'Old_OT_3_Confirm'
      when state__view__current_page__name in ('AutoPay-wBalance','AutoPay-noBalance') then 'Old_AP_1'
      when state__view__current_page__name in ('AutoPay-wBalance-Credit-Review','AutoPay-noBalance-Credit-Review','AutoPay-wBalance-Checking-Review','AutoPay-noBalance-Checking-Review','AutoPay-wBalance-Savings-Review','AutoPay-noBalance-Savings-Review') then 'Old_AP_2'
      when state__view__current_page__name in ('AutoPay-noBalance-Savings-Confirm','AutoPay-wBalance-Savings-Confirm','AutoPay-noBalance-Checking-Confirm','AutoPay-noBalance-Credit-Confirm','AutoPay-wBalance-Checking-Confirm','AutoPay-wBalance-Credit-Confirm') then 'Old_AP_3_Confirm'
      when state__view__current_page__name in ('pay-bill.onetime') OR ((LOWER(visit__settings) = 'pay-bill.make-a-payment-hp') AND lower(state__view__current_page__name) = 'billing-and-transactions') then 'New_OT_1'
      ELSE '#'
   end as payment_step_current,
  state__view__current_page__name as current_page,
  case
      when next_page in ('pay-bill.onetime-payment-date') then 'New_OT_2'
      when next_page in ('pay-bill.onetime-payment-method') then 'New_OT_3'
      when next_page in ('pay-bill.onetime-review-autopay-option', 'pay-bill.onetime-review-no-autopay-option') then 'New_OT_4'
      when next_page in ('pay-bill.onetime-confirmation', 'pay-bill.onetime-confirmation-with-autopay-enrollment') then 'New_OT_5_Confirm'
      when next_page in ('pay-bill.autopay-enrollment') then 'New_AP_1'
      when next_page in ('pay-bill.autopay-enrollment-confirmation') then 'New_AP_2_Confirm'
      when next_page in ('OneTime-noAutoPay', 'OneTime-wAutoPay') then 'Old_OT_1'
      when next_page in ('OneTime-noAutoPay-Credit-Review', 'OneTime-noAutoPay-Checking-Review', 'OneTime-noAutoPay-Savings-Review', 'OneTime-wAutoPay-Credit-Review', 'OneTime-wAutoPay-Checking-Review', 'OneTime-wAutoPay-Savings-Review') then 'Old_OT_2'
      when next_page in ('OneTime-noAutoPay-Credit-Confirm', 'OneTime-noAutoPay-Checking-Confirm', 'OneTime-noAutoPay-Savings-Confirm', 'OneTime-wAutoPay-Credit-Confirm', 'OneTime-wAutoPay-Checking-Confirm', 'OneTime-wAutoPay-Savings-Confirm') then 'Old_OT_3_Confirm'
      when next_page in ('AutoPay-wBalance','AutoPay-noBalance') then 'Old_AP_1'
      when next_page in ('AutoPay-wBalance-Credit-Review','AutoPay-noBalance-Credit-Review','AutoPay-wBalance-Checking-Review','AutoPay-noBalance-Checking-Review','AutoPay-wBalance-Savings-Review','AutoPay-noBalance-Savings-Review') then 'Old_AP_2'
      when next_page in ('AutoPay-noBalance-Savings-Confirm','AutoPay-wBalance-Savings-Confirm','AutoPay-noBalance-Checking-Confirm','AutoPay-noBalance-Credit-Confirm','AutoPay-wBalance-Checking-Confirm','AutoPay-wBalance-Credit-Confirm') then 'Old_AP_3_Confirm'
      when next_page in ('pay-bill.onetime') OR ((LOWER(visit__settings) = 'pay-bill.make-a-payment-hp') AND lower(state__view__current_page__name) = 'billing-and-transactions') then 'New_OT_1'        ELSE '#'
   end as payment_step_next,
  next_page as next_page,
  next_timestamp - message__timestamp AS duration,  -- get the amount of time spent on a page by subtracting the start time of their next page from the current page start time,
  browser_size_breakpoint,
  partition_date,
  source_table
FROM asp_metrics_payment_duration_order
  WHERE
     (partition_date BETWEEN '${env:START_DATE}' AND '${env:END_DATE}')
;

SELECT '***** END STEP THREE: asp_metrics_payment_duration_agg ******'
;

------------------------------------------------------------

SELECT '***** Cleaning up tables for use ******'
;

TRUNCATE TABLE payment_stats_avg;
TRUNCATE TABLE payment_stats_05;
TRUNCATE TABLE payment_stats_25;
TRUNCATE TABLE payment_stats_50;
TRUNCATE TABLE payment_stats_75;
TRUNCATE TABLE payment_stats_95;
TRUNCATE TABLE payment_stats_stdev;
TRUNCATE TABLE payment_stats_min;
TRUNCATE TABLE payment_stats_max;
TRUNCATE TABLE payment_stats_total;
TRUNCATE TABLE payment_stats_count;

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
  INSERT OVERWRITE TABLE payment_stats_avg PARTITION (payment_flow, partition_date, metric, source_table)
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      avg_to_next_step,
      payment_flow,
      partition_date,
      'Average' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE payment_stats_05 PARTITION (payment_flow, partition_date, metric, source_table)
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      percentiles_to_next_step[0],
      payment_flow,
      partition_date,
      '.05 percentile' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE payment_stats_25 PARTITION (payment_flow, partition_date, metric, source_table)
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      percentiles_to_next_step[1],
      payment_flow,
      partition_date,
      '.25 percentile' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE payment_stats_50 PARTITION (payment_flow, partition_date, metric, source_table)
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      percentiles_to_next_step[2],
      payment_flow,
      partition_date,
      '.5 percentile' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE payment_stats_75 PARTITION (payment_flow, partition_date, metric, source_table)
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      percentiles_to_next_step[3],
      payment_flow,
      partition_date,
      '.75 percentile' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE payment_stats_95 PARTITION (payment_flow, partition_date, metric, source_table)
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      percentiles_to_next_step[4],
      payment_flow,
      partition_date,
      '.95 percentile' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE payment_stats_stdev PARTITION (payment_flow, partition_date, metric, source_table)
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      stdev_to_next_step,
      payment_flow,
      partition_date,
      'stdev' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE payment_stats_min PARTITION (payment_flow, partition_date, metric, source_table)
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      min_to_next_step,
      payment_flow,
      partition_date,
      'min' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE payment_stats_max PARTITION (payment_flow, partition_date, metric, source_table)
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      max_to_next_step,
      payment_flow,
      partition_date,
      'max' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE payment_stats_total PARTITION (payment_flow, partition_date, metric, source_table)
    SELECT
      payment_step_current,
      payment_step_next, --next_step,
      total_to_next_step,
      payment_flow,
      partition_date,
      'total' as metric,-- metric,
      source_table
  INSERT OVERWRITE TABLE payment_stats_count PARTITION (payment_flow, partition_date, metric, source_table)
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
  SELECT * FROM payment_stats_avg
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM payment_stats_05
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM payment_stats_25
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM payment_stats_50
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM payment_stats_75
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM payment_stats_95
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM payment_stats_stdev
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM payment_stats_min
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM payment_stats_max
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM payment_stats_total
;
INSERT OVERWRITE TABLE asp_metrics_payment_flow_stats PARTITION (payment_flow, partition_date, metric, source_table)
  SELECT * FROM payment_stats_count
;

SELECT '***** END STEP FOUR ******'
;
