-------------------------------------------------------------------------------

--Populates the temp table net_bill_pay_analytics_monthly with aggregated billpay data
--Currently populates data from various sources for L-CHTR, L-BHN, and L-TWC
---PLEASE INSERT THIS LATER....
--SUM(IF(message__name IN('View Current Bill','View Statement','Statement') AND message__category = 'Custom Link', 1, 0)) AS view_online_statements,
--   SIZE(COLLECT_SET(IF((state__view__current_page__name IN ('OneTime-noAutoPay-Savings-Confirm','OneTime-noAutoPay-Checking-Confirm','OneTime-noAutoPay-Credit-Confirm')) AND (message__category = 'Page View'), visit__visit_id, NULL))) as one_time_payment_success,
--   SIZE(COLLECT_SET(IF((state__view__current_page__name IN ('OneTime-wAutoPay-Checking-Confirm','OneTime-wAutoPay-Savings-Confirm','OneTime-wAutoPay-Credit-Confirm')) AND (message__category = 'Page View'), visit__visit_id, NULL))) as one_time_payment_with_autopay_confirm_count,
--   SIZE(COLLECT_SET(IF((state__view__current_page__name IN ('AutoPay-noBalance-Checking-Confirm','AutoPay-noBalance-Savings-Confirm','AutoPay-noBalance-Credit-Confirm')) AND (message__category = 'Page View'), visit__visit_id, NULL)))
--   +
--   SIZE(COLLECT_SET(IF((state__view__current_page__name IN ('AutoPay-wBalance-Checking-Confirm','AutoPay-wBalance-Savings-Confirm','AutoPay-wBalance-Credit-Confirm')) AND (message__category = 'Page View'), visit__visit_id, NULL))) as autopay_setup_success

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-- Drop and rebuild net_bill_pay_analytics_monthly temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.net_bill_pay_analytics_${env:CADENCE};

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.net_bill_pay_analytics_${env:CADENCE}
(
  step STRING ,
  count_hhs INT,
  count_user_names INT,
  count_visits INT,
  count_visitors INT,
  count_page_views INT,
  company STRING,
  ${env:pf} STRING
)
;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-CHTR Bill Pay Calculations --

INSERT INTO TABLE ${env:TMP_db}.net_bill_pay_analytics_${env:CADENCE}
SELECT  step,
        COUNT(DISTINCT hh) AS count_hhs,
        COUNT(DISTINCT user_name) AS count_user_names,
        COUNT(DISTINCT visit_id) AS count_visits,
        COUNT(DISTINCT device_id) AS count_visitors,
        COUNT(*) AS count_page_views,
        'L-CHTR' AS company,
        ${env:pf}
FROM (SELECT
      ${env:pf} ,
      page_name_previous,
      page_name_current,
      hh,
      user_name,
      visit_id,
      device_id,
      CASE
        WHEN (page_name_current = 'OneTime-wAutoPay') THEN "1a"
        WHEN (page_name_current = 'OneTime-noAutoPay') THEN "1b"
        WHEN (page_name_current = 'OneTime-wAutoPay-Credit-Review') THEN "2a"
        WHEN (page_name_current = 'OneTime-wAutoPay-Checking-Review') THEN "2b"
        WHEN (page_name_current = 'OneTime-wAutoPay-Savings-Review') THEN "2c"
        WHEN (page_name_current = 'OneTime-noAutoPay-Credit-Review') THEN "2d"
        WHEN (page_name_current = 'OneTime-noAutoPay-Checking-Review') THEN "2e"
        WHEN (page_name_current = 'OneTime-noAutoPay-Savings-Review') THEN "2f"
        WHEN (page_name_previous = 'OneTime-wAutoPay-Credit-Review' AND page_name_current = 'OneTime-wAutoPay-Credit-Confirm') THEN "3a"
        WHEN (page_name_previous = 'OneTime-wAutoPay-Checking-Review' AND page_name_current = 'OneTime-wAutoPay-Checking-Confirm') THEN "3b"
        WHEN (page_name_previous = 'OneTime-wAutoPay-Savings-Review' AND page_name_current = 'OneTime-wAutoPay-Savings-Confirm') THEN "3c"
        WHEN (page_name_current = 'OneTime-noAutoPay-Credit-Confirm') THEN "3d"
        WHEN (page_name_current = 'OneTime-noAutoPay-Checking-Confirm') THEN "3e"
        WHEN (page_name_current = 'OneTime-noAutoPay-Savings-Confirm') THEN "3f"
        WHEN (page_name_current = 'AutoPay-wBalance') THEN "4a"
        WHEN (page_name_current = 'AutoPay-wBalance-Credit-Review') THEN "5a"
        WHEN (page_name_current = 'AutoPay-wBalance-Checking-Review') THEN "5b"
        WHEN (page_name_current = 'AutoPay-wBalance-Savings-Review') THEN "5c"
        WHEN (page_name_previous = 'AutoPay-wBalance-Credit-Review' AND page_name_current = 'AutoPay-wBalance-Credit-Confirm') THEN "6a"
        WHEN (page_name_previous = 'AutoPay-wBalance-Checking-Review' AND page_name_current = 'AutoPay-wBalance-Checking-Confirm') THEN "6b"
        WHEN (page_name_previous = 'AutoPay-wBalance-Savings-Review' AND page_name_current = 'AutoPay-wBalance-Savings-Confirm') THEN "6c"
        WHEN (page_name_current = 'AutoPay-noBalance') THEN "7a"
        WHEN (page_name_current = 'AutoPay-noBalance-Credit-Review') THEN "8a"
        WHEN (page_name_current = 'AutoPay-noBalance-Checking-Review') THEN "8b"
        WHEN (page_name_current = 'AutoPay-noBalance-Savings-Review') THEN "8c"
        WHEN (page_name_current = 'AutoPay-noBalance-Credit-Confirm') THEN "9a"
        WHEN (page_name_current = 'AutoPay-noBalance-Checking-Confirm') THEN "9b"
        WHEN (page_name_previous = 'AutoPay-noBalance-Savings-Review' AND page_name_current = 'AutoPay-noBalance-Savings-Confirm') THEN "9c"
        WHEN (LOWER(page_name_current) = 'pay-bill.onetime-confirmation') THEN "10a"
        WHEN (LOWER(page_name_current) = 'pay-bill.onetime-confirmation-with-autopay-enrollment') THEN "10b"
        WHEN (LOWER(page_name_current) = 'pay-bill.autopay-enrollment-confirmation') THEN "10c"
        WHEN (page_name_current = 'autopayEnrollmentConfirmationWithPayment') THEN "10d"
        ELSE "0"
        END AS step
      FROM
          (SELECT ${env:pf},
                  lag(page_name, 1) over (partition by visit_id order by seq_number) AS page_name_previous,
                  page_name AS page_name_current,
                  hh,
                  user_name,
                  visit_id,
                  device_id
                  FROM
                    (SELECT
                      ${env:ap} as ${env:pf},
                      ne.visit__account__enc_account_number AS hh,
                      ne.visit__user__enc_id AS user_name,
                      ne.state__view__current_page__name AS page_name,
                      ne.visit__visit_id AS visit_id,
                      ne.visit__device__enc_uuid AS device_id,
                      ne.message__sequence_number AS seq_number
                      FROM asp_v_net_events AS ne
                      LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm on ne.partition_date  = fm.partition_date
                      WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
                      AND message__category = 'Page View'
                      AND   (state__view__current_page__name
                              IN(   'OneTime-wAutoPay'
                                    'OneTime-noAutoPay',
                                    'OneTime-wAutoPay-Credit-Review',
                                    'OneTime-wAutoPay-Checking-Review',
                                    'OneTime-wAutoPay-Savings-Review',
                                    'OneTime-noAutoPay-Credit-Review',
                                    'OneTime-noAutoPay-Checking-Review',
                                    'OneTime-noAutoPay-Savings-Review',
                                    'OneTime-wAutoPay-Credit-Confirm',
                                    'OneTime-wAutoPay-Checking-Confirm',
                                    'OneTime-wAutoPay-Savings-Confirm',
                                    'OneTime-noAutoPay-Credit-Confirm',
                                    'OneTime-noAutoPay-Checking-Confirm',
                                    'OneTime-noAutoPay-Savings-Confirm',
                                    'AutoPay-wBalance',
                                    'AutoPay-wBalance-Credit-Review',
                                    'AutoPay-wBalance-Checking-Review',
                                    'AutoPay-wBalance-Savings-Review',
                                    'AutoPay-wBalance-Credit-Confirm',
                                    'AutoPay-wBalance-Checking-Confirm',
                                    'AutoPay-wBalance-Savings-Confirm',
                                    'AutoPay-noBalance',
                                    'AutoPay-noBalance-Credit-Review',
                                    'AutoPay-noBalance-Checking-Review',
                                    'AutoPay-noBalance-Savings-Review',
                                    'AutoPay-noBalance-Credit-Confirm',
                                    'AutoPay-noBalance-Checking-Confirm',
                                    'AutoPay-noBalance-Savings-Confirm',
                                    'pay-bill.onetime-confirmation',
                                    'pay-bill.onetime-confirmation-with-autopay-enrollment',
                                    'pay-bill.autopay-enrollment-confirmation',
                                    'autopayEnrollmentConfirmationWithPayment'))
               ) AS agg_events

          ) AS agg_windows
        WHERE (page_name_current IN('OneTime-wAutoPay'
                                    'OneTime-noAutoPay',
                                    'OneTime-wAutoPay-Credit-Review',
                                    'OneTime-wAutoPay-Checking-Review',
                                    'OneTime-wAutoPay-Savings-Review',
                                    'OneTime-noAutoPay-Credit-Review',
                                    'OneTime-noAutoPay-Checking-Review',
                                    'OneTime-noAutoPay-Savings-Review',
                                    'AutoPay-noBalance',
                                    'AutoPay-noBalance-Credit-Review',
                                    'AutoPay-noBalance-Checking-Review',
                                    'AutoPay-noBalance-Savings-Review',
                                    'AutoPay-noBalance-Credit-Confirm',
                                    'AutoPay-noBalance-Checking-Confirm',
                                    'OneTime-noAutoPay-Credit-Confirm',
                                    'OneTime-noAutoPay-Checking-Confirm',
                                    'OneTime-noAutoPay-Savings-Confirm',
                                    'AutoPay-wBalance',
                                    'AutoPay-wBalance-Credit-Review',
                                    'AutoPay-wBalance-Checking-Review',
                                    'AutoPay-wBalance-Savings-Review',
                                    'pay-bill.onetime-confirmation',
                                    'pay-bill.onetime-confirmation-with-autopay-enrollment',
                                    'pay-bill.autopay-enrollment-confirmation',
                                    'autopayEnrollmentConfirmationWithPayment')
          OR (page_name_previous =  'OneTime-wAutoPay-Credit-Review'    AND page_name_current = 'OneTime-wAutoPay-Credit-Confirm')
          OR (page_name_previous =  'OneTime-wAutoPay-Checking-Review'  AND page_name_current = 'OneTime-wAutoPay-Checking-Confirm')
          OR (page_name_previous =  'OneTime-wAutoPay-Savings-Review'   AND page_name_current = 'OneTime-wAutoPay-Savings-Confirm')
          OR (page_name_previous =  'AutoPay-wBalance-Credit-Review'    AND page_name_current = 'AutoPay-wBalance-Credit-Confirm')
          OR (page_name_previous =  'AutoPay-wBalance-Checking-Review'  AND page_name_current = 'AutoPay-wBalance-Checking-Confirm')
          OR (page_name_previous =  'AutoPay-wBalance-Savings-Review'   AND page_name_current = 'AutoPay-wBalance-Savings-Confirm')
          OR (page_name_previous =  'AutoPay-noBalance-Savings-Review'  AND page_name_current = 'AutoPay-noBalance-Savings-Confirm')
        )
    ) AS agg_scenarios
GROUP BY ${env:pf}, step
ORDER BY ${env:pf}, step
;



SELECT '*****-- End L-CHTR Bill Pay Calculations --*****' -- 49.093 seconds
;

-- End L-CHTR Bill Pay Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Bill  Pay Aggregation --

DROP TABLE IF EXISTS ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE} PURGE
;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}
( view_online_statement_count INT,
  one_time_payment_count INT,
  one_time_payments_confirm_count INT,
  one_time_payments_with_autopay_confirm_count INT,
  setup_autopay_count INT,
  successful_autopay_confirm_count INT,
  one_time_payment_updated_wotap INT,
  auto_payment_confirm_updated_wotap INT,
  company STRING,
  ${env:pf} STRING )
;

-------------------------------------------------------------------------------

INSERT INTO ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}

SELECT  tmp_statement_view.view_online_statement_count,
        SUM(IF(step IN ('2a','2b','2c','2d','2e','2f'),count_visits,0)) AS one_time_payment_count,
        SUM(IF(step IN ('3d','3e','3f','10a'),count_visits,0)) AS one_time_payments_confirm_count, --updated 2018-02-26 DPrince
        SUM(IF(step IN ('3a','3b','3c', '10b'),count_visits,0)) AS one_time_payments_with_autopay_confirm_count, --updated 2018-02-26 DPrince
        SUM(IF(step IN ('5a','5b','5c','8a','8b','8c'),count_visits,0)) AS setup_autopay_count,
        SUM(IF(step IN ('6a','6b','6c','9a','9b','9c', '10c'),count_visits,0)) AS successful_autopay_confirm_count, --updated 2018-02-26 DPrince
        SUM(IF(step IN ('3d','3e','3f','10a','10b','10d'),count_visits,0)) AS one_time_payment_updated_wotap, --New definition (otp + otp w/ap)
        SUM(IF(step IN ('3a','3b','3c','6a','6b','6c','9a','9b','9c','10b','10c','10d'),count_visits,0)) AS auto_payment_confirm_updated_wotap, --New definition (ap + otp w/ap)
        'L-CHTR' AS company,
        bpam.${env:pf}
FROM ${env:TMP_db}.net_bill_pay_analytics_${env:CADENCE} bpam
LEFT JOIN
    ( SELECT SUM(1) AS view_online_statement_count,
             'L-CHTR' AS company,
             ${env:pf}
      FROM
         (SELECT message__name, ${env:ap} as ${env:pf} FROM asp_v_net_events ne
          LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm on ne.partition_date  = fm.partition_date
          WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
          AND message__category = 'Custom Link'
          AND (message__name IN ('View Current Bill', 'View Statement','Statement', 'pay-bill.billing-statement-download')
                OR (message__name = 'pay-bill.view-statements' AND visit__settings["post_prop26"] IS NOT NULL)
              )
         ) dictionary
      GROUP BY ${env:pf}
    ) tmp_statement_view
ON bpam.company = tmp_statement_view.company AND bpam.${env:pf} = tmp_statement_view.${env:pf}
GROUP BY bpam.${env:pf},
view_online_statement_count
;

SELECT '*****-- End L-CHTR Bill Pay Aggregation --*****' -- 25.869 seconds
;

-------------------------------------------------------------------------------

INSERT INTO ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}
-- 2018-03-06 changed from count distinct case when to a sum if. We want to capture instances instead of unique visits
SELECT  SUM(IF(message__name RLIKE('.*stmtdownload/.*') AND message__category = 'Exit Link' AND visit__settings["post_evar9"] = 'RES', 1, 0)) AS view_online_statement_count,
        sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 36') AND state__view__current_page__page_type='RES',1,0)) AS one_time_payment_count,
        sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 31') AND state__view__current_page__page_type='RES',1,0)) AS one_time_payments_confirm_count,
        INT(NULL) as one_time_payments_with_autopay_confirm_count,
        sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 19') AND state__view__current_page__page_type='RES',1,0)) AS setup_autopay_count,
        sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 24') AND state__view__current_page__page_type='RES',1,0)) AS successful_autopay_confirm_count,
        sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 31') AND state__view__current_page__page_type='RES',1,0)) AS one_time_payment_updated_wotap,
        sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 24') AND state__view__current_page__page_type='RES',1,0)) AS auto_payment_confirm_updated_wotap,
        'L-BHN' AS company,
        ${env:pf}
FROM    (SELECT message__name,
                message__category,
                message__feature__name,
                visit__settings,
                state__view__current_page__page_type,
                visit__visit_id,
                ${env:ap} as ${env:pf}
         FROM asp_v_bhn_bill_pay_events
         LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
         WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))) dictionary
GROUP BY ${env:pf};

SELECT '*****-- End L-BHN Bill Pay Aggregation --*****' -- 139.601 seconds
;


-------------------------------------------------------------------------------
INSERT INTO ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}

SELECT  SUM(IF(state__view__current_page__elements__name RLIKE 'ebpp:statement download.*'
           AND ARRAY_CONTAINS(message__feature__name,'Instance of eVar7'),1,0)) AS view_online_statement_count,

        SIZE(COLLECT_SET(CASE WHEN state__view__current_page__elements__name = 'MyTWC > Billing > Make Payment > Payment Confirmation'
                     THEN visit__visit_id END)) AS one_time_payment_count,
        --
        SIZE(COLLECT_SET(CASE WHEN operation__operation_type RLIKE '.*one time.*'
                          AND state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you'
                          AND message__category = 'Page View'
                          THEN visit__visit_id END)) AS one_time_payments_confirm_count,

        INT(NULL) as one_time_payments_with_autopay_confirm_count,--

        SIZE(COLLECT_SET(CASE WHEN state__view__current_page__elements__name RLIKE '.*AutoPay > Setup.*'
                               AND state__view__current_page__elements__name RLIKE '.*Success.*'
                     THEN visit__visit_id END)) AS setup_autopay_count,
        --
        SIZE(COLLECT_SET(CASE WHEN operation__operation_type RLIKE '.*recurring:.*'
                          AND state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you'
                          AND message__category = 'Page View'
                          THEN visit__visit_id END)) AS successful_autopay_confirm_count,
--Adding OTP w-AP metric name for continuity in Prod Monthly Reporting
        SIZE(COLLECT_SET(CASE WHEN operation__operation_type RLIKE '.*one time.*'
                          AND state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you'
                          AND message__category = 'Page View'
                          THEN visit__visit_id END)) AS one_time_payment_updated_wotap,
----Adding AP w-OTP +AP setup for continuity in Prod Monthly Reporting
        SIZE(COLLECT_SET(CASE WHEN operation__operation_type RLIKE '.*recurring:.*'
                          AND state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you'
                          AND message__category = 'Page View'
                          THEN visit__visit_id END)) AS auto_payment_confirm_updated_wotap,
        'L-TWC' AS company,
        ${env:pf}
FROM    (SELECT  state__view__current_page__elements__name,
                 state__view__current_page__page_name,
                 message__feature__name,
                 message__category,
                 operation__operation_type,
                 visit__visit_id,
                 ${env:ap} as ${env:pf}
        FROM asp_v_twc_residential_global_events
        LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm on ${env:ape}  = partition_date
        WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
       ) dictionary
GROUP BY ${env:pf}
;

SELECT '*****-- End L-TWC Bill Pay Aggregation --*****' -- 413.279 seconds
;

-- End L-TWC Bill Pay Aggregation --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- End L-TWC Bill Pay Aggregation --

SELECT '*****-- End Bill Pay Aggregation Calculations --*****'
;

SELECT '\n\n*****-- Now Inserting Bill Pay Aggregation Calculations into asp_${env:CADENCE}_agg_raw --*****\n\n'
;


----- Insert into agg table -------
INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (SELECT   'view_online_statement_count' as metric,
               view_online_statement_count AS value,
               'Visits' as unit,
               'resi' as domain,
               company,
               ${env:pf}
      FROM ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}
      UNION
      SELECT   'one_time_payment_count' as metric,
               one_time_payment_count AS value,
               'Visits' as unit,
               'resi' as domain,
               company,
               ${env:pf}
      FROM ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}
      UNION
      SELECT   'one_time_payments_confirm_count' as metric,
               one_time_payments_confirm_count AS value,
               'Visits' as unit,
               'resi' as domain,
               company,
               ${env:pf}
      FROM ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}
      UNION
      SELECT   'one_time_payments_with_autopay_confirm_count' as metric,
               one_time_payments_with_autopay_confirm_count AS value,
               'Visits' as unit,
               'resi' as domain,
               company,
               ${env:pf}
      FROM ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}
      UNION
      SELECT   'setup_autopay_count' as metric,
               setup_autopay_count AS value,
               'Visits' as unit,
               'resi' as domain,
               company,
               ${env:pf}
      FROM ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}
      UNION
      SELECT   'successful_autopay_confirm_count' as metric,
               successful_autopay_confirm_count AS value,
               'Visits' as unit,
               'resi' as domain,
               company,
               ${env:pf}
      FROM ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}
      UNION
      SELECT   'one_time_payment_updated_wotap' as metric,
               one_time_payment_updated_wotap AS value,
               'Visits' as unit,
               'resi' as domain,
               company,
               ${env:pf}
      FROM ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}
      UNION
      SELECT   'auto_payment_confirm_updated_wotap' as metric,
               auto_payment_confirm_updated_wotap AS value,
               'Visits' as unit,
               'resi' as domain,
               company,
               ${env:pf}
      FROM ${env:TMP_db}.net_bill_pay_agg_metrics_${env:CADENCE}
    ) q
WHERE q.value is not null
;
