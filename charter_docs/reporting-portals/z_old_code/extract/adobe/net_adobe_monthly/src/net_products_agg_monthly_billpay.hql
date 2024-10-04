-------------------------------------------------------------------------------

--Populates the temp table net_bill_pay_analytics_monthly with aggregated billpay data
--Currently populates data from various sources for L-CHTR, L-BHN, and L-TWC

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-CHTR Bill Pay Calculations --

INSERT INTO TABLE ${env:TMP_db}.net_bill_pay_analytics_monthly --PARTITION (year_month = '${env:YEAR_MONTH}')
SELECT
step,
COUNT(DISTINCT hh) AS count_hhs,
COUNT(DISTINCT user_name) AS count_user_names,
COUNT(DISTINCT visit_id) AS count_visits,
COUNT(DISTINCT device_id) AS count_visitors,
COUNT(*) AS count_page_views,
'L-CHTR' AS company,
year_month
FROM
    (SELECT
    year_month,
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
    ELSE "0"
    END AS step
    FROM
        (SELECT
        '${env:YEAR_MONTH}' AS year_month,
        lag(page_name, 1) over (partition by visit_id order by seq_number) AS page_name_previous,
        page_name AS page_name_current,
        hh,
        user_name,
        visit_id,
        device_id
        FROM
            (SELECT
            ne.partition_date,
            ne.visit__account__enc_account_number AS hh,
            ne.visit__user__enc_id AS user_name,
            ne.state__view__current_page__name AS page_name,
            ne.visit__visit_id AS visit_id,
            ne.visit__device__enc_uuid AS device_id,
            ne.message__sequence_number AS seq_number
            FROM
            net_events AS ne
            WHERE
            ne.partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
            AND (state__view__current_page__name = 'OneTime-wAutoPay'
            OR state__view__current_page__name = 'OneTime-noAutoPay'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Credit-Review'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Checking-Review'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Savings-Review'
            OR state__view__current_page__name = 'OneTime-noAutoPay-Credit-Review'
            OR state__view__current_page__name = 'OneTime-noAutoPay-Checking-Review'
            OR state__view__current_page__name = 'OneTime-noAutoPay-Savings-Review'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Credit-Confirm'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Checking-Confirm'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Savings-Confirm'
            OR state__view__current_page__name = 'OneTime-noAutoPay-Credit-Confirm'
            OR state__view__current_page__name = 'OneTime-noAutoPay-Checking-Confirm'
            OR state__view__current_page__name = 'OneTime-noAutoPay-Savings-Confirm'
            OR state__view__current_page__name = 'AutoPay-wBalance'
            OR state__view__current_page__name = 'AutoPay-wBalance-Credit-Review'
            OR state__view__current_page__name = 'AutoPay-wBalance-Checking-Review'
            OR state__view__current_page__name = 'AutoPay-wBalance-Savings-Review'
            OR state__view__current_page__name = 'AutoPay-wBalance-Credit-Confirm'
            OR state__view__current_page__name = 'AutoPay-wBalance-Checking-Confirm'
            OR state__view__current_page__name = 'AutoPay-wBalance-Savings-Confirm'
            OR state__view__current_page__name = 'AutoPay-noBalance'
            OR state__view__current_page__name = 'AutoPay-noBalance-Credit-Review'
            OR state__view__current_page__name = 'AutoPay-noBalance-Checking-Review'
            OR state__view__current_page__name = 'AutoPay-noBalance-Savings-Review'
            OR state__view__current_page__name = 'AutoPay-noBalance-Credit-Confirm'
            OR state__view__current_page__name = 'AutoPay-noBalance-Checking-Confirm'
            OR state__view__current_page__name = 'AutoPay-noBalance-Savings-Confirm')
            ) AS agg_events
        ) AS agg_windows
    WHERE
    (page_name_current = 'OneTime-wAutoPay')
    OR (page_name_current = 'OneTime-noAutoPay')
    OR (page_name_current = 'OneTime-wAutoPay-Credit-Review')
    OR (page_name_current = 'OneTime-wAutoPay-Checking-Review')
    OR (page_name_current = 'OneTime-wAutoPay-Savings-Review')
    OR (page_name_current = 'OneTime-noAutoPay-Credit-Review')
    OR (page_name_current = 'OneTime-noAutoPay-Checking-Review')
    OR (page_name_current = 'OneTime-noAutoPay-Savings-Review')
    OR (page_name_previous = 'OneTime-wAutoPay-Credit-Review' AND page_name_current = 'OneTime-wAutoPay-Credit-Confirm')
    OR (page_name_previous = 'OneTime-wAutoPay-Checking-Review' AND page_name_current = 'OneTime-wAutoPay-Checking-Confirm')
    OR (page_name_previous = 'OneTime-wAutoPay-Savings-Review' AND page_name_current = 'OneTime-wAutoPay-Savings-Confirm')
    OR (page_name_current = 'OneTime-noAutoPay-Credit-Confirm')
    OR (page_name_current = 'OneTime-noAutoPay-Checking-Confirm')
    OR (page_name_current = 'OneTime-noAutoPay-Savings-Confirm')
    OR (page_name_current = 'AutoPay-wBalance')
    OR (page_name_current = 'AutoPay-wBalance-Credit-Review')
    OR (page_name_current = 'AutoPay-wBalance-Checking-Review')
    OR (page_name_current = 'AutoPay-wBalance-Savings-Review')
    OR (page_name_previous = 'AutoPay-wBalance-Credit-Review' AND page_name_current = 'AutoPay-wBalance-Credit-Confirm')
    OR (page_name_previous = 'AutoPay-wBalance-Checking-Review' AND page_name_current = 'AutoPay-wBalance-Checking-Confirm')
    OR (page_name_previous = 'AutoPay-wBalance-Savings-Review' AND page_name_current = 'AutoPay-wBalance-Savings-Confirm')
    OR (page_name_current = 'AutoPay-noBalance')
    OR (page_name_current = 'AutoPay-noBalance-Credit-Review')
    OR (page_name_current = 'AutoPay-noBalance-Checking-Review')
    OR (page_name_current = 'AutoPay-noBalance-Savings-Review')
    OR (page_name_current = 'AutoPay-noBalance-Credit-Confirm')
    OR (page_name_current = 'AutoPay-noBalance-Checking-Confirm')
    OR (page_name_previous = 'AutoPay-noBalance-Savings-Review' AND page_name_current = 'AutoPay-noBalance-Savings-Confirm')
    ) AS agg_scenarios
GROUP BY
year_month,
step
;

SELECT '*****-- End L-CHTR Bill Pay Calculations --*****' -- 49.093 seconds
;

-- End L-CHTR Bill Pay Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Bill Pay Aggregation --

DROP TABLE IF EXISTS ${env:TMP_db}.net_bill_pay_agg_metrics PURGE
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_bill_pay_agg_metrics
(
view_online_statement_count INT,
one_time_payment_count INT,
one_time_payments_confirm_count INT,
setup_autopay_count INT,
successful_autopay_confirm_count INT,
company STRING,
year_month STRING
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;

-------------------------------------------------------------------------------

INSERT INTO ${env:TMP_db}.net_bill_pay_agg_metrics

SELECT
tmp_statement_view.view_online_statement_count,
SUM(IF(step IN ('2a','2b','2c','2d','2e','2f'),count_visits,0)) AS one_time_payment_count,
SUM(IF(step IN ('3a','3b','3c','3d','3e','3f'),count_visits,0)) AS one_time_payments_confirm_count,
SUM(IF(step IN ('5a','5b','5c','8a','8b','8c'),count_visits,0)) AS setup_autopay_count,
SUM(IF(step IN ('6a','6b','6c','9a','9b','9c'),count_visits,0)) AS successful_autopay_confirm_count,
'L-CHTR' AS company,
bpam.year_month
FROM
${env:TMP_db}.net_bill_pay_analytics_monthly bpam
JOIN 
    (
    SELECT
    SUM(CASE WHEN message__name IN ('View Current Bill', 'View Statement') THEN 1 ELSE 0 END) AS view_online_statement_count,
    'L-CHTR' AS company,
    date_yearmonth(partition_date) AS year_month
    FROM net_events ne
    WHERE
    partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
    GROUP BY
    date_yearmonth(partition_date)
    ) tmp_statement_view 
ON bpam.company = tmp_statement_view.company AND bpam.year_month = tmp_statement_view.year_month
GROUP BY
bpam.year_month,
view_online_statement_count
;

SELECT '*****-- End L-CHTR Bill Pay Aggregation --*****' -- 25.869 seconds
;

-------------------------------------------------------------------------------

INSERT INTO ${env:TMP_db}.net_bill_pay_agg_metrics

SELECT
SUM(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 11') AND state__view__current_page__page_type='RES',1,0)) AS view_online_statement_count,
COUNT(DISTINCT (CASE WHEN(array_contains(message__feature__name, 'Custom Event 36') AND state__view__current_page__page_type='RES') THEN visit__visit_id END)) AS one_time_payment_count,
COUNT(DISTINCT (CASE WHEN(array_contains(message__feature__name, 'Custom Event 31') AND state__view__current_page__page_type='RES') THEN visit__visit_id END)) AS one_time_payments_confirm_count,
COUNT(DISTINCT (CASE WHEN(array_contains(message__feature__name, 'Custom Event 19') AND state__view__current_page__page_type='RES') THEN visit__visit_id END)) AS setup_autopay_count,
COUNT(DISTINCT (CASE WHEN(array_contains(message__feature__name, 'Custom Event 24') AND state__view__current_page__page_type='RES') THEN visit__visit_id END)) AS successful_autoplay_confirm_count,
'L-BHN' AS company,
'${env:YEAR_MONTH}' AS year_month
FROM
bhn_bill_pay_events
WHERE
(partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
AND
epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
GROUP BY
date_yearmonth(epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver'))
;

SELECT '*****-- End L-BHN Bill Pay Aggregation --*****' -- 139.601 seconds
;

-------------------------------------------------------------------------------

INSERT INTO ${env:TMP_db}.net_bill_pay_agg_metrics

SELECT
SUM(IF(state__view__current_page__elements__name RLIKE 'ebpp:statement download.*' AND ARRAY_CONTAINS(message__feature__name,'Instance of eVar7'),1,0)) AS view_online_statement_count,
NULL AS one_time_payment_count,
--
SIZE(COLLECT_SET(CASE WHEN state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you'
AND operation__operation_type
IN ('one time payment > dc',
'one time:saved > cc',
'one time:saved > dc',
'one time:unknown > ach',
'one time:unknown > cc',
'one time:unknown > dc')
THEN visit__visit_id END)) AS one_time_payments_confirm_count,
--
NULL AS setup_autopay_count,
--
COUNT(DISTINCT (CASE WHEN state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you'
AND operation__operation_type
IN ('recurring:unknown > cc','recurring:unknown > ach','recurring:unknown > dc')
THEN visit__visit_id END)) AS successful_autoplay_confirm_count,
'L-TWC' AS company,
'${env:YEAR_MONTH}' AS year_month
FROM
twc_residential_global_events
WHERE
(partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
AND
epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
GROUP BY
date_yearmonth(epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver'))
;

SELECT '*****-- End L-TWC Bill Pay Aggregation --*****' -- 413.279 seconds
;

-- End L-TWC Bill Pay Aggregation --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- End L-TWC Bill Pay Aggregation --

SELECT '*****-- End Bill Pay Aggregation Calculations --*****'
;
