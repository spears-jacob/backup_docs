SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.partition.stats=true;

-- When reprocessing consider that the rule for steps 6a, 6b and 6c applies starting 2016-08-11. Both rules need to be consider when calculating August.
-- Before that date use 'OneTime-wAutoPay-Credit-Confirm','OneTime-wAutoPay-Checking-Confirm','OneTime-wAutoPay-Savings-Confirm'  as current page

INSERT INTO TABLE net_bill_pay_analytics_monthly PARTITION (year_month)
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
        END as step
    FROM    
        (SELECT
            date_yearmonth(partition_date) as year_month,
            lag(page_name, 1) over (partition by visit_id order by seq_number) as page_name_previous,
            page_name as page_name_current,
            hh,
            user_name,
            visit_id,
            device_id
        FROM
            (SELECT
            ne.partition_date,
            ne.visit__account__enc_account_number as hh,
            ne.visit__user__enc_id as user_name,
            ne.state__view__current_page__name as page_name,
            ne.visit__visit_id as visit_id,
            ne.visit__device__enc_uuid as device_id,
            ne.message__sequence_number as seq_number
            FROM
            net_events as ne
            WHERE
            ne.partition_date BETWEEN '${hiveconf:MONTH_START_DATE}' AND '${hiveconf:MONTH_END_DATE}'
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
            ) as agg_events
        )as agg_windows 
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
    ) as agg_scenarios
GROUP BY
    year_month,
    step
ORDER BY
    year_month,
    step
;
