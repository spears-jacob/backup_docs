SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.partition.stats=true;

-- When reprocessing consider that the rule for steps 6a, 6b and 6c applies starting 2016-08-11. Both rules need to be consider when calculating August.
-- Before that date use 'OneTime-wAutoPay-Credit-Confirm_STVA','OneTime-wAutoPay-Checking-Confirm_STVA','OneTime-wAutoPay-Savings-Confirm_STVA'  as current page

INSERT INTO TABLE net_bill_pay_analytics_STVA_monthly PARTITION (year_month)
SELECT
    step,
    COUNT(DISTINCT hh) AS count_hhs,
    COUNT(DISTINCT user_name) AS count_user_names,
    COUNT(DISTINCT visit_id) AS count_visits,
    COUNT(DISTINCT device_id) AS count_visitors,
    COUNT(*) AS count_page_views,
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
            WHEN (page_name_current = 'OneTime-wAutoPay_STVA') THEN "1a"
            WHEN (page_name_current = 'OneTime-noAutoPay_STVA') THEN "1b"
            WHEN (page_name_current = 'OneTime-wAutoPay-Credit-Review_STVA') THEN "2a"
            WHEN (page_name_current = 'OneTime-wAutoPay-Checking-Review_STVA') THEN "2b"
            WHEN (page_name_current = 'OneTime-wAutoPay-Savings-Review_STVA') THEN "2c"
            WHEN (page_name_current = 'OneTime-noAutoPay-Credit-Review_STVA') THEN "2d"
            WHEN (page_name_current = 'OneTime-noAutoPay-Checking-Review_STVA') THEN "2e"
            WHEN (page_name_current = 'OneTime-noAutoPay-Savings-Review_STVA') THEN "2f"
            WHEN (page_name_previous = 'OneTime-wAutoPay-Credit-Review_STVA' AND page_name_current = 'OneTime-wAutoPay-Credit-Confirm_STVA') THEN "3a"
            WHEN (page_name_previous = 'OneTime-wAutoPay-Checking-Review_STVA' AND page_name_current = 'OneTime-wAutoPay-Checking-Confirm_STVA') THEN "3b"
            WHEN (page_name_previous = 'OneTime-wAutoPay-Savings-Review_STVA' AND page_name_current = 'OneTime-wAutoPay-Savings-Confirm_STVA') THEN "3c"
            WHEN (page_name_current = 'OneTime-noAutoPay-Credit-Confirm_STVA') THEN "3d"
            WHEN (page_name_current = 'OneTime-noAutoPay-Checking-Confirm_STVA') THEN "3e"
            WHEN (page_name_current = 'OneTime-noAutoPay-Savings-Confirm_STVA') THEN "3f"
            WHEN (page_name_current = 'AutoPay-wBalance_STVA') THEN "4a"
            WHEN (page_name_current = 'AutoPay-wBalance-Credit-Review_STVA') THEN "5a"
            WHEN (page_name_current = 'AutoPay-wBalance-Checking-Review_STVA') THEN "5b"
            WHEN (page_name_current = 'AutoPay-wBalance-Savings-Review_STVA') THEN "5c"
            WHEN (page_name_previous = 'AutoPay-wBalance-Credit-Review_STVA' AND page_name_current = 'AutoPay-wBalance-Credit-Confirm_STVA') THEN "6a"
            WHEN (page_name_previous = 'AutoPay-wBalance-Checking-Review_STVA' AND page_name_current = 'AutoPay-wBalance-Checking-Confirm_STVA') THEN "6b"
            WHEN (page_name_previous = 'AutoPay-wBalance-Savings-Review_STVA' AND page_name_current = 'AutoPay-wBalance-Savings-Confirm_STVA') THEN "6c"
            WHEN (page_name_current = 'AutoPay-noBalance_STVA') THEN "7a"
            WHEN (page_name_current = 'AutoPay-noBalance-Credit-Review_STVA') THEN "8a"
            WHEN (page_name_current = 'AutoPay-noBalance-Checking-Review_STVA') THEN "8b"
            WHEN (page_name_current = 'AutoPay-noBalance-Savings-Review_STVA') THEN "8c"
            WHEN (page_name_previous = 'AutoPay-noBalance-Credit-Review_STVA' AND page_name_current = 'AutoPay-noBalance-Credit-Confirm_STVA') THEN "9a"
            WHEN (page_name_previous = 'AutoPay-noBalance-Checking-Review_STVA' AND page_name_current = 'AutoPay-noBalance-Checking-Confirm_STVA') THEN "9b"
            WHEN (page_name_previous = 'AutoPay-noBalance-Savings-Review_STVA' AND page_name_current = 'AutoPay-noBalance-Savings-Confirm_STVA') THEN "9c"
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
            AND (state__view__current_page__name = 'OneTime-wAutoPay_STVA'
            OR state__view__current_page__name = 'OneTime-noAutoPay_STVA'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Credit-Review_STVA'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Checking-Review_STVA'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Savings-Review_STVA'
            OR state__view__current_page__name = 'OneTime-noAutoPay-Credit-Review_STVA'
            OR state__view__current_page__name = 'OneTime-noAutoPay-Checking-Review_STVA'
            OR state__view__current_page__name = 'OneTime-noAutoPay-Savings-Review_STVA'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Credit-Confirm_STVA'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Checking-Confirm_STVA'
            OR state__view__current_page__name = 'OneTime-wAutoPay-Savings-Confirm_STVA' 
            OR state__view__current_page__name = 'OneTime-noAutoPay-Credit-Confirm_STVA'
            OR state__view__current_page__name = 'OneTime-noAutoPay-Checking-Confirm_STVA'
            OR state__view__current_page__name = 'OneTime-noAutoPay-Savings-Confirm_STVA'
            OR state__view__current_page__name = 'AutoPay-wBalance_STVA'
            OR state__view__current_page__name = 'AutoPay-wBalance-Credit-Review_STVA'
            OR state__view__current_page__name = 'AutoPay-wBalance-Checking-Review_STVA'
            OR state__view__current_page__name = 'AutoPay-wBalance-Savings-Review_STVA' 
      OR state__view__current_page__name = 'AutoPay-wBalance-Credit-Confirm_STVA'
      OR state__view__current_page__name = 'AutoPay-wBalance-Checking-Confirm_STVA'
      OR state__view__current_page__name = 'AutoPay-wBalance-Savings-Confirm_STVA'              
            OR state__view__current_page__name = 'AutoPay-noBalance_STVA'
            OR state__view__current_page__name = 'AutoPay-noBalance-Credit-Review_STVA' 
            OR state__view__current_page__name = 'AutoPay-noBalance-Checking-Review_STVA'
            OR state__view__current_page__name = 'AutoPay-noBalance-Savings-Review_STVA' 
            OR state__view__current_page__name = 'AutoPay-noBalance-Credit-Confirm_STVA'
            OR state__view__current_page__name = 'AutoPay-noBalance-Checking-Confirm_STVA'
            OR state__view__current_page__name = 'AutoPay-noBalance-Savings-Confirm_STVA') 
            ) as agg_events
        )as agg_windows 
    WHERE
        (page_name_current = 'OneTime-wAutoPay_STVA')
        OR (page_name_current = 'OneTime-noAutoPay_STVA') 
        OR (page_name_current = 'OneTime-wAutoPay-Credit-Review_STVA') 
        OR (page_name_current = 'OneTime-wAutoPay-Checking-Review_STVA') 
        OR (page_name_current = 'OneTime-wAutoPay-Savings-Review_STVA')
        OR (page_name_current = 'OneTime-noAutoPay-Credit-Review_STVA')
        OR (page_name_current = 'OneTime-noAutoPay-Checking-Review_STVA')
        OR (page_name_current = 'OneTime-noAutoPay-Savings-Review_STVA')
        OR (page_name_previous = 'OneTime-wAutoPay-Credit-Review_STVA' AND page_name_current = 'OneTime-wAutoPay-Credit-Confirm_STVA')
        OR (page_name_previous = 'OneTime-wAutoPay-Checking-Review_STVA' AND page_name_current = 'OneTime-wAutoPay-Checking-Confirm_STVA')
        OR (page_name_previous = 'OneTime-wAutoPay-Savings-Review_STVA' AND page_name_current = 'OneTime-wAutoPay-Savings-Confirm_STVA')
        OR (page_name_current = 'OneTime-noAutoPay-Credit-Confirm_STVA')
        OR (page_name_current = 'OneTime-noAutoPay-Checking-Confirm_STVA')
        OR (page_name_current = 'OneTime-noAutoPay-Savings-Confirm_STVA')
        OR (page_name_current = 'AutoPay-wBalance_STVA')
        OR (page_name_current = 'AutoPay-wBalance-Credit-Review_STVA') 
        OR (page_name_current = 'AutoPay-wBalance-Checking-Review_STVA') 
        OR (page_name_current = 'AutoPay-wBalance-Savings-Review_STVA')
        OR (page_name_previous = 'AutoPay-wBalance-Credit-Review_STVA' AND page_name_current = 'AutoPay-wBalance-Credit-Confirm_STVA')
        OR (page_name_previous = 'AutoPay-wBalance-Checking-Review_STVA' AND page_name_current = 'AutoPay-wBalance-Checking-Confirm_STVA')
        OR (page_name_previous = 'AutoPay-wBalance-Savings-Review_STVA' AND page_name_current = 'AutoPay-wBalance-Savings-Confirm_STVA')
        OR (page_name_current = 'AutoPay-noBalance_STVA')
        OR (page_name_current = 'AutoPay-noBalance-Credit-Review_STVA') 
        OR (page_name_current = 'AutoPay-noBalance-Checking-Review_STVA') 
        OR (page_name_current = 'AutoPay-noBalance-Savings-Review_STVA')
        OR (page_name_previous = 'AutoPay-noBalance-Credit-Review_STVA' AND page_name_current = 'AutoPay-noBalance-Credit-Confirm_STVA')
        OR (page_name_previous = 'AutoPay-noBalance-Checking-Review_STVA' AND page_name_current = 'AutoPay-noBalance-Checking-Confirm_STVA')
        OR (page_name_previous = 'AutoPay-noBalance-Savings-Review_STVA' AND page_name_current = 'AutoPay-noBalance-Savings-Confirm_STVA')
    ) as agg_scenarios
GROUP BY
    year_month,
    step
ORDER BY
    year_month,
    step
;
