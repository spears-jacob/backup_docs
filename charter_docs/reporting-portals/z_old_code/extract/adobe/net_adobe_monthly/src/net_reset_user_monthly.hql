SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

DROP TABLE IF EXISTS ${env:TMP_db}.visit_ids;
CREATE TABLE ${env:TMP_db}.visit_ids AS
SELECT 
    distinct visit__visit_id
FROM
    net_events 
WHERE
    partition_date BETWEEN '${hiveconf:MONTH_START_DATE}' AND '${hiveconf:MONTH_END_DATE}'
     AND (state__view__current_page__name = "recover-id-1.btm" OR 
    state__view__previous_page__name = "recover-id-1.btm" OR 
    state__view__current_page__name = "recover-id-1.nbtm" OR 
    state__view__previous_page__name = "recover-id-1.nbtm");

CREATE TABLE IF NOT EXISTS net_reset_user_monthly (run_year string, run_month string, step string, count_hhs int, count_user_names int, count_visits int, count_visitors int, count_page_views int);

INSERT INTO TABLE net_reset_user_monthly
SELECT
    run_year,
    run_month,
    step,
    COUNT(distinct hh) as count_hhs,
    COUNT(distinct user_name) as count_user_names,
    COUNT(DISTINCT visit_id) as count_visits,
    COUNT(DISTINCT device_id) as count_visitors,
    COUNT(*) as count_page_views
FROM
    (SELECT
        year(run_date) as run_year,
        month(run_date) as run_month,
        page_name_previous,
        page_name_current, 
        hh,
        user_name,
        visit_id,
        device_id,

        CASE
            WHEN (message_category = "Custom Link" AND page_name_current = "recover-id-1.btm") THEN "1"
            WHEN (page_name_previous = "recover-id-1.btm" AND page_name_current = "recover-id-final.btm") THEN "2"
            WHEN (message_category = "Custom Link" AND page_name_current = "recover-id-1.nbtm") THEN "3"
            WHEN (page_name_previous = "recover-id-1.nbtm" AND page_name_current = "recover-id-2.nbtm") THEN "4"
            WHEN (page_name_previous = "recover-id-2.nbtm" AND page_name_current = "recover-id-3.nbtm") THEN "5"
            WHEN (page_name_previous = "recover-id-3.nbtm" AND page_name_current = "recover-id-final.nbtm") THEN "6"
            WHEN (page_name_previous = "recover-id-3.nbtm" AND page_name_current = "recover-id-3a.nbtm") THEN "7"
            WHEN (page_name_previous = "recover-id-3.nbtm" AND page_name_current = "recover-id-3c.nbtm") THEN "8"
            WHEN (page_name_previous = "recover-id-3a.nbtm" AND page_name_current = "recover-id-3b.nbtm") THEN "9"
            WHEN (page_name_previous = "recover-id-3a.nbtm" AND page_name_current = "recover-id-3b1.nbtm") THEN "10"
            WHEN (page_name_previous = "recover-id-3b.nbtm" AND page_name_current = "recover-id-final.nbtm") THEN "11"
            WHEN (page_name_previous = "recover-id-3b1.nbtm" AND page_name_current = "recover-id-final.nbtm") THEN "12"
            WHEN (page_name_previous = "recover-id-3c.nbtm" AND page_name_current = "recover-id-3.nbtm") THEN "13"
            WHEN (page_name_previous = "recover-id-3a.nbtm" AND page_name_current = "recover-id-3c.nbtm") THEN "14"
            WHEN (page_name_previous = "recover-id-2.nbtm" AND page_name_current = "recover-id-3c.nbtm") THEN "15"
            WHEN (page_name_previous = "recover-id-1.btm" AND page_name_current = "recover-id-1.nbtm") THEN "16"
            ELSE "0"
        END as step
    FROM
        (SELECT
            run_date,
            lag(page_name, 1) over (partition by visit_id order by seq_number) as page_name_previous,
            page_name as page_name_current,
            hh,
            user_name,
            visit_id,
            device_id,
            message_name,
            message_category
        FROM
            (SELECT
                ne.partition_date as run_date,
                ne.visit__account__enc_account_number as hh,
                ne.visit__user__enc_id as user_name,
                ne.state__view__current_page__name as page_name,
                ne.visit__visit_id as visit_id,
                ne.visit__device__enc_uuid as device_id,
                ne.message__sequence_number as seq_number,
                ne.message__name as message_name,
                ne.message__category as message_category
            FROM
                net_events as ne
                INNER JOIN ${env:TMP_db}.visit_ids tmp
                ON ne.visit__visit_id = tmp.visit__visit_id
            WHERE
                ne.partition_date BETWEEN '${hiveconf:MONTH_START_DATE}' AND '${hiveconf:MONTH_END_DATE}'
                AND (state__view__current_page__name = "recover-id-1.btm"
                OR state__view__current_page__name = "recover-id-final.btm"
                OR state__view__current_page__name = "recover-id-1.nbtm"
                OR state__view__current_page__name = "recover-id-2.nbtm"
                OR state__view__current_page__name = "recover-id-3.nbtm"
                OR state__view__current_page__name = "recover-id-final.nbtm"
                OR state__view__current_page__name = "recover-id-3a.nbtm"
                OR state__view__current_page__name = "recover-id-3c.nbtm"
                OR state__view__current_page__name = "recover-id-3b.nbtm"
                OR state__view__current_page__name = "recover-id-3b1.nbtm"
                OR (message__name = "Recover ID SRT" AND message__category = "Custom Link"))
            ) as agg_events
        )as agg_windows
    WHERE
            (message_category = "Custom Link" AND page_name_current = "recover-id-1.btm") 
            OR (page_name_previous = "recover-id-1.btm" AND page_name_current = "recover-id-final.btm") 
            OR (message_category = "Custom Link" AND page_name_current = "recover-id-1.nbtm")  
            OR (page_name_previous = "recover-id-1.nbtm" AND page_name_current = "recover-id-2.nbtm")  
            OR (page_name_previous = "recover-id-2.nbtm" AND page_name_current = "recover-id-3.nbtm") 
            OR (page_name_previous = "recover-id-3.nbtm" AND page_name_current = "recover-id-final.nbtm") 
            OR (page_name_previous = "recover-id-3.nbtm" AND page_name_current = "recover-id-3a.nbtm") 
            OR (page_name_previous = "recover-id-3.nbtm" AND page_name_current = "recover-id-3c.nbtm") 
            OR (page_name_previous = "recover-id-3a.nbtm" AND page_name_current = "recover-id-3b.nbtm") 
            OR (page_name_previous = "recover-id-3a.nbtm" AND page_name_current = "recover-id-3b1.nbtm") 
            OR (page_name_previous = "recover-id-3b.nbtm" AND page_name_current = "recover-id-final.nbtm") 
            OR (page_name_previous = "recover-id-3b1.nbtm" AND page_name_current = "recover-id-final.nbtm") 
            OR (page_name_previous = "recover-id-3c.nbtm" AND page_name_current = "recover-id-3.nbtm")
            OR (page_name_previous = "recover-id-3a.nbtm" AND page_name_current = "recover-id-3c.nbtm")
            OR (page_name_previous = "recover-id-2.nbtm" AND page_name_current = "recover-id-3c.nbtm") 
            OR (page_name_previous = "recover-id-1.btm" AND page_name_current = "recover-id-1.nbtm")
    ) as agg_scenarios
GROUP BY
    run_year,
    run_month,
    step
ORDER BY
    run_year,
    run_month,
    step;

DROP TABLE IF EXISTS ${env:TMP_db}.visit_ids;
