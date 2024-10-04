SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

use ${env:ENVIRONMENT};

DROP TABLE IF EXISTS ${env:TMP_db}.visit_ids;
CREATE TABLE ${env:TMP_db}.visit_ids AS
SELECT 
    distinct visit__visit_id
FROM
    net_events 
WHERE partition_date BETWEEN '${hiveconf:MONTH_START_DATE}' AND '${hiveconf:MONTH_END_DATE}'
    AND (state__view__current_page__name = "my-account.create-id-1.nbtm" OR 
    state__view__previous_page__name = "my-account.create-id-1.nbtm" OR
    state__view__current_page__name = "my-account.create-id-1.bam" OR 
    state__view__previous_page__name = "my-account.create-id-1.bam"); 

CREATE TABLE IF NOT EXISTS net_create_username_monthly (run_year string, run_month string, step string, count_hhs int, count_user_names int, count_visits int, count_visitors int, count_page_views int);

INSERT INTO TABLE net_create_username_monthly
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
        run_year,
        run_month,
        page_name_previous,
        page_name_current,
        hh,
        user_name,
        visit_id,
        device_id,

        CASE
            WHEN (page_name_previous = "my-account.create-id-1.bam" AND page_name_current = "my-account.create-id-2.btm") THEN "1"
            WHEN (page_name_previous = "my-account.create-id-2.btm" AND page_name_current = "my-account.create-id-final.btm") THEN "2"
            WHEN (page_name_previous = "my-account.create-id-1.bam" AND page_name_current = "my-account.create-id-2.bam") THEN "3"
            WHEN (page_name_previous = "my-account.create-id-2.bam" AND page_name_current = "my-account.create-id-final.bam") THEN "4"
            WHEN (page_name_previous = "my-account.create-id-1.bam" AND page_name_current = "my-account.create-id-1a.bam") THEN "5"
            WHEN (page_name_previous = "my-account.create-id-1a.bam" AND page_name_current = "my-account.create-id-2.bam") THEN "6"
            WHEN (page_name_current = "my-account.create-id-1.bam") THEN "11"
            WHEN (page_name_previous = "my-account.create-id-1.nbtm" AND page_name_current = "my-account.create-id-1a.nbtm") THEN "7"
            WHEN (page_name_previous = "my-account.create-id-1.nbtm" AND page_name_current = "my-account.create-id-2.nbtm") THEN "8"
            WHEN (page_name_previous = "my-account.create-id-1a.nbtm" AND page_name_current = "my-account.create-id-2.nbtm") THEN "9"
            WHEN (page_name_previous = "my-account.create-id-2.nbtm" AND page_name_current = "my-account.create-id-final.nbtm") THEN "10"
            WHEN (page_name_current = "my-account.create-id-1.nbtm") THEN "12"
            ELSE "0"
        END as step
    FROM
        (SELECT
            year(run_date) as run_year,
            month(run_date) as run_month,
            run_date,
            lag(page_name, 1) over (partition by visit_id order by seq_number) as page_name_previous,
            page_name as page_name_current,
            hh,
            user_name,
            visit_id,
            device_id
        FROM
            (SELECT
                ne.partition_date as run_date,
                ne.visit__account__enc_account_number as hh,
                ne.visit__user__enc_id as user_name,
                ne.state__view__current_page__name as page_name,
                ne.visit__visit_id as visit_id,
                ne.visit__device__enc_uuid as device_id,
                ne.message__sequence_number as seq_number
            FROM
                net_events as ne
                INNER JOIN ${env:TMP_db}.visit_ids tmp
                ON ne.visit__visit_id = tmp.visit__visit_id
            WHERE
                ne.partition_date BETWEEN '${hiveconf:MONTH_START_DATE}' AND '${hiveconf:MONTH_END_DATE}'
                AND (state__view__current_page__name = "my-account.create-id-1.bam"
                OR state__view__current_page__name = "my-account.create-id-1a.bam"
                OR state__view__current_page__name = "my-account.create-id-2.bam"
                OR state__view__current_page__name = "my-account.create-id-final.bam"
                OR state__view__current_page__name = "my-account.create-id-2.btm"
                OR state__view__current_page__name = "my-account.create-id-final.btm"
                OR state__view__current_page__name = "my-account.create-id-1.nbtm"
                OR state__view__current_page__name = "my-account.create-id-1a.nbtm"
                OR state__view__current_page__name = "my-account.create-id-2.nbtm"
                OR state__view__current_page__name = "my-account.create-id-final.nbtm")
            ) as agg_events
        )as agg_windows
    WHERE
            (page_name_previous = "my-account.create-id-1.bam" AND page_name_current = "my-account.create-id-2.btm")
            OR (page_name_previous = "my-account.create-id-2.btm" AND page_name_current = "my-account.create-id-final.btm")
            OR (page_name_previous = "my-account.create-id-1.bam" AND page_name_current = "my-account.create-id-2.bam")
            OR (page_name_previous = "my-account.create-id-2.bam" AND page_name_current = "my-account.create-id-final.bam")
            OR (page_name_previous = "my-account.create-id-1.bam" AND page_name_current = "my-account.create-id-1a.bam")
            OR (page_name_previous = "my-account.create-id-1a.bam" AND page_name_current = "my-account.create-id-2.bam")
            OR (page_name_current = "my-account.create-id-1.bam")
            OR (page_name_previous = "my-account.create-id-1.nbtm" AND page_name_current = "my-account.create-id-1a.nbtm")
            OR (page_name_previous = "my-account.create-id-1.nbtm" AND page_name_current = "my-account.create-id-2.nbtm")
            OR (page_name_previous = "my-account.create-id-1a.nbtm" AND page_name_current = "my-account.create-id-2.nbtm")
            OR (page_name_previous = "my-account.create-id-2.nbtm" AND page_name_current = "my-account.create-id-final.nbtm")
            OR (page_name_current = "my-account.create-id-1.nbtm")
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