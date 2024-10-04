SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

use ${env:ENVIRONMENT};

SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.execution.engine=mr;

INSERT INTO TABLE net_auth_flow_page_visits_monthly PARTITION(year_month)
SELECT
    auth_flow,
    page_name,
    COUNT(DISTINCT hh) AS count_hhs,
    COUNT(DISTINCT user_name) AS count_user_names,
    COUNT(DISTINCT visit_id) AS count_visits,
    COUNT(DISTINCT device_id) AS count_visitors,
    COUNT(*) AS count_page_views,
    year_month
FROM
    (SELECT
        regexp_extract(ne.partition_date,'(.*\-.*)\-.*',1) AS year_month,
        'Create Username' AS auth_flow,
        ne.visit__account__enc_account_number AS hh,
        ne.visit__user__enc_id AS user_name,
        ne.state__view__current_page__name AS page_name,
        ne.visit__visit_id AS visit_id,
        ne.visit__device__enc_uuid AS device_id,
        ne.message__sequence_number AS seq_number
    FROM
        net_events AS ne
    WHERE
        ne.partition_date BETWEEN '${hiveconf:MONTH_START_DATE}' AND '${hiveconf:MONTH_END_DATE}'
        AND (LOWER(state__view__current_page__name) = "my-account.create-id-1."
        OR LOWER(state__view__current_page__name) = "my-account.create-id-2."

        OR LOWER(state__view__current_page__name) = "my-account.create-id-1.bam"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-1.bam_stva"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-1a.bam"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-1a.bam_stva"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-2.bam"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-2.bam_stva"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-final.bam"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-final.bam_stva"

        OR LOWER(state__view__current_page__name) = "my-account.create-id-1a.btm"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-2.btm"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-2.btm_stva"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-final.btm"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-final.btm_stva"

        OR LOWER(state__view__current_page__name) = "my-account.create-id-1.nbtm"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-1.nbtm_stva"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-1a.nbtm"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-1a.nbtm_stva"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-2.nbtm"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-2.nbtm_stva"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-final.nbtm"
        OR LOWER(state__view__current_page__name) = "my-account.create-id-final.nbtm_stva")
    ) agg_events
GROUP BY
    auth_flow,
    year_month,
    page_name
ORDER BY
    auth_flow,
    year_month,
    page_name
;

INSERT INTO TABLE net_auth_flow_page_visits_monthly PARTITION(year_month)
SELECT
    auth_flow,
    page_name,
    COUNT(DISTINCT hh) AS count_hhs,
    COUNT(DISTINCT user_name) AS count_user_names,
    COUNT(DISTINCT visit_id) AS count_visits,
    COUNT(DISTINCT device_id) AS count_visitors,
    COUNT(*) AS count_page_views,
    year_month
FROM
    (SELECT
        regexp_extract(ne.partition_date,'(.*\-.*)\-.*',1) AS year_month,
        'Recover Username' AS auth_flow,        
        ne.visit__account__enc_account_number AS hh,
        ne.visit__user__enc_id AS user_name,
        ne.state__view__current_page__name AS page_name,
        ne.visit__visit_id AS visit_id,
        ne.visit__device__enc_uuid AS device_id,
        ne.message__sequence_number AS seq_number
    FROM
        net_events AS ne
    WHERE
        ne.partition_date BETWEEN '${hiveconf:MONTH_START_DATE}' AND '${hiveconf:MONTH_END_DATE}'
        AND (LOWER(state__view__current_page__name) = "login"
    
        OR LOWER(state__view__current_page__name) = "recover-id-1.btm"
        OR LOWER(state__view__current_page__name) = "recover-id-final.btm"
        OR LOWER(state__view__current_page__name) = "recover-id-1.btm_stva"
        OR LOWER(state__view__current_page__name) = "recover-id-final.btm_stva"
    
        OR LOWER(state__view__current_page__name) = "recover-id-1.nbtm"
        OR LOWER(state__view__current_page__name) = "recover-id-2.nbtm"
        OR LOWER(state__view__current_page__name) = "recover-id-3.nbtm"
        OR LOWER(state__view__current_page__name) = "recover-id-final.nbtm"
        OR LOWER(state__view__current_page__name) = "recover-id-3c.nbtm"
        OR LOWER(state__view__current_page__name) = "recover-id-3a.nbtm"
        OR LOWER(state__view__current_page__name) = "recover-id-3b.nbtm"
        OR LOWER(state__view__current_page__name) = "recover-id-3b1.nbtm"
        OR LOWER(state__view__current_page__name) = "recover-id-1.nbtm_stva"
        OR LOWER(state__view__current_page__name) = "recover-id-2.nbtm_stva"
        OR LOWER(state__view__current_page__name) = "recover-id-3.nbtm_stva"
        OR LOWER(state__view__current_page__name) = "recover-id-final.nbtm_stva"
        OR LOWER(state__view__current_page__name) = "recover-id-3c.nbtm_stva"
        OR LOWER(state__view__current_page__name) = "recover-id-3a.nbtm_stva"
        OR LOWER(state__view__current_page__name) = "recover-id-3b.nbtm_stva"
        OR LOWER(state__view__current_page__name) = "recover-id-3b1.nbtm_stva")
    ) agg_events
GROUP BY
    auth_flow,
    year_month,
    page_name
ORDER BY
    auth_flow,
    year_month,
    page_name
;

INSERT INTO TABLE net_auth_flow_page_visits_monthly PARTITION(year_month)
SELECT
    auth_flow,
    page_name,
    COUNT(DISTINCT hh) AS count_hhs,
    COUNT(DISTINCT user_name) AS count_user_names,
    COUNT(DISTINCT visit_id) AS count_visits,
    COUNT(DISTINCT device_id) AS count_visitors,
    COUNT(*) AS count_page_views,
    year_month
FROM
    (SELECT
        regexp_extract(ne.partition_date,'(.*\-.*)\-.*',1) AS year_month,
        'Recover Password' AS auth_flow,        
        ne.visit__account__enc_account_number AS hh,
        ne.visit__user__enc_id AS user_name,
        ne.state__view__current_page__name AS page_name,
        ne.visit__visit_id AS visit_id,
        ne.visit__device__enc_uuid AS device_id,
        ne.message__sequence_number AS seq_number
    FROM
        net_events AS ne
    WHERE
        ne.partition_date BETWEEN '${hiveconf:MONTH_START_DATE}' AND '${hiveconf:MONTH_END_DATE}'
        AND (LOWER(state__view__current_page__name) = "reset-password.1"
        OR LOWER(state__view__current_page__name) = "reset-password.1_stva"
        OR LOWER(state__view__current_page__name) = "reset-password.1a"
        OR LOWER(state__view__current_page__name) = "reset-password.1b"
        OR LOWER(state__view__current_page__name) = "reset-password.1b_stva"
        OR LOWER(state__view__current_page__name) = "reset-password.2"
        OR LOWER(state__view__current_page__name) = "reset-password.2_stva"
        OR LOWER(state__view__current_page__name) = "reset-password.2a"
        OR LOWER(state__view__current_page__name) = "reset-password.2a_stva"
        OR LOWER(state__view__current_page__name) = "reset-password.2b"
        OR LOWER(state__view__current_page__name) = "reset-password.2b1"
        OR LOWER(state__view__current_page__name) = "reset-password.2c"
        OR LOWER(state__view__current_page__name) = "reset-password.2c_stva"
        OR LOWER(state__view__current_page__name) = "reset-password.3"
        OR LOWER(state__view__current_page__name) = "reset-password.3_stva"
        OR LOWER(state__view__current_page__name) = "reset-password.final"
        OR LOWER(state__view__current_page__name) = "reset-password.final2"
        OR LOWER(state__view__current_page__name) = "reset-password.final_stva"
        OR LOWER(state__view__current_page__name) = "reset-password.final2_stva"
        OR LOWER(state__view__current_page__name) = "reset-password.2b_stva"
        OR LOWER(state__view__current_page__name) = "reset-password.2b1_stva")
    ) agg_events
GROUP BY
    auth_flow,
    year_month,
    page_name
ORDER BY
    auth_flow,
    year_month,
    page_name
;
