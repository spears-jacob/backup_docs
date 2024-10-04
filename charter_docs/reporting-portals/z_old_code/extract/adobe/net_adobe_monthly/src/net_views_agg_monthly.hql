SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

use ${env:ENVIRONMENT};

SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.exec.parallel=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
--SET hive.tez.container.size=8192;
--SET hive.tez.java.opts=-Xmx6554m;

DROP TABLE IF EXISTS ${env:TMP_db}.net_views_monthly;
CREATE TABLE ${env:TMP_db}.net_views_monthly AS
    SELECT
        ne.*
    FROM
        net_events ne
    WHERE
        ne.partition_date IS NOT NULL AND
        ne.partition_date BETWEEN '${hiveconf:LAST_MONTH_BEGIN_DATE}' AND '${hiveconf:LAST_MONTH_END_DATE}'
;

DROP TABLE IF EXISTS ${env:TMP_db}.viewers_net_monthly;
CREATE TABLE ${env:TMP_db}.viewers_net_monthly AS
SELECT 
    '${hiveconf:LAST_YEAR_MONTH}' as year_month, 
    sum(agg.total_views_last_month) as total_views_last_month
FROM
    (
    SELECT
         viewers_id,
        SUM(IF(total_views_last_month > 0, 1, 0)) AS total_views_last_month
    FROM (
        SELECT
            visit__device__enc_uuid AS viewers_id,
            state__content__stream__view_id AS view_id,
            count(*) AS total_views_last_month
        FROM
            ${env:TMP_db}.net_views_monthly
        WHERE
            visit__device__enc_uuid IS NOT NULL AND
            visit__device__enc_uuid <> "" 
        GROUP BY visit__device__enc_uuid, state__content__stream__view_id
    ) AS viewers_net_temp
    GROUP BY viewers_id
    ) agg
;


DROP TABLE IF EXISTS ${env:TMP_db}.net_monthly_views_aggs;
CREATE TABLE ${env:TMP_db}.net_monthly_views_aggs AS
SELECT
    '${hiveconf:LAST_YEAR_MONTH}' as year_month,
    MAX(visit__visit_id) AS visit_id,
    state__content__stream__view_id AS view_id,
    MAX(visit__user__enc_id) AS customer__user_id, 
    MAX(visit__account__enc_account_number) AS customer__account_id, 
    IF(MAX(visit__connection__network_status) == "OnNet", true, false) AS customer__onNet, 
    MAX(visit__account__subscription__service_level) AS customer__subscriptionType, 
    MAX(visit__device__enc_uuid) AS device__id, 
    (MAX(visit__location__country_name)) AS location__country_name, 
    (MAX(visit__location__region)) AS location__region, 
    (MAX(visit__location__enc_city)) AS location__city, 
    (MAX(visit__location__state)) AS location__state, 
    (MAX(visit__location__enc_zip_code)) AS location__zip_code, 
    (MAX(state__content__details__type)) AS program__type, 
    (MAX(state__content__details__title)) AS program__title, 
    (MAX(state__content__details__genres)) AS program__genres, 
    (MAX(state__content__programmer__linear__network_name)) AS programmer__linear__network_name, 
    (MAX(state__content__programmer__linear__channel_category)) AS programmer__linear__channel_category, 
    (MAX(state__content__programmer__linear__channel_number)) AS programmer__linear__channel_number, 
    (MAX(visit__connection__type)) AS network_type,
    SUM(IF(message__name == "Play Load Failure",1,0)) AS total_stream_failures
FROM
    ${env:TMP_db}.net_views_monthly
GROUP BY
    state__content__stream__view_id;

DROP TABLE IF EXISTS ${env:TMP_db}.net_views_monthly_gral_aggs;
CREATE TABLE ${env:TMP_db}.net_views_monthly_gral_aggs AS
SELECT
    year_month,
    COUNT(distinct visit_id) AS visits,
    COUNT(distinct view_id) AS views,
    COUNT(distinct device__id) AS visitors,
    COUNT(distinct customer__user_id) AS customer__user_ids, 
    COUNT(distinct customer__account_id) AS customer__households, 
    CountArrayToMap(COLLECT_LIST(customer__onNet)) AS customer__onNet, 
    CountArrayToMap(COLLECT_LIST(customer__subscriptionType)) AS customer__subscriptionType, 
    CountArrayToMap(COLLECT_LIST(location__country_name)) AS location__country_name, 
    CountArrayToMap(COLLECT_LIST(location__region)) AS location__region, 
    CountArrayToMap(COLLECT_LIST(location__city)) AS location__city, 
    CountArrayToMap(COLLECT_LIST(location__state)) AS location__state, 
    CountArrayToMap(COLLECT_LIST(location__zip_code)) AS location__zip_code, 
    CountArrayToMap(COLLECT_LIST(program__type)) AS program__type, 
    CountArrayToMap(COLLECT_LIST(program__title)) AS program__title, 
    CountArrayToMap(COLLECT_LIST(key1)) AS program__genres, 
    CountArrayToMap(COLLECT_LIST(programmer__linear__network_name)) AS programmer__linear__network_name, 
    CountArrayToMap(COLLECT_LIST(programmer__linear__channel_category)) AS programmer__linear__channel_category, 
    CountArrayToMap(COLLECT_LIST(programmer__linear__channel_number)) AS programmer__linear__channel_number, 
    CountArrayToMap(COLLECT_LIST(network_type)) AS network_type,
    SUM(total_stream_failures) AS total_stream_failures
FROM
    ${env:TMP_db}.net_monthly_views_aggs
 LATERAL VIEW EXPLODE(program__genres) program__genres_exploded as key1
GROUP BY
    year_month
;

INSERT INTO TABLE net_views_agg_monthly 
SELECT 
    agg.*,
    tmp.total_views_last_month
FROM
    ${env:TMP_db}.net_views_monthly_gral_aggs agg
LEFT OUTER JOIN
    ${env:TMP_db}.viewers_net_monthly tmp ON (tmp.year_month = agg.year_month)
;

DROP TABLE IF EXISTS ${env:TMP_db}.net_views_monthly;
DROP TABLE IF EXISTS ${env:TMP_db}.viewers_net_monthly;
DROP TABLE IF EXISTS ${env:TMP_db}.net_monthly_views_aggs;
DROP TABLE IF EXISTS ${env:TMP_db}.net_views_monthly_gral_aggs;
