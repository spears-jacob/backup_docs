SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.exec.parallel=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
--SET hive.tez.container.size=8192;
--SET hive.tez.java.opts=-Xmx6554m;

DROP TABLE IF EXISTS ${env:TMP_db}.net_events_monthly;
CREATE TABLE ${env:TMP_db}.net_events_monthly AS
    SELECT
        ne.*
    FROM
        net_events ne
    WHERE
        ne.partition_date IS NOT NULL AND
        ne.partition_date BETWEEN '${hiveconf:LAST_MONTH_BEGIN_DATE}' AND '${hiveconf:LAST_MONTH_END_DATE}'
;

DROP TABLE IF EXISTS ${env:TMP_db}.net_monthly_gral_aggs;
CREATE TABLE ${env:TMP_db}.net_monthly_gral_aggs AS
SELECT
    CONCAT(YEAR(partition_date), '-', LPAD(MONTH(partition_date),2,'0')) AS year_month,
    COUNT(distinct visit__visit_id) AS visits,
    COUNT(distinct visit__device__enc_uuid) AS visitors,
    COUNT(distinct visit__user__enc_id) AS customer__user_ids, 
    COUNT(distinct visit__account__enc_account_number) AS customer__households, 
    COUNT(visit__user__is_test_user) AS customer__test_accounts 
FROM
    ${env:TMP_db}.net_events_monthly
WHERE
    partition_date IS NOT NULL
GROUP BY
    CONCAT(YEAR(partition_date), '-', LPAD(MONTH(partition_date),2,'0'))
;

DROP TABLE IF EXISTS ${env:TMP_db}.net_monthly_visits_aggs;
CREATE TABLE ${env:TMP_db}.net_monthly_visits_aggs AS
SELECT
    CONCAT(YEAR(partition_date), '-', LPAD(MONTH(partition_date),2,'0')) AS year_month,
    visit__account__enc_account_number, 
    visit__visit_id,
    COLLECT_SET(visit__connection__network_status) AS customer__network_status, 
    COLLECT_SET(visit__connection__type) AS network_type,
    COLLECT_SET(visit__account__subscription__service_level) AS customer__subscriptionTypes, 
    COLLECT_SET(visit__device__model) AS device__models, 
    COLLECT_SET(visit__device__operating_system) AS device__os, 
    COLLECT_SET(visit__location__country_code) AS location__country_code, 
    COLLECT_SET(visit__location__country_name) AS location__country_name, 
    COLLECT_SET(visit__location__region) AS location__region, 
    COLLECT_SET(visit__location__region_name) AS location__region_name, 
    COLLECT_SET(visit__location__enc_city) AS location__city, 
    COLLECT_SET(visit__location__state) AS location__state, 
    COLLECT_SET(visit__location__enc_zip_code) AS location__zip_code, 
    IF(SUBSTR(MAX(visit__visit_id), (LENGTH(MAX(visit__device__enc_uuid)) + 1)) > 1, 0, 1) AS first_time_users,
    CAST(SUM(IF(LOWER(message__name) LIKE "search%", 1, 0)) AS INT) AS total_conversions_by_type,
    COLLECT_SET(state__view__current_page__section) AS total_page_views_by_section,
    IF((MAX(visit__login__failed_attempts) IS NULL), 0, MAX(visit__login__failed_attempts)) AS total_login_failures
FROM
    ${env:TMP_db}.net_events_monthly
WHERE visit__account__enc_account_number is not null 
    AND visit__visit_id is not null
GROUP BY
    CONCAT(YEAR(partition_date), '-', LPAD(MONTH(partition_date),2,'0')),
    visit__account__enc_account_number,
    visit__visit_id
;

DROP TABLE IF EXISTS ${env:TMP_db}.net_monthly_hhs_aggs;
CREATE TABLE ${env:TMP_db}.net_monthly_hhs_aggs AS
SELECT
    year_month,
    visit__account__enc_account_number, 
    CountArrayToMap(COLLECT_LIST(key1)) AS customer__network_status, 
    CountArrayToMap(COLLECT_LIST(key2)) AS network_type,
    CountArrayToMap(COLLECT_LIST(key3)) AS customer__subscriptionTypes,
    CountArrayToMap(COLLECT_LIST(key5)) AS device__os, 
    CountArrayToMap(COLLECT_LIST(key7)) AS location__country_name, 
    CountArrayToMap(COLLECT_LIST(key8)) AS location__region, 
    CountArrayToMap(COLLECT_LIST(key10)) AS location__city, 
    CountArrayToMap(COLLECT_LIST(key11)) AS location__state, 
    CountArrayToMap(COLLECT_LIST(key12)) AS location__zip_code, 
    CountArrayToMap(COLLECT_LIST(key13)) AS total_page_views_by_section,
    SUM(first_time_users) AS first_time_users,
    SUM(total_conversions_by_type) AS total_conversions_by_type,
    SUM(total_login_failures) AS total_login_failures
FROM
    ${env:TMP_db}.net_monthly_visits_aggs
 LATERAL VIEW EXPLODE(customer__network_status) customer__network_status_exploded as key1
 LATERAL VIEW EXPLODE(network_type) network_type_exploded as key2
 LATERAL VIEW EXPLODE(customer__subscriptionTypes) customer__subscriptionTypes_exploded as key3
 LATERAL VIEW EXPLODE(device__os) device__os_exploded as key5
 LATERAL VIEW EXPLODE(location__country_name) location__country_name_exploded as key7
 LATERAL VIEW EXPLODE(location__region) location__region_exploded as key8
 LATERAL VIEW EXPLODE(location__city) location__city_exploded as key10
 LATERAL VIEW EXPLODE(location__state) location__state_exploded as key11
 LATERAL VIEW EXPLODE(location__zip_code) location__zip_code_exploded as key12
 LATERAL VIEW EXPLODE(total_page_views_by_section) total_page_views_by_section_exploded as key13
GROUP BY
    year_month,
    visit__account__enc_account_number
;

DROP TABLE IF EXISTS ${env:TMP_db}.net_01_customer_network_status;
CREATE TABLE ${env:TMP_db}.net_01_customer_network_status AS
SELECT year_month AS year_month, collect_set(customer__network_status) AS customer__network_status
FROM (
SELECT '${hiveconf:LAST_YEAR_MONTH}' AS year_month, CONCAT(key01, ":", CAST(sum(value01) AS STRING)) as customer__network_status
FROM  
    (SELECT explode(customer__network_status) AS (key01, value01) FROM ${env:TMP_db}.net_monthly_hhs_aggs) tmp01
GROUP BY key01
) agg01
group by year_month;

DROP TABLE IF EXISTS ${env:TMP_db}.net_02_network_type;
CREATE TABLE ${env:TMP_db}.net_02_network_type AS
SELECT year_month AS year_month, collect_set(network_type) AS network_type
FROM (
SELECT '${hiveconf:LAST_YEAR_MONTH}' AS year_month, CONCAT(key02, ":", CAST(sum(value02) AS STRING)) as network_type
FROM  
    (SELECT explode(network_type) AS (key02, value02) FROM ${env:TMP_db}.net_monthly_hhs_aggs) tmp02
GROUP BY key02
) agg02
group by year_month;

DROP TABLE IF EXISTS ${env:TMP_db}.net_03_customer_subscriptionTypes;
CREATE TABLE ${env:TMP_db}.net_03_customer_subscriptionTypes AS
SELECT year_month AS year_month, collect_set(customer__subscriptionTypes) AS customer__subscriptionTypes
FROM (
SELECT '${hiveconf:LAST_YEAR_MONTH}' AS year_month, CONCAT(key03, ":", CAST(sum(value03) AS STRING)) as customer__subscriptionTypes
FROM  
    (SELECT explode(customer__subscriptionTypes) AS (key03, value03) FROM ${env:TMP_db}.net_monthly_hhs_aggs) tmp03
GROUP BY key03
) agg03
group by year_month;

DROP TABLE IF EXISTS ${env:TMP_db}.net_04_device_os;
CREATE TABLE ${env:TMP_db}.net_04_device_os AS
SELECT year_month AS year_month, collect_set(device__os) AS device__os
FROM (
SELECT '${hiveconf:LAST_YEAR_MONTH}' AS year_month, CONCAT(key04, ":", CAST(sum(value04) AS STRING)) as device__os
FROM  
 (SELECT explode(device__os) AS (key04, value04) FROM ${env:TMP_db}.net_monthly_hhs_aggs) tmp04
GROUP BY key04
) agg04
group by year_month;

DROP TABLE IF EXISTS ${env:TMP_db}.net_05_location_country_name;
CREATE TABLE ${env:TMP_db}.net_05_location_country_name AS
SELECT year_month AS year_month, collect_set(location__country_name) AS location__country_name
FROM (
SELECT '${hiveconf:LAST_YEAR_MONTH}' AS year_month, CONCAT(key05, ":", CAST(sum(value05) AS STRING)) as location__country_name
FROM  
 (SELECT explode(location__country_name) AS (key05, value05) FROM ${env:TMP_db}.net_monthly_hhs_aggs) tmp05
GROUP BY key05
) agg05
group by year_month;

DROP TABLE IF EXISTS ${env:TMP_db}.net_06_location_region;
CREATE TABLE ${env:TMP_db}.net_06_location_region AS
SELECT year_month AS year_month, collect_set(location__region) AS location__region
FROM (
SELECT '${hiveconf:LAST_YEAR_MONTH}' AS year_month, CONCAT(key06, ":", CAST(sum(value06) AS STRING)) as location__region
FROM  
 (SELECT explode(location__region) AS (key06, value06) FROM ${env:TMP_db}.net_monthly_hhs_aggs) tmp06
GROUP BY key06
) agg06
group by year_month;

DROP TABLE IF EXISTS ${env:TMP_db}.net_07_location_city;
CREATE TABLE ${env:TMP_db}.net_07_location_city AS
SELECT year_month AS year_month, collect_set(location__city) AS location__city
FROM (
SELECT '${hiveconf:LAST_YEAR_MONTH}' AS year_month, CONCAT(key07, ":", CAST(sum(value07) AS STRING)) as location__city
FROM  
 (SELECT explode(location__city) AS (key07, value07) FROM ${env:TMP_db}.net_monthly_hhs_aggs) tmp07
GROUP BY key07
) agg07
group by year_month;

DROP TABLE IF EXISTS ${env:TMP_db}.net_08_location_state;
CREATE TABLE ${env:TMP_db}.net_08_location_state AS
SELECT year_month AS year_month, collect_set(location__state) AS location__state
FROM (
SELECT '${hiveconf:LAST_YEAR_MONTH}' AS year_month, CONCAT(key08, ":", CAST(sum(value08) AS STRING)) as location__state
FROM  
 (SELECT explode(location__state) AS (key08, value08) FROM ${env:TMP_db}.net_monthly_hhs_aggs) tmp08
GROUP BY key08
) agg08
group by year_month;

DROP TABLE IF EXISTS ${env:TMP_db}.net_09_location_zip_code;
CREATE TABLE ${env:TMP_db}.net_09_location_zip_code AS
SELECT year_month AS year_month, collect_set(location__zip_code) AS location__zip_code
FROM (
SELECT '${hiveconf:LAST_YEAR_MONTH}' AS year_month, CONCAT(key09, ":", CAST(sum(value09) AS STRING)) as location__zip_code
FROM  
 (SELECT explode(location__zip_code) AS (key09, value09) FROM ${env:TMP_db}.net_monthly_hhs_aggs) tmp09
GROUP BY key09
) agg09
group by year_month;

DROP TABLE IF EXISTS ${env:TMP_db}.net_10_total_page_views_by_section;
CREATE TABLE ${env:TMP_db}.net_10_total_page_views_by_section AS
SELECT year_month AS year_month, collect_set(total_page_views_by_section) AS total_page_views_by_section
FROM (
SELECT '${hiveconf:LAST_YEAR_MONTH}' AS year_month, CONCAT(key10, ":", CAST(sum(value10) AS STRING)) as total_page_views_by_section
FROM  
 (SELECT explode(total_page_views_by_section) AS (key10, value10) FROM ${env:TMP_db}.net_monthly_hhs_aggs) tmp10
GROUP BY key10
) agg10
group by year_month;

DROP TABLE IF EXISTS ${env:TMP_db}.net_11_monthly_aggs;
CREATE TABLE ${env:TMP_db}.net_11_monthly_aggs AS
SELECT
    year_month,
    SUM(first_time_users) AS first_time_users,
    SUM(total_conversions_by_type) AS total_conversions_by_type,
    SUM(total_login_failures) AS total_login_failures
FROM
    ${env:TMP_db}.net_monthly_hhs_aggs
GROUP BY
    year_month;

--set hive.auto.convert.join = false;

INSERT INTO TABLE net_visits_agg_monthly 
SELECT 
    agg01.year_month, 
    agg01.visits,
    agg01.visitors,
    agg01.customer__user_ids, 
    agg01.customer__households, 
    agg01.customer__test_accounts,  

    tbl01.customer__network_status, 
    tbl02.network_type, 
    tbl03.customer__subscriptionTypes, 
    tbl04.device__os, 
    tbl05.location__country_name, 
    tbl06.location__region, 
    tbl07.location__city, 
    tbl08.location__state, 
    tbl09.location__zip_code, 
    tbl10.total_page_views_by_section,

    tbl11.first_time_users,
    tbl11.total_conversions_by_type,
    tbl11.total_login_failures
FROM
    ${env:TMP_db}.net_monthly_gral_aggs agg01
    JOIN ${env:TMP_db}.net_01_customer_network_status tbl01 
    ON agg01.year_month = tbl01.year_month
    JOIN ${env:TMP_db}.net_02_network_type tbl02
    ON tbl01.year_month = tbl02.year_month
    JOIN ${env:TMP_db}.net_03_customer_subscriptionTypes tbl03
    ON tbl01.year_month = tbl03.year_month
    JOIN ${env:TMP_db}.net_04_device_os tbl04
    ON tbl01.year_month = tbl04.year_month
    JOIN ${env:TMP_db}.net_05_location_country_name tbl05
    ON tbl01.year_month = tbl05.year_month
    JOIN ${env:TMP_db}.net_06_location_region tbl06
    ON tbl01.year_month = tbl06.year_month
    JOIN ${env:TMP_db}.net_07_location_city tbl07
    ON tbl01.year_month = tbl07.year_month
    JOIN ${env:TMP_db}.net_08_location_state tbl08
    ON tbl01.year_month = tbl08.year_month
    JOIN ${env:TMP_db}.net_09_location_zip_code tbl09
    ON tbl01.year_month = tbl09.year_month
    JOIN ${env:TMP_db}.net_10_total_page_views_by_section tbl10
    ON tbl01.year_month = tbl10.year_month
    JOIN ${env:TMP_db}.net_11_monthly_aggs tbl11
    ON agg01.year_month = tbl11.year_month
;

DROP TABLE IF EXISTS ${env:TMP_db}.net_events_monthly;
DROP TABLE IF EXISTS ${env:TMP_db}.net_monthly_gral_aggs;
DROP TABLE IF EXISTS ${env:TMP_db}.net_monthly_visits_aggs;
DROP TABLE IF EXISTS ${env:TMP_db}.net_monthly_hhs_aggs;
DROP TABLE IF EXISTS ${env:TMP_db}.net_01_customer_network_status;
DROP TABLE IF EXISTS ${env:TMP_db}.net_02_network_type;
DROP TABLE IF EXISTS ${env:TMP_db}.net_03_customer_subscriptionTypes;
DROP TABLE IF EXISTS ${env:TMP_db}.net_04_device_os;
DROP TABLE IF EXISTS ${env:TMP_db}.net_05_location_country_name;
DROP TABLE IF EXISTS ${env:TMP_db}.net_06_location_region;
DROP TABLE IF EXISTS ${env:TMP_db}.net_07_location_city;
DROP TABLE IF EXISTS ${env:TMP_db}.net_08_location_state;
DROP TABLE IF EXISTS ${env:TMP_db}.net_09_location_zip_code;
DROP TABLE IF EXISTS ${env:TMP_db}.net_10_total_page_views_by_section;
DROP TABLE IF EXISTS ${env:TMP_db}.net_11_monthly_aggs;
