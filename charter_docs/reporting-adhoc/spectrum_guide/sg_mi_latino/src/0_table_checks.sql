SET hive.auto.convert.join=false;

-- ============
-- == Check vod events for usage by accounts
-- ============
-- 2017-06	2893313	1933543	1933049
SET BEGIN_DATE = '2017-06-01';
SET END_DATE = '2017-06-30';

SELECT
      SUBSTRING(vod.partition_date_time,0, 7) AS partition_year_month,
      SIZE(COLLECT_SET(vod.session__equipment_mac_id_aes256)) AS vod_mac_count,
      SIZE(COLLECT_SET(ah.equipment__derived_mac_address_aes256)) AS bi_mac_count,
      SIZE(COLLECT_SET(vod.account__number_aes256)) AS vod_account_count,
      SIZE(COLLECT_SET(ah.account__number_aes256)) AS bi_account_count
FROM prod.vod_concurrent_stream_session_event vod
INNER JOIN test_tmp.all_packages_account_equipment_mac_base ah
ON vod.session__equipment_mac_id_aes256 = ah.equipment__derived_mac_address_aes256
WHERE vod.partition_date_time BETWEEN  ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
AND vod.title__service_category_name != 'ADS'
AND vod.session__is_error = FALSE
AND vod.session__viewing_in_s < 14400
AND vod.session__viewing_in_s >0
AND vod.asset__class_code = 'MOVIE'
AND vod.session__vod_lease_sid IS NOT NULL


GROUP BY
      SUBSTRING(vod.partition_date_time,0, 7)
      ;

-- 2017-06	927460	927460	2400067
-- ============
-- == Check AGG for usage by accounts
-- ============
SELECT
      SUBSTRING(vod.partition_date_time,0,7) AS partition_year_month,
      SIZE(COLLECT_SET(vod.account__number_aes256)) AS vod_account_count,
      SIZE(COLLECT_SET(acct.account__number_aes256)) AS bi_account_count,
      SIZE(COLLECT_SET(acct.equipment__derived_mac_address_aes256)) AS bi_mac_count
      -- vod.account_category AS account__category,
      -- acct.product__video_package_type,
      -- SUM(vod.total_view_duration_in_s)/3600 AS total_duration_in_hours,
      -- SUM(vod.total_number_of_views) AS total_views
FROM prod.VOD_ACCT_LEVEL_USAGE_DAILY_AGG vod --> EXISTING VOD USAGE TABLE
INNER JOIN test_tmp.all_packages_account_equipment_mac_base acct --> ACCOUNTS IDENTIFIED AS ACTIVE IN A MONTH
  ON acct.account__number_aes256 = vod.account__number_aes256
  AND acct.partition_year_month = SUBSTRING(vod.partition_date_time,0,7)
WHERE vod.partition_date_time BETWEEN  ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
  AND vod.total_view_duration_in_s > 0 ---> Key filter that reduces accounts with vod usage, there are accounts in this table with no VOD viewing time
GROUP BY
      SUBSTRING(vod.partition_date_time,0,7)--,
      -- vod.account__number_aes256,
      -- vod.account_category,
      -- acct.product__video_package_type
      ;

SET BEGIN_DATE = '2017-06-01';
SET END_DATE = '2017-06-30';
DROP TABLE IF EXISTS test_tmp.bi_accounts_with_vod_activity_in_events;
CREATE TABLE test_tmp.bi_accounts_with_vod_activity_in_events AS
  SELECT
        SUBSTRING(vod.partition_date_time,0, 7) AS partition_year_month,
        vod.session__equipment_mac_id_aes256,
        ah.equipment__derived_mac_address_aes256,
        -- vod.account__number_aes256,
        ah.account__number_aes256
  FROM prod.vod_concurrent_stream_session_event vod
  INNER JOIN test_tmp.all_packages_account_equipment_mac_base ah
  ON vod.session__equipment_mac_id_aes256 = ah.equipment__derived_mac_address_aes256
  WHERE vod.partition_date_time BETWEEN  ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
  AND vod.title__service_category_name != 'ADS'
  AND vod.session__is_error = FALSE
  AND vod.session__viewing_in_s < 14400
  AND vod.session__viewing_in_s >0
  AND vod.asset__class_code = 'MOVIE'
  AND vod.session__vod_lease_sid IS NOT NULL


  GROUP BY
        SUBSTRING(vod.partition_date_time,0, 7),
        vod.session__equipment_mac_id_aes256,
        ah.equipment__derived_mac_address_aes256,
        -- vod.account__number_aes256,
        ah.account__number_aes256
        ;





DROP TABLE IF EXISTS test_tmp.missing_accounts_vod;
CREATE TABLE test_tmp.missing_accounts_vod AS

SELECT
    vod.partition_date_time,
    vod.account__number_aes256 AS agg_account__number_aes256,
    vod_accounts.session__equipment_mac_id_aes256,
    vod_accounts.equipment__derived_mac_address_aes256,
    vod_accounts.account__number_aes256 AS bi_account__number_aes256

FROM  prod.VOD_ACCT_LEVEL_USAGE_DAILY_AGG vod
INNER JOIN
( SELECT * FROM test_tmp.bi_accounts_with_vod_activity_in_events) vod_accounts

ON vod.account__number_aes256 = vod_accounts.account__number_aes256

WHERE partition_date_time BETWEEN  ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}

GROUP BY
        vod.partition_date_time,
        vod.account__number_aes256,
        vod_accounts.session__equipment_mac_id_aes256,
        vod_accounts.equipment__derived_mac_address_aes256,
        vod_accounts.account__number_aes256
;
