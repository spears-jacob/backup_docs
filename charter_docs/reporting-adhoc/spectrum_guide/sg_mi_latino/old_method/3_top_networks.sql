SET BEGIN_DATE = '2017-06-01';
SET END_DATE = '2017-06-30';

-- ============
-- == EQUIPMENT WITH MAC ID
-- ============
DROP TABLE IF EXISTS test_tmp.latino_account_equipment_mac_base;
CREATE TABLE test_tmp.latino_account_equipment_mac_base AS
SELECT
      ah.partition_year_month,
      ah.account__number_aes256,
      ah.account__category,
      ah.product__video_package_type,
      eqp.equipment__derived_mac_address_aes256

FROM test.latino_account_history_base ah
INNER JOIN prod.account_equipment_history eqp
      ON eqp.account__number_aes256 = ah.account__number_aes256
      AND SUBSTRING(eqp.partition_date_time,0,7) = ah.partition_year_month
WHERE
      eqp.partition_date_time BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
      AND eqp.partition_date_time IN (CONCAT(SUBSTRING(partition_date_time,0,7), '-', '01'), LAST_DAY(partition_date_time))
      AND eqp.equipment__category_name IN ('HD/DVR Converters', 'Standard Digital Converters', 'HD Converters')
      AND eqp.equipment__derived_mac_address_aes256 IS NOT NULL
;

-- ============
-- == VOD TOP NETWORKS
-- ============
DROP TABLE IF EXISTS test_tmp.vod_latino_top_networks;
CREATE TABLE test_tmp.vod_latino_top_networks AS

DROP TABLE IF EXISTS test_tmp.vod_latino_base;
CREATE TABLE test_tmp.vod_latino_base AS

SELECT
      ah.partition_year_month,
      ah.account__category,
      ah.product__video_package_type,
      ah.account__number_aes256,
      vod.title__content_provider_name,
      CASE
          WHEN count(distinct vod.session__vod_lease_sid) != 0 THEN count(distinct vod.session__vod_lease_sid)
          ELSE count(*)
      END AS total_number_of_views,
      SUM(vod.session__viewing_in_s) AS seconds_viewed,
      SUM(vod.session__viewing_in_s)/3600 AS hours_viewed,
      SIZE(COLLECT_SET(ah.account__number_aes256)) AS households
FROM test_tmp.latino_account_equipment_mac_base ah
INNER JOIN prod.vod_concurrent_stream_session_event vod
      ON ah.equipment__derived_mac_address_aes256 = vod.session__equipment_mac_id_aes256
      AND ah.partition_year_month = SUBSTRING(vod.partition_date_time, 0, 7)
WHERE vod.partition_date_time BETWEEN  ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
      AND vod.title__service_category_name != 'ADS'
      AND vod.session__is_error = FALSE
      AND vod.session__viewing_in_s < 14400
      AND vod.session__viewing_in_s >0
      AND vod.asset__class_code = 'MOVIE'
      AND vod.session__vod_lease_sid IS NOT NULL
GROUP BY
      ah.partition_year_month,
      ah.account__category,
      ah.product__video_package_type,
      ah.account__number_aes256,
      vod.title__content_provider_name
      ;

-- ============
-- == RESULTS
-- ============
SELECT * FROM test_tmp.vod_latino_top_networks;


--USAGE PER PACKAGE
SELECT
      partition_year_month,
      account__category,
      product__video_package_type,
      SIZE(COLLECT_SET(account__number_aes256)) AS households,
      SUM(seconds_viewed) AS  seconds_viewed,
      SUM(hours_viewed) AS  hours_viewed,
      SUM(total_number_of_views) AS content_views
FROM test_tmp.vod_latino
GROUP BY
      partition_year_month,
      account__category,
      product__video_package_type

;


SELECT
      partition_year_month,
      account__category,

      SIZE(COLLECT_SET(account__number_aes256)) AS households,
      SUM(seconds_viewed) AS  seconds_viewed,
      SUM(hours_viewed) AS  hours_viewed,
      SUM(total_number_of_views) AS content_views
FROM test_tmp.vod_latino_base
GROUP BY
      partition_year_month,
      account__category
;
