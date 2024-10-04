-- TEST CHECK: --('++/tEPWrIaSSxUjL8YIrePBWIgxm9MCg7rwjfLwd29o=')

SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=3368709120;
SET mapreduce.input.fileinputformat.split.minsize=3368709120;

SET BEGIN_DATE = '2017-06-01';
SET END_DATE = '2017-06-30';

-- ============
-- == ACCOUNT HISTORY: accounts active in a month with all packages, excludes specified packages
-- ============
DROP TABLE IF EXISTS test_tmp.all_video_account_history_base;
CREATE TABLE test_tmp.all_video_account_history_base AS

WITH tmp AS
  (SELECT
          SUBSTRING(ah.partition_date_time,0,7) AS partition_year_month,
          ah.account__number_aes256,
          -- ah.product__package_category,
          'All Packages' AS product__video_package_type,
          SIZE(COLLECT_SET(partition_date_time)) AS days_active
  FROM
          prod.account_history ah
  WHERE
          ah.partition_date_time BETWEEN  ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
      AND ah.partition_date_time IN (CONCAT(SUBSTRING(partition_date_time,0,7), '-', '01'), LAST_DAY(partition_date_time))
      AND ah.account__type IN ('SUBSCRIBER')
      AND ah.customer__type IN ('Residential')
      AND ah.meta__file_type IN ('Residential')
      AND ah.product__package_category LIKE '%Video%'
      AND ah.product__video_package_type IS NOT NULL
      AND ah.product__video_package_type NOT IN
                                  ( 'Mi Plan Latino',
                                    'SPP Mi Plan Latino',
                                    'Mi Plan Latino Gold',
                                    'SPP Mi Plan Latino Gold',
                                    'Mi Plan Latino Silver',
                                    'SPP Mi Plan Latino Silver',
                                    'TV Stream',
                                    'SPP Stream',
                                    'TV Stream Plus',
                                    'SPP Stream Plus',
                                    'Spectrum TV Stream',
                                    'Bulk Video',
                                    'N/A')
  GROUP BY
          SUBSTRING(ah.partition_date_time,0,7),
          ah.account__number_aes256,
          -- ah.product__package_category,
          'All Packages'
  )

SELECT
        partition_year_month,
        account__number_aes256,
        product__video_package_type,
        days_active
FROM tmp
WHERE days_active >= 2
;

-- ============
-- == EQUIPMENT: equipment classification for all packages
-- ============
DROP TABLE IF EXISTS test_tmp.all_video_account_equipment_base;
CREATE TABLE test_tmp.all_video_account_equipment_base AS

SELECT
  SUBSTRING(eqp.partition_date_time,0,7) AS partition_year_month,
  eqp.account__number_aes256,
  IF(array_contains(collect_set(eqp.equipment__category_name),'HD/DVR Converters'),'DVR', 'NON-DVR') AS account__category
FROM
  prod.account_equipment_history eqp
INNER JOIN
  test_tmp.all_video_account_history_base ah
ON
  eqp.account__number_aes256 = ah.account__number_aes256
AND
  SUBSTRING(eqp.partition_date_time,0,7) = ah.partition_year_month
WHERE
      eqp.partition_date_time BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
  AND eqp.partition_date_time IN (CONCAT(SUBSTRING(partition_date_time,0,7), '-', '01'), LAST_DAY(partition_date_time))
  AND eqp.equipment__category_name IN ('HD/DVR Converters', 'Standard Digital Converters', 'HD Converters')
  AND eqp.equipment__derived_mac_address_aes256 IS NOT NULL
GROUP BY
  SUBSTRING(eqp.partition_date_time,0,7),
  eqp.account__number_aes256
;
-- ============
-- == ALL VIDEO PACKAGE BASE
-- ============
INSERT INTO test.all_video_account_equipment_base PARTITION(partition_year_month)

SELECT
      ah.account__number_aes256,
      eqp.account__category,
      ah.product__video_package_type,
      ah.days_active,
      ah.partition_year_month
FROM test_tmp.all_video_account_history_base ah
LEFT JOIN test_tmp.all_video_account_equipment_base eqp
  ON ah.account__number_aes256 = eqp.account__number_aes256
  AND ah.partition_year_month = eqp.partition_year_month
;


-- ============
-- == VOD Usage: BASE
-- ============
DROP TABLE IF EXISTS test_tmp.vod_all_package_base;
CREATE TABLE test_tmp.vod_all_package_base AS
SELECT
      SUBSTRING(vod.partition_date_time,0,7) AS partition_year_month,
      vod.account__number_aes256,
      vod.account_category AS account__category,
      acct.product__video_package_type,
      SUM(vod.total_view_duration_in_s)/3600 AS total_duration_in_hours,
      SUM(vod.total_number_of_views) AS total_views
FROM prod.VOD_ACCT_LEVEL_USAGE_DAILY_AGG vod --> EXISTING VOD USAGE TABLE
INNER JOIN test_tmp.all_video_account_history_base acct --> ACCOUNTS IDENTIFIED AS ACTIVE IN A MONTH
  ON acct.account__number_aes256 = vod.account__number_aes256
  AND acct.partition_year_month = SUBSTRING(vod.partition_date_time,0,7)
WHERE vod.partition_date_time BETWEEN  ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
  AND vod.total_view_duration_in_s > 0 ---> Key filter that reduces accounts with vod usage, there are accounts in this table with no VOD viewing time
GROUP BY
      SUBSTRING(vod.partition_date_time,0,7),
      vod.account__number_aes256,
      vod.account_category,
      acct.product__video_package_type
      ;

-- ============
-- == VOD Usage: SUBSCRIBERS WITH VOD ACTIVITY + VOD USAGE
-- ============
DROP TABLE IF EXISTS test_tmp.vod_all_package_usage;
CREATE TABLE test_tmp.vod_all_package_usage AS

SELECT
      partition_year_month,
      account__category,
      SIZE(COLLECT_SET(account__number_aes256)) AS households,
      SUM(total_duration_in_hours) AS  hours_viewed,
      SUM(total_views) AS content_views
FROM test_tmp.vod_all_package_base
GROUP BY
      partition_year_month,
      account__category
;


-- ============
-- == VOD Usage: VIDEO SUBSCRIBERS ONLY - DO NOT NECESSARILY HAVE VOD USAGE - EXCLUDES SPECIFIC PACKAGES
-- ============
DROP TABLE IF EXISTS test_tmp.all_package_subscribers_monthly;
CREATE TABLE test_tmp.all_package_subscribers_monthly AS
SELECT
        partition_year_month,
        account__category,
        SIZE(COLLECT_SET(account__number_aes256)) AS households
FROM test_tmp.all_video_account_equipment_base
GROUP BY
        partition_year_month,
        account__category
;

-- ============
-- == VOD Usage: USAGE BY NETWORK - utilizes existing VOD table to derive account numbers used - NO DVR VS NON-DVR
-- ============
DROP TABLE IF EXISTS test_tmp.vod_top_networks;
DROP TABLE IF EXISTS test_tmp.vod_all_video_packages_top_networks;
CREATE TABLE test_tmp.vod_top_networks AS

SELECT
      SUBSTRING(eqp.partition_date_time,0,7) AS partition_year_month,
      vod.title__content_provider_name,
      SUM(vod.session__viewing_in_s)/3600 AS hours_viewed,
      SIZE(COLLECT_SET(eqp.account__number_aes256)) AS households
FROM prod.account_history ah
INNER JOIN prod.account_equipment_history eqp
      ON eqp.equipment__derived_mac_address_aes256 = vod.session__equipment_mac_id_aes256
      AND SUBSTRING(eqp.partition_date_time,0,7) = SUBSTRING(vod.partition_date_time, 0, 7)
INNER JOIN prod.vod_concurrent_stream_session_event VOD
      ON eqp.equipment__derived_mac_address_aes256 = vod.session__equipment_mac_id_aes256
      AND SUBSTRING(eqp.partition_date_time,0,7) = SUBSTRING(vod.partition_date_time, 0, 7)
WHERE vod.title__service_category_name != 'ADS'
      AND vod.partition_date_time BETWEEN  ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
      AND vod.session__is_error = FALSE
      AND vod.session__viewing_in_s < 14400
      AND vod.session__viewing_in_s >0
      AND vod.asset__class_code = 'MOVIE'
      AND vod.session__vod_lease_sid IS NOT NULL
      AND eqp.account__number_aes256 IN (SELECT account__number_aes256 FROM test_tmp.vod_all_package_base ) --> RECORD DRIVER
      AND eqp.partition_date_time BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
      AND eqp.equipment__category_name IN ('HD/DVR Converters', 'Standard Digital Converters', 'HD Converters')
      AND eqp.equipment__derived_mac_address_aes256 IS NOT NULL
GROUP BY
      SUBSTRING(eqp.partition_date_time,0,7),
      vod.title__content_provider_name
      ;


-- ============
-- == RESULTS
-- ============
SELECT * FROM test_tmp.vod_all_package_usage;
SELECT * FROM test_tmp.all_package_subscribers_monthly;
SELECT * FROM test_tmp.vod_top_networks;
