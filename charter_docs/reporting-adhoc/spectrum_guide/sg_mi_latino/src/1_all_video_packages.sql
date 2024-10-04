SET BEGIN_DATE = '2017-10-01';
SET END_DATE = '2017-10-31';



SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=3368709120;
SET mapreduce.input.fileinputformat.split.minsize=3368709120;
-- ============
-- == VERIMATRIX + BUSINESS INTELLIGENCE
-- ============
-- ============
-- == STEP 1: ACCOUNT HISTORY: accounts active in a month with all packages, excludes specified packages
-- ============
DROP TABLE IF EXISTS test_tmp.all_video_account_history_base;
CREATE TABLE test_tmp.all_video_account_history_base AS

WITH tmp AS
  (SELECT
          SUBSTRING(ah.partition_date_time,0,7) AS partition_year_month,
          ah.product__is_spectrum_guide,
          ah.system__kma_desc,
          ah.account__type,
          ah.customer__type,
          'All Packages' AS product__video_package_type,
          ah.account__number_aes256,
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
          ah.product__is_spectrum_guide,
          ah.system__kma_desc,
          ah.account__type,
          ah.customer__type,
          'All Packages',
          ah.account__number_aes256
  )

SELECT
        partition_year_month,
        system__kma_desc,
        account__type,
        customer__type,
        account__number_aes256,
        product__is_spectrum_guide,
        product__video_package_type,
        days_active
FROM tmp
WHERE days_active >= 2
    ;

-- ============
-- == STEP 2: EQUIPMENT: equipment classification for all packages
-- ============
DROP TABLE IF EXISTS test_tmp.all_video_account_equipment_base;
CREATE TABLE test_tmp.all_video_account_equipment_base AS

SELECT
      SUBSTRING(eqp.partition_date_time,0,7) AS partition_year_month,
      eqp.account__number_aes256,
      IF(array_contains(collect_set(eqp.equipment__category_name),'HD/DVR Converters'),'DVR', 'NON-DVR') AS account__category
FROM prod.account_equipment_history eqp
INNER JOIN test_tmp.all_video_account_history_base ah
      ON eqp.account__number_aes256 = ah.account__number_aes256
      AND SUBSTRING(eqp.partition_date_time,0,7) = ah.partition_year_month
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
-- == STEP 3: ALL VIDEO PACKAGE BASE
-- ============
DROP TABLE IF EXISTS test.all_video_account_equipment_base;
CREATE TABLE test.all_video_account_equipment_base AS

SELECT
      ah.system__kma_desc,
      ah.account__type,
      ah.customer__type,
      ah.account__number_aes256,
      ah.product__is_spectrum_guide,
      eqp.account__category,
      ah.product__video_package_type,
      ah.days_active,
      ah.partition_year_month
FROM test_tmp.all_video_account_history_base ah
INNER JOIN test_tmp.all_video_account_equipment_base eqp
      ON ah.account__number_aes256 = eqp.account__number_aes256
      AND ah.partition_year_month = eqp.partition_year_month
;

-- ============
-- == STEP 4: BASE TABLE WITH MAC ID
-- ============
DROP TABLE IF EXISTS test_tmp.all_packages_account_equipment_mac_base;
CREATE TABLE test_tmp.all_packages_account_equipment_mac_base AS

SELECT
      ah.system__kma_desc,
      ah.account__type,
      ah.customer__type,
      ah.account__number_aes256,
      ah.product__is_spectrum_guide,
      ah.account__category,
      ah.product__video_package_type,
      eqp.equipment__derived_mac_address_aes256,
      ah.partition_year_month
FROM test.all_video_account_equipment_base ah
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
-- == STEP 5: VOD BASE TMP
-- ============
SET hive.auto.convert.join=false;

DROP TABLE IF EXISTS test_tmp.vod_all_packages_base;
CREATE TABLE test_tmp.vod_all_packages_base AS
SELECT
      ah.system__kma_desc,
      ah.account__number_aes256,
      ah.product__is_spectrum_guide,
      ah.account__category,
      ah.product__video_package_type,
      ah.equipment__derived_mac_address_aes256,
      vod.title__content_provider_name,
      CASE
          WHEN COUNT(DISTINCT vod.session__vod_lease_sid) != 0 THEN COUNT(DISTINCT vod.session__vod_lease_sid)
          ELSE COUNT(*)
      END AS total_number_of_views,
      SUM(vod.session__viewing_in_s) AS seconds_viewed,
      ah.partition_year_month
FROM test_tmp.all_packages_account_equipment_mac_base ah
INNER JOIN
            (
              SELECT
                    vod.session__equipment_mac_id_aes256,
                    vod.title__content_provider_name,
                    vod.session__vod_lease_sid,
                    vod.session__viewing_in_s,
                    SUBSTRING(vod.partition_date_time,0, 7) AS partition_year_month
              FROM prod.vod_concurrent_stream_session_event vod
              WHERE
                        vod.partition_date_time BETWEEN  ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
                    AND vod.title__service_category_name != 'ADS'
                    AND vod.session__is_error = FALSE
                    AND vod.session__viewing_in_s < 14400
                    AND vod.session__viewing_in_s >0
                    AND vod.asset__class_code = 'MOVIE'
                    -- AND vod.session__vod_lease_sid IS NOT NULL
            ) vod
      ON ah.equipment__derived_mac_address_aes256 = vod.session__equipment_mac_id_aes256
      AND ah.partition_year_month = vod.partition_year_month
GROUP BY
      ah.system__kma_desc,
      ah.account__number_aes256,
      ah.product__is_spectrum_guide,
      ah.account__category,
      ah.product__video_package_type,
      ah.equipment__derived_mac_address_aes256,
      vod.title__content_provider_name,
      ah.partition_year_month
      ;

-- ============
-- == STEP 6: VOD BASE
-- ============
INSERT OVERWRITE TABLE test.vod_all_packages_base PARTITION(partition_year_month)

SELECT
      system__kma_desc,
      account__number_aes256,
      product__is_spectrum_guide,
      account__category,
      product__video_package_type,
      title__content_provider_name,
      total_number_of_views,
      seconds_viewed,
      partition_year_month
FROM test_tmp.vod_all_packages_base
;
