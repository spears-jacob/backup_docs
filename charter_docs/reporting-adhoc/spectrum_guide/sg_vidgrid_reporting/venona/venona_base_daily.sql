
-- ============
-- == STEP 0: UNIT TESTING
-- ============
ACCT AES256: SdO88CM+YvHIUCsoIFkSQdCSsk+e9bCZS7q/TFSonXk=
MAC AES: '+1LBzGGsLh7R9mIMbaCxiQ==' | MAC AES256: 'i/GuPFH32wJMWulZYMaJyA=='
MAC AES: 'KPRihiIOV4x0rRwwrMaebA==' | MAC AES256: '9lL1YZeqONeJJOLFMey4Rw=='

-- ============
-- == STEP 0: Hive Adjustments
-- ============
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
SET BEGIN_DATE = '2017-10-01';
SET END_DATE = '2017-10-31';

-- ============
-- == STEP 1: VENONA BASE TABLE
-- ============

DROP TABLE IF EXISTS test.venona_guide_pageviews_and_settings;
CREATE TABLE test.venona_guide_pageviews_and_settings AS
SELECT
      visit__device__uuid,
      prod.epoch_converter(received__timestamp ,'America/Denver') AS partition_date_denver,
      state__view__current_page__elements__standardized_name,
      state__view__current_page__page_name,
      message__name,
      from_unixtime(CAST(received__timestamp/1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss') AS event_time


FROM prod.venona_events
WHERE
          partition_date_utc BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
      AND visit__application_details__application_name = 'Spectrum Guide'
      AND
        (
          (
              state__view__current_page__elements__standardized_name IN ('showStandardGuide', 'showVideoGuide')
          AND message__name IN ('selectAction')
          )
          OR
          (   state__view__current_page__page_name IN  ('guide')
          AND message__name IN ('pageView')
          )
        )
;

-- ============
-- == STEP 2: VENONA + SG BASE TABLE
-- ============
DROP TABLE IF EXISTS test.venona_sg_base_daily;
CREATE TABLE test.venona_sg_base_daily AS
SELECT
      partition_date_denver,
      system__kma_desc,
      account__number_aes256,
      account__mac_id_aes256,
      state__view__current_page__elements__standardized_name,
      state__view__current_page__page_name,
      message__name,
      event_time

FROM test.venona_guide_pageviews_and_settings events
INNER JOIN
    (
        SELECT
              run_date,
              system__kma_desc,
              account__number_aes256,
              account__mac_id_aes256
        FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
        WHERE
              run_date BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
              AND account__type = 'SUBSCRIBER'
              AND customer__type = 'Residential'
    ) deployed
ON events.partition_date_denver = deployed.run_date
AND events.visit__device__uuid = prod.aes_encrypt(prod.aes_decrypt256(deployed.account__mac_id_aes256))
;

-- ============
-- == STEP 3 PULL GUIDE TYPE SETTINGS
-- ============
DROP TABLE IF EXISTS test.venona_sg_guide_type_rankings_account;
CREATE TABLE test.venona_sg_guide_type_rankings_account AS
SELECT
      system__kma_desc,
      account__number_aes256,
      account__mac_id_aes256,
      state__view__current_page__elements__standardized_name,
      RANK() OVER  (PARTITION BY account__mac_id_aes256 ORDER BY event_time ASC) AS ranking,
      event_time AS min_window_time,
      LEAD (event_time) OVER (PARTITION BY account__mac_id_aes256 ORDER BY event_time ASC ) AS max_window_time
FROM test.venona_sg_base_daily
WHERE
          state__view__current_page__elements__standardized_name IN ('showStandardGuide', 'showVideoGuide')
      AND message__name IN ('selectAction')
      -- AND account__number_aes256 IN ('SdO88CM+YvHIUCsoIFkSQdCSsk+e9bCZS7q/TFSonXk=')
;


-- ============
-- == STEP 4: CREATE TABLE TO ADJUST MIN AND MAX WINDOW TIMES FRAMES PER CUSTOMER
-- ============

DROP TABLE IF EXISTS test.venona_sg_guide_type_windows_adjusted_account;
CREATE TABLE test.venona_sg_guide_type_windows_adjusted_account AS

SELECT
      system__kma_desc,
      account__number_aes256,
      account__mac_id_aes256,
      ranking,
      state__view__current_page__elements__standardized_name,
      CASE WHEN ranking = 1 THEN from_unixtime(CAST(1506816000000/1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss') ELSE min_window_time END AS min_window_time_adjusted,
      CASE WHEN max_window_time IS NULL THEN from_unixtime(CAST(1509494400000/1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss') ELSE max_window_time END AS max_window_time_adjusted
FROM test.venona_sg_guide_type_rankings_account
;



-- ============
-- == STEP 5: PULL RECORDS LINKED TO A GUIDE PAGE VIEW AND ITS TIME
-- ============

DROP TABLE IF EXISTS test.venona_sg_guide_menu_pageviews_account;
CREATE TABLE IF NOT EXISTS test.venona_sg_guide_menu_pageviews_account AS
SELECT
      system__kma_desc,
      account__number_aes256,
      account__mac_id_aes256,
      event_time
FROM test.venona_sg_base_daily
WHERE
          state__view__current_page__page_name IN  ('guide')
      AND message__name IN ('pageView')
      -- AND account__number_aes256 IN ('SdO88CM+YvHIUCsoIFkSQdCSsk+e9bCZS7q/TFSonXk=')
;

-- ============
-- == STEP 6: BUILD AGG
-- ============

SET hive.auto.convert.join=false;
add jar hdfs:///udf/Anonymize_pii.jar;


DROP TABLE IF EXISTS test.venona_sg_guide_type_to_page_views_account;
CREATE TABLE test.venona_sg_guide_type_to_page_views_account AS
SELECT
      rankings.system__kma_desc,
      rankings.state__view__current_page__elements__standardized_name,
      SIZE(COLLECT_SET(rankings.account__number_aes256)) AS account_count,
      SIZE(COLLECT_SET(rankings.account__mac_id_aes256)) AS mac_count,
      SUM(IF(views.event_time >= rankings.min_window_time_adjusted AND views.event_time < rankings.max_window_time_adjusted, 1, 0)) AS guide_views
FROM test.venona_sg_guide_type_windows_adjusted_account rankings
INNER JOIN test.venona_sg_guide_menu_pageviews_account views
ON rankings.account__number_aes256 = views.account__number_aes256
AND rankings.account__mac_id_aes256 = views.account__mac_id_aes256
AND rankings.system__kma_desc = views.system__kma_desc
GROUP BY
      rankings.system__kma_desc,
      rankings.state__view__current_page__elements__standardized_name
    ;
