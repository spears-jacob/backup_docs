

SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

SET BEGIN_DATE = '2017-10-01';
SET END_DATE = '2017-10-31';

-- =============
-- DERIVE MACs + STATES
-- =============
DROP TABLE IF EXISTS test.venona_macs_base_monthly;
CREATE TABLE test.venona_macs_base_monthly AS

SELECT
      visit__device__uuid,
      SUBSTRING(prod.epoch_converter(received__timestamp ,'America/Denver'), 0, 7) AS partition_year_month,
      state__view__current_page__elements__standardized_name
FROM prod.venona_events

WHERE
          partition_date_utc BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
      AND visit__application_details__application_name = 'Spectrum Guide'
      AND state__view__current_page__elements__standardized_name IN ('showStandardGuide', 'showVideoGuide')
      AND message__name IN ('selectAction') --> indicates that a certain guide type was set as a setting on a particular day



GROUP BY
      visit__device__uuid,
      SUBSTRING(prod.epoch_converter(received__timestamp ,'America/Denver'), 0, 7),
      state__view__current_page__elements__standardized_name
      ;

-- =============
-- DERIVE ACCOUNTS + MACs + STATES
-- =============
DROP TABLE IF EXISTS test.venona_sg_base_monthly;
CREATE TABLE test.venona_sg_base_monthly AS
SELECT
  partition_year_month,
  system__kma_desc,
  account__number_aes256,
  account__mac_id_aes256,
  state__view__current_page__elements__standardized_name

FROM test.venona_macs_base_monthly events
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
  ON events.partition_year_month = SUBSTRING(deployed.run_date, 0, 7)
  AND events.visit__device__uuid = prod.aes_encrypt(prod.aes_decrypt256(deployed.account__mac_id_aes256))
;


-- =============
-- AGG TABLE - ACCOUNTS
-- =============
DROP TABLE IF EXISTS test.venona_sg_guide_type_agg_hhs_monthly;
CREATE TABLE test.venona_sg_guide_type_agg_hhs_monthly AS
SELECT
      deployed_acct.partition_year_month,
      deployed_acct.system__kma_desc,
      SIZE(COLLECT_SET(IF(array_contains(deployed_acct.menu_guide_states,'showStandardGuide') = TRUE, deployed_acct.account__number_aes256, NULL))) AS hhs_showStandardGuide_count,
      SIZE(COLLECT_SET(IF(array_contains(deployed_acct.menu_guide_states,'showVideoGuide') = TRUE, deployed_acct.account__number_aes256, NULL))) AS hhs_showVideoGuide_count,
      SIZE(COLLECT_SET(IF((array_contains(deployed_acct.menu_guide_states,'showStandardGuide') = TRUE AND array_contains(deployed_acct.menu_guide_states,'showVideoGuide') = TRUE ) , deployed_acct.account__number_aes256, NULL))) AS hhs_showStandardGuide_and_showVideoGuide_count,
      SIZE(COLLECT_SET(IF((array_contains(deployed_acct.menu_guide_states,'showStandardGuide') = TRUE AND SIZE(deployed_acct.menu_guide_states) = 1), deployed_acct.account__number_aes256, NULL))) AS hhs_only_showStandardGuide_count,
      SIZE(COLLECT_SET(IF((array_contains(deployed_acct.menu_guide_states,'showVideoGuide') = TRUE AND SIZE(deployed_acct.menu_guide_states) = 1), deployed_acct.account__number_aes256, NULL))) AS hhs_only_showVideoGuide_count,
      SIZE(COLLECT_SET(deployed_acct.account__number_aes256)) AS hhs_with_guide_menu_usage_count
FROM (
      SELECT
            partition_year_month,
            system__kma_desc,
            account__number_aes256,
            COLLECT_SET(state__view__current_page__elements__standardized_name) AS menu_guide_states
      FROM test.venona_sg_base_monthly
      GROUP BY
            partition_year_month,
            system__kma_desc,
            account__number_aes256
    ) deployed_acct

GROUP BY
      deployed_acct.partition_year_month,
      deployed_acct.system__kma_desc
;

-- =============
-- AGG TABLE - MACS
-- =============

DROP TABLE IF EXISTS test.venona_sg_guide_type_agg_macs_monthly;
CREATE TABLE test.venona_sg_guide_type_agg_macs_monthly AS
SELECT
      deployed_macs.partition_year_month,
      deployed_macs.system__kma_desc,
      SIZE(COLLECT_SET(IF(array_contains(deployed_macs.menu_guide_states,'showStandardGuide') = TRUE, deployed_macs.account__mac_id_aes256, NULL))) AS stbs_showStandardGuide_count,
      SIZE(COLLECT_SET(IF(array_contains(deployed_macs.menu_guide_states,'showVideoGuide') = TRUE, deployed_macs.account__mac_id_aes256, NULL))) AS stbs_showVideoGuide_count,
      SIZE(COLLECT_SET(IF((array_contains(deployed_macs.menu_guide_states,'showStandardGuide') = TRUE AND array_contains(deployed_macs.menu_guide_states,'showVideoGuide') = TRUE ), deployed_macs.account__mac_id_aes256, NULL))) AS stbs_showStandardGuide_and_showVideoGuide_count,
      SIZE(COLLECT_SET(IF((array_contains(deployed_macs.menu_guide_states,'showStandardGuide') = TRUE AND SIZE(deployed_macs.menu_guide_states) = 1), deployed_macs.account__mac_id_aes256, NULL))) AS stbs_only_showStandardGuide_count,
      SIZE(COLLECT_SET(IF((array_contains(deployed_macs.menu_guide_states,'showVideoGuide') = TRUE AND SIZE(deployed_macs.menu_guide_states) = 1), deployed_macs.account__mac_id_aes256, NULL))) AS stbs_only_showVideoGuide_count,
      SIZE(COLLECT_SET(deployed_macs.account__mac_id_aes256)) AS stbs_with_guide_menu_usage_count


FROM (
      SELECT
            partition_year_month,
            system__kma_desc,
            account__mac_id_aes256,
            COLLECT_SET(state__view__current_page__elements__standardized_name) AS menu_guide_states
      FROM test.venona_sg_base_monthly
      GROUP BY
            partition_year_month,
            system__kma_desc,
            account__mac_id_aes256
    ) deployed_macs


GROUP BY
      deployed_macs.partition_year_month,
      deployed_macs.system__kma_desc
;
