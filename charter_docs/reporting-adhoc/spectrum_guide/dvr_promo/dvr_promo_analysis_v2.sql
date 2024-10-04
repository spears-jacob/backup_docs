-- ============
-- == DATES
-- ============
SET BEGIN_DATE = '2017-12-01';

-- ============
-- == STEP 1: DERIVE LIST OF ACCOUNTS THAT EVER HAD THE DVR PROMO CODE - ONE DAY PRIOR TO BEGIN_DATE
-- ============
DROP TABLE IF EXISTS test.start_end_dvr_promo_by_account;
CREATE TABLE test.start_end_dvr_promo_by_account AS

SELECT
      account__number_aes256,
      MIN(partition_date_time) AS dvr_promo_start,
      MAX(partition_date_time) AS dvr_promo_end
FROM prod.account_history
WHERE
          (
            product__promotion_description RLIKE '^.*SV501.*$' --> WE SEARCH FOR THE DVR PROMO CODE THROUGH ALL HISTORY
          OR product__promotion_description RLIKE '^.*AT.*$'
          OR product__promotion_description RLIKE '^.*MC.*$'
          OR product__promotion_description RLIKE '^.*AW.*$'
          OR product__promotion_description RLIKE '^.*W.*$'
          OR product__promotion_description RLIKE '^.*Z1.*$'
          OR product__promotion_description RLIKE '^.*DS.*$'
          OR product__promotion_description RLIKE '^.*4.*$'
          OR product__promotion_description RLIKE '^.*GD.*$'
          OR product__promotion_description RLIKE '^.*LG.*$'
          OR product__promotion_description RLIKE '^.*2B.*$'
          OR product__promotion_description RLIKE '^.*GO.*$'
          OR product__promotion_description RLIKE '^.*M.*$'
          OR product__promotion_description RLIKE '^.*K3.*$'
          OR product__promotion_description RLIKE '^.*6A.*$'
          OR product__promotion_description RLIKE '^.*GM.*$'
          OR product__promotion_description RLIKE '^.*EV.*$'
          OR product__promotion_description RLIKE '^.*ME.*$'
          OR product__promotion_description RLIKE '^.*SV421.*$'
          OR product__promotion_description RLIKE '^.*SV800.*$'
          OR product__promotion_description RLIKE '^.*SV011.*$'
          OR product__promotion_description RLIKE '^.*SV420.*$'
        )
      AND partition_date_time <  DATE_ADD(${hiveconf:BEGIN_DATE} , -1)
GROUP BY
      account__number_aes256
;

-- ============
-- == STEP 2: DERIVE DISTINCT LIST OF ACCOUNTs + MACS. NEEDED TO AVOID DUPLICATE COUNTING DOWNSTREAM
-- ============
--DATA DISCLAIMERS:
--FILTERS USED IN THE MONTHLY AND WEEKLY EXECUTIVE ARE APPLIED BELOW TO DERIVE A CONSISTENT CURRENT DEPLOYED COUNT
--THIS WILL ENSURE MY COUNTS ARE TIED TO MY CURRENT DEPLOYED BASE.

DROP TABLE IF EXISTS test.sg_deployed;
CREATE TABLE test.sg_deployed AS

SELECT
      deployed.run_date,
      deployed.account__number_aes256,
      deployed.account__mac_id_aes256,
      MAX(account__category) AS account__category,
      MAX(system__kma_desc) AS system__kma_desc
FROM
      prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY deployed
WHERE
          deployed.run_date = ${hiveconf:BEGIN_DATE}
      AND deployed.account__type = 'SUBSCRIBER'
      AND deployed.customer__type = 'Residential'
      AND deployed.customer__disconnect_date IS NULL
      AND deployed.EQUIPMENT__CATEGORY_NAME IN ('HD/DVR Converters', 'Standard Digital Converters', 'HD Converters')
      AND SUBSTR(deployed.EQUIPMENT__MODEL_TYPE,1,1) <> 'Q'
      AND SUBSTR(deployed.EQUIPMENT__MODEL_TYPE,1,1) <> 'K'
      AND deployed.EQUIPMENT__STATUS_CODE = 'H'
      AND deployed.EQUIPMENT__IS_REPORTABLE = TRUE
      AND deployed.EQUIPMENT__DISCONNECT_DATE IS NULL
GROUP BY
      deployed.run_date,
      deployed.account__number_aes256,
      deployed.account__mac_id_aes256
;

-- ============
-- == STEP 3: DERIVE DAILY AGG BY ACCOUNT WITH DVR PROMO IDENTIFICATION
-- ============
DROP TABLE IF EXISTS test.dvr_promo_daily_agg_by_account;
CREATE TABLE test.dvr_promo_daily_agg_by_account AS
SELECT
      deployed.run_date,
      deployed.account__category,
      deployed.system__kma_desc,
      CASE
        WHEN (
  product__promotion_description RLIKE '^.*SV501.*$' --> WE SEARCH FOR THE DVR PROMO CODE THROUGH ALL HISTORY
OR product__promotion_description RLIKE '^.*AT.*$'
OR product__promotion_description RLIKE '^.*MC.*$'
OR product__promotion_description RLIKE '^.*AW.*$'
OR product__promotion_description RLIKE '^.*W.*$'
OR product__promotion_description RLIKE '^.*Z1.*$'
OR product__promotion_description RLIKE '^.*DS.*$'
OR product__promotion_description RLIKE '^.*4.*$'
OR product__promotion_description RLIKE '^.*GD.*$'
OR product__promotion_description RLIKE '^.*LG.*$'
OR product__promotion_description RLIKE '^.*2B.*$'
OR product__promotion_description RLIKE '^.*GO.*$'
OR product__promotion_description RLIKE '^.*M.*$'
OR product__promotion_description RLIKE '^.*K3.*$'
OR product__promotion_description RLIKE '^.*6A.*$'
OR product__promotion_description RLIKE '^.*GM.*$'
OR product__promotion_description RLIKE '^.*EV.*$'
OR product__promotion_description RLIKE '^.*ME.*$'
OR product__promotion_description RLIKE '^.*SV421.*$'
OR product__promotion_description RLIKE '^.*SV800.*$'
OR product__promotion_description RLIKE '^.*SV011.*$'
OR product__promotion_description RLIKE '^.*SV420.*$'
)THEN 'current_dvr_promo' --> IF THE CUSTOMER HAS THE DVR PROMO CODE ON BEGIN DATE, THEN CURRENT PROMOTION CUSTOMER
        WHEN dvr_promo_his.account__number_aes256 IS NOT NULL THEN 'previous_dvr_promo' --> IF THE CUSTOMER DOES NOT HAVE THE PROMO CODE ON BEGIN DATE BUT HAD IT AT SOME POINT IN THE PAST, THEN PREVIOUS DVR PROMO
        ELSE 'never_dvr_promo'
      END AS dvr_promo_bucket,
      CASE
            WHEN (
  product__promotion_description RLIKE '^.*SV501.*$' --> WE SEARCH FOR THE DVR PROMO CODE THROUGH ALL HISTORY
OR product__promotion_description RLIKE '^.*AT.*$'
OR product__promotion_description RLIKE '^.*MC.*$'
OR product__promotion_description RLIKE '^.*AW.*$'
OR product__promotion_description RLIKE '^.*W.*$'
OR product__promotion_description RLIKE '^.*Z1.*$'
OR product__promotion_description RLIKE '^.*DS.*$'
OR product__promotion_description RLIKE '^.*4.*$'
OR product__promotion_description RLIKE '^.*GD.*$'
OR product__promotion_description RLIKE '^.*LG.*$'
OR product__promotion_description RLIKE '^.*2B.*$'
OR product__promotion_description RLIKE '^.*GO.*$'
OR product__promotion_description RLIKE '^.*M.*$'
OR product__promotion_description RLIKE '^.*K3.*$'
OR product__promotion_description RLIKE '^.*6A.*$'
OR product__promotion_description RLIKE '^.*GM.*$'
OR product__promotion_description RLIKE '^.*EV.*$'
OR product__promotion_description RLIKE '^.*ME.*$'
OR product__promotion_description RLIKE '^.*SV421.*$'
OR product__promotion_description RLIKE '^.*SV800.*$'
OR product__promotion_description RLIKE '^.*SV011.*$'
OR product__promotion_description RLIKE '^.*SV420.*$'
)AND DATEDIFF(${hiveconf:BEGIN_DATE}, COALESCE(dvr_promo_start, run_date)) <=90 THEN 'under_90_days_on_promo'
            WHEN (
  product__promotion_description RLIKE '^.*SV501.*$' --> WE SEARCH FOR THE DVR PROMO CODE THROUGH ALL HISTORY
OR product__promotion_description RLIKE '^.*AT.*$'
OR product__promotion_description RLIKE '^.*MC.*$'
OR product__promotion_description RLIKE '^.*AW.*$'
OR product__promotion_description RLIKE '^.*W.*$'
OR product__promotion_description RLIKE '^.*Z1.*$'
OR product__promotion_description RLIKE '^.*DS.*$'
OR product__promotion_description RLIKE '^.*4.*$'
OR product__promotion_description RLIKE '^.*GD.*$'
OR product__promotion_description RLIKE '^.*LG.*$'
OR product__promotion_description RLIKE '^.*2B.*$'
OR product__promotion_description RLIKE '^.*GO.*$'
OR product__promotion_description RLIKE '^.*M.*$'
OR product__promotion_description RLIKE '^.*K3.*$'
OR product__promotion_description RLIKE '^.*6A.*$'
OR product__promotion_description RLIKE '^.*GM.*$'
OR product__promotion_description RLIKE '^.*EV.*$'
OR product__promotion_description RLIKE '^.*ME.*$'
OR product__promotion_description RLIKE '^.*SV421.*$'
OR product__promotion_description RLIKE '^.*SV800.*$'
OR product__promotion_description RLIKE '^.*SV011.*$'
OR product__promotion_description RLIKE '^.*SV420.*$'
)AND DATEDIFF(${hiveconf:BEGIN_DATE}, COALESCE(dvr_promo_start, run_date)) BETWEEN 91 AND 180 THEN '91_to_180_days_on_promo'
            WHEN (
  product__promotion_description RLIKE '^.*SV501.*$' --> WE SEARCH FOR THE DVR PROMO CODE THROUGH ALL HISTORY
OR product__promotion_description RLIKE '^.*AT.*$'
OR product__promotion_description RLIKE '^.*MC.*$'
OR product__promotion_description RLIKE '^.*AW.*$'
OR product__promotion_description RLIKE '^.*W.*$'
OR product__promotion_description RLIKE '^.*Z1.*$'
OR product__promotion_description RLIKE '^.*DS.*$'
OR product__promotion_description RLIKE '^.*4.*$'
OR product__promotion_description RLIKE '^.*GD.*$'
OR product__promotion_description RLIKE '^.*LG.*$'
OR product__promotion_description RLIKE '^.*2B.*$'
OR product__promotion_description RLIKE '^.*GO.*$'
OR product__promotion_description RLIKE '^.*M.*$'
OR product__promotion_description RLIKE '^.*K3.*$'
OR product__promotion_description RLIKE '^.*6A.*$'
OR product__promotion_description RLIKE '^.*GM.*$'
OR product__promotion_description RLIKE '^.*EV.*$'
OR product__promotion_description RLIKE '^.*ME.*$'
OR product__promotion_description RLIKE '^.*SV421.*$'
OR product__promotion_description RLIKE '^.*SV800.*$'
OR product__promotion_description RLIKE '^.*SV011.*$'
OR product__promotion_description RLIKE '^.*SV420.*$'
)AND DATEDIFF(${hiveconf:BEGIN_DATE}, COALESCE(dvr_promo_start, run_date)) >= 181 THEN 'over_181_days_on_promo'
            WHEN dvr_promo_his.account__number_aes256  IS NOT NULL AND DATEDIFF(${hiveconf:BEGIN_DATE}, dvr_promo_end) <=90 THEN 'under_90_days_off_promo'
            WHEN dvr_promo_his.account__number_aes256  IS NOT NULL AND DATEDIFF(${hiveconf:BEGIN_DATE}, dvr_promo_end) BETWEEN 91 AND 180 THEN '91_to_180_days_off_promo'
            WHEN dvr_promo_his.account__number_aes256  IS NOT NULL AND DATEDIFF(${hiveconf:BEGIN_DATE}, dvr_promo_end) >= 181 THEN 'over_181_days_off_promo'
            ELSE 'N/A'
      END AS dvr_promo_tenure_bucket,
      deployed.account__number_aes256,
      deployed.account__mac_id_aes256
FROM test.sg_deployed deployed
LEFT JOIN prod.account_history ah --> PROMO CODE DESCRIPTION NOT YET AVAILABLE IN THE DEPLOYED TABLE. HAVE TO JOIN TO ACCOUNT HISTORY
      ON deployed.account__number_aes256 = ah.account__number_aes256
      AND deployed.run_date = ah.partition_date_time
LEFT JOIN test.start_end_dvr_promo_by_account dvr_promo_his
      ON deployed.account__number_aes256 = dvr_promo_his.account__number_aes256
WHERE
          deployed.run_date = ${hiveconf:BEGIN_DATE}
      AND ah.partition_date_time = ${hiveconf:BEGIN_DATE}
GROUP BY
      deployed.run_date,
      deployed.account__category,
      deployed.system__kma_desc,
      CASE
        WHEN (
  product__promotion_description RLIKE '^.*SV501.*$' --> WE SEARCH FOR THE DVR PROMO CODE THROUGH ALL HISTORY
OR product__promotion_description RLIKE '^.*AT.*$'
OR product__promotion_description RLIKE '^.*MC.*$'
OR product__promotion_description RLIKE '^.*AW.*$'
OR product__promotion_description RLIKE '^.*W.*$'
OR product__promotion_description RLIKE '^.*Z1.*$'
OR product__promotion_description RLIKE '^.*DS.*$'
OR product__promotion_description RLIKE '^.*4.*$'
OR product__promotion_description RLIKE '^.*GD.*$'
OR product__promotion_description RLIKE '^.*LG.*$'
OR product__promotion_description RLIKE '^.*2B.*$'
OR product__promotion_description RLIKE '^.*GO.*$'
OR product__promotion_description RLIKE '^.*M.*$'
OR product__promotion_description RLIKE '^.*K3.*$'
OR product__promotion_description RLIKE '^.*6A.*$'
OR product__promotion_description RLIKE '^.*GM.*$'
OR product__promotion_description RLIKE '^.*EV.*$'
OR product__promotion_description RLIKE '^.*ME.*$'
OR product__promotion_description RLIKE '^.*SV421.*$'
OR product__promotion_description RLIKE '^.*SV800.*$'
OR product__promotion_description RLIKE '^.*SV011.*$'
OR product__promotion_description RLIKE '^.*SV420.*$'
)THEN 'current_dvr_promo'
        WHEN dvr_promo_his.account__number_aes256  IS NOT NULL THEN 'previous_dvr_promo'
        ELSE 'never_dvr_promo'
      END,
      CASE
            WHEN (
  product__promotion_description RLIKE '^.*SV501.*$' --> WE SEARCH FOR THE DVR PROMO CODE THROUGH ALL HISTORY
OR product__promotion_description RLIKE '^.*AT.*$'
OR product__promotion_description RLIKE '^.*MC.*$'
OR product__promotion_description RLIKE '^.*AW.*$'
OR product__promotion_description RLIKE '^.*W.*$'
OR product__promotion_description RLIKE '^.*Z1.*$'
OR product__promotion_description RLIKE '^.*DS.*$'
OR product__promotion_description RLIKE '^.*4.*$'
OR product__promotion_description RLIKE '^.*GD.*$'
OR product__promotion_description RLIKE '^.*LG.*$'
OR product__promotion_description RLIKE '^.*2B.*$'
OR product__promotion_description RLIKE '^.*GO.*$'
OR product__promotion_description RLIKE '^.*M.*$'
OR product__promotion_description RLIKE '^.*K3.*$'
OR product__promotion_description RLIKE '^.*6A.*$'
OR product__promotion_description RLIKE '^.*GM.*$'
OR product__promotion_description RLIKE '^.*EV.*$'
OR product__promotion_description RLIKE '^.*ME.*$'
OR product__promotion_description RLIKE '^.*SV421.*$'
OR product__promotion_description RLIKE '^.*SV800.*$'
OR product__promotion_description RLIKE '^.*SV011.*$'
OR product__promotion_description RLIKE '^.*SV420.*$'
)AND DATEDIFF(${hiveconf:BEGIN_DATE}, COALESCE(dvr_promo_start, run_date)) <=90 THEN 'under_90_days_on_promo'
            WHEN (
  product__promotion_description RLIKE '^.*SV501.*$' --> WE SEARCH FOR THE DVR PROMO CODE THROUGH ALL HISTORY
OR product__promotion_description RLIKE '^.*AT.*$'
OR product__promotion_description RLIKE '^.*MC.*$'
OR product__promotion_description RLIKE '^.*AW.*$'
OR product__promotion_description RLIKE '^.*W.*$'
OR product__promotion_description RLIKE '^.*Z1.*$'
OR product__promotion_description RLIKE '^.*DS.*$'
OR product__promotion_description RLIKE '^.*4.*$'
OR product__promotion_description RLIKE '^.*GD.*$'
OR product__promotion_description RLIKE '^.*LG.*$'
OR product__promotion_description RLIKE '^.*2B.*$'
OR product__promotion_description RLIKE '^.*GO.*$'
OR product__promotion_description RLIKE '^.*M.*$'
OR product__promotion_description RLIKE '^.*K3.*$'
OR product__promotion_description RLIKE '^.*6A.*$'
OR product__promotion_description RLIKE '^.*GM.*$'
OR product__promotion_description RLIKE '^.*EV.*$'
OR product__promotion_description RLIKE '^.*ME.*$'
OR product__promotion_description RLIKE '^.*SV421.*$'
OR product__promotion_description RLIKE '^.*SV800.*$'
OR product__promotion_description RLIKE '^.*SV011.*$'
OR product__promotion_description RLIKE '^.*SV420.*$'
)AND DATEDIFF(${hiveconf:BEGIN_DATE}, COALESCE(dvr_promo_start, run_date)) BETWEEN 91 AND 180 THEN '91_to_180_days_on_promo'
            WHEN (
  product__promotion_description RLIKE '^.*SV501.*$' --> WE SEARCH FOR THE DVR PROMO CODE THROUGH ALL HISTORY
OR product__promotion_description RLIKE '^.*AT.*$'
OR product__promotion_description RLIKE '^.*MC.*$'
OR product__promotion_description RLIKE '^.*AW.*$'
OR product__promotion_description RLIKE '^.*W.*$'
OR product__promotion_description RLIKE '^.*Z1.*$'
OR product__promotion_description RLIKE '^.*DS.*$'
OR product__promotion_description RLIKE '^.*4.*$'
OR product__promotion_description RLIKE '^.*GD.*$'
OR product__promotion_description RLIKE '^.*LG.*$'
OR product__promotion_description RLIKE '^.*2B.*$'
OR product__promotion_description RLIKE '^.*GO.*$'
OR product__promotion_description RLIKE '^.*M.*$'
OR product__promotion_description RLIKE '^.*K3.*$'
OR product__promotion_description RLIKE '^.*6A.*$'
OR product__promotion_description RLIKE '^.*GM.*$'
OR product__promotion_description RLIKE '^.*EV.*$'
OR product__promotion_description RLIKE '^.*ME.*$'
OR product__promotion_description RLIKE '^.*SV421.*$'
OR product__promotion_description RLIKE '^.*SV800.*$'
OR product__promotion_description RLIKE '^.*SV011.*$'
OR product__promotion_description RLIKE '^.*SV420.*$'
)AND DATEDIFF(${hiveconf:BEGIN_DATE}, COALESCE(dvr_promo_start, run_date)) >= 181 THEN 'over_181_days_on_promo'
            WHEN dvr_promo_his.account__number_aes256  IS NOT NULL AND DATEDIFF(${hiveconf:BEGIN_DATE}, dvr_promo_end) <=90 THEN 'under_90_days_off_promo'
            WHEN dvr_promo_his.account__number_aes256  IS NOT NULL AND DATEDIFF(${hiveconf:BEGIN_DATE}, dvr_promo_end) BETWEEN 91 AND 180 THEN '91_to_180_days_off_promo'
            WHEN dvr_promo_his.account__number_aes256  IS NOT NULL AND DATEDIFF(${hiveconf:BEGIN_DATE}, dvr_promo_end) >= 181 THEN 'over_181_days_off_promo'
            ELSE 'N/A'
      END,
      deployed.account__number_aes256,
      deployed.account__mac_id_aes256
;


-- ============
-- == STEP 4: DERIVE DAILY AGG BY ACCOUNT WITH DVR PROMO IDENTIFICATION ROLLED UP AND ADJUSTED FOR DATA DISCREPANCIES
-- ============
--Data issues:
-- 1) There are accounts labeled as account__category = 'Non-DVR' that are identified as having a DVR promo. Part of this is that we need to capture more stbs and improve the way we identify DVR (%40).
-- The other part is that BI labeled these accounts as DVR promo but there is no-equipment indication that the stbs are DVR. This only happens .15% of the time.
-- 2) For one account, you were not able to find a deloyed_timestamp. As a result, you default it to month begin date.
-- 3) you were asked to calculate acct tenure for anyone that does not have the dvr promo or did not have it in the past. As a result, you add another case stmt for tenure.

DROP TABLE IF EXISTS test.dvr_promo_daily_agg;
CREATE TABLE test.dvr_promo_daily_agg AS
SELECT
      run_date,
      account__category,
      system__kma_desc,
      CASE
        WHEN account__category = 'NON-DVR' AND  dvr_promo_bucket IN ('current_dvr_promo', 'previous_dvr_promo') THEN  'never_dvr_promo'
        ELSE dvr_promo_bucket
      END AS dvr_promo_bucket,
      CASE
            WHEN (account__category = 'NON-DVR' OR (account__category = 'DVR' AND dvr_promo_bucket = 'never_dvr_promo' )) AND  DATEDIFF(${hiveconf:BEGIN_DATE},COALESCE(TO_DATE(FROM_UNIXTIME(SG_ACCOUNT_DEPLOYED_TIMESTAMP)),${hiveconf:BEGIN_DATE} )) <=90 THEN 'acct_tenure_under_90_days'
            WHEN (account__category = 'NON-DVR' OR (account__category = 'DVR' AND dvr_promo_bucket = 'never_dvr_promo' )) AND  DATEDIFF(${hiveconf:BEGIN_DATE},COALESCE(TO_DATE(FROM_UNIXTIME(SG_ACCOUNT_DEPLOYED_TIMESTAMP)),${hiveconf:BEGIN_DATE} )) BETWEEN 91 AND 180 THEN 'acct_tenure_91_to_180_days'
            WHEN (account__category = 'NON-DVR' OR (account__category = 'DVR' AND dvr_promo_bucket = 'never_dvr_promo' )) AND  DATEDIFF(${hiveconf:BEGIN_DATE},COALESCE(TO_DATE(FROM_UNIXTIME(SG_ACCOUNT_DEPLOYED_TIMESTAMP)),${hiveconf:BEGIN_DATE} )) >= 181 THEN 'acct_tenure_over_181_days'
            ELSE dvr_promo_tenure_bucket
      END AS dvr_promo_tenure_bucket,
      SIZE(COLLECT_SET(account__number_aes256))   AS account__number_aes256,
      SIZE(COLLECT_SET(account__mac_id_aes256)) AS account__mac_id_aes256
FROM test.dvr_promo_daily_agg_by_account dvr
LEFT JOIN (SELECT
                  account__number_aes256 AS acct,
                  MAX(SG_ACCOUNT_DEPLOYED_TIMESTAMP) AS SG_ACCOUNT_DEPLOYED_TIMESTAMP
          FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
          WHERE run_date = ${hiveconf:BEGIN_DATE}
          GROUP BY account__number_aes256
        ) deployed
          ON deployed.acct = dvr.account__number_aes256
GROUP BY
      run_date,
      account__category,
      system__kma_desc,
      CASE
        WHEN account__category = 'NON-DVR' AND  dvr_promo_bucket IN ('current_dvr_promo', 'previous_dvr_promo') THEN  'never_dvr_promo'
        ELSE dvr_promo_bucket
      END       ,
      CASE
            WHEN (account__category = 'NON-DVR' OR (account__category = 'DVR' AND dvr_promo_bucket = 'never_dvr_promo' )) AND  DATEDIFF(${hiveconf:BEGIN_DATE},COALESCE(TO_DATE(FROM_UNIXTIME(SG_ACCOUNT_DEPLOYED_TIMESTAMP)),${hiveconf:BEGIN_DATE} )) <=90 THEN 'acct_tenure_under_90_days'
            WHEN (account__category = 'NON-DVR' OR (account__category = 'DVR' AND dvr_promo_bucket = 'never_dvr_promo' )) AND  DATEDIFF(${hiveconf:BEGIN_DATE},COALESCE(TO_DATE(FROM_UNIXTIME(SG_ACCOUNT_DEPLOYED_TIMESTAMP)),${hiveconf:BEGIN_DATE} )) BETWEEN 91 AND 180 THEN 'acct_tenure_91_to_180_days'
            WHEN (account__category = 'NON-DVR' OR (account__category = 'DVR' AND dvr_promo_bucket = 'never_dvr_promo' )) AND  DATEDIFF(${hiveconf:BEGIN_DATE},COALESCE(TO_DATE(FROM_UNIXTIME(SG_ACCOUNT_DEPLOYED_TIMESTAMP)),${hiveconf:BEGIN_DATE} )) >= 181 THEN 'acct_tenure_over_181_days'
            ELSE dvr_promo_tenure_bucket
      END
;


-- ============
-- == STEP 5: DVR PROMO MONTHLY - DISTINCT TO REMOVE DUPLICATES DUE TO MACS
-- ============
DROP TABLE IF EXISTS test.dvr_promo_monthly_by_account_monthly_no_macs;
CREATE TABLE test.dvr_promo_monthly_by_account_monthly_no_macs AS

SELECT
      run_date,
      account__category,
      system__kma_desc,
      dvr_promo_bucket,
      dvr_promo_tenure_bucket,
      account__number_aes256
FROM  test.dvr_promo_daily_agg_by_account
WHERE
      run_date BETWEEN ${hiveconf:BEGIN_DATE} AND LAST_DAY(${hiveconf:BEGIN_DATE})
      AND account__category = 'DVR' --> YOU ADD THIS TO IGNORE CASES THAT ARE NON-DVR BUT HAVE THE DVR PROMO, HAPPENS LESS THAN .15% OF THE TIME
GROUP BY
      run_date,
      account__category,
      system__kma_desc,
      dvr_promo_bucket,
      dvr_promo_tenure_bucket,
      account__number_aes256
      ;



-- ============
-- == STEP 6: MONTHLY DVR USAGE WITH DVR PROMO ID
-- ============
--DATA DISCLAIMER: JUST BECAUSE SOMEONE HAS THE DVR PROMO DOES NOT ENSURE THAT THEY WILL HAVE ANY DVR USAGE
DROP TABLE IF EXISTS test.zodiac_dvr_action_monthly_w_promo_agg;
CREATE TABLE test.zodiac_dvr_action_monthly_w_promo_agg AS

SELECT
      zodiac.system__kma_desc,
      zodiac.sg_deployed_type,
      zodiac.account__category,
      dvr_promo.dvr_promo_bucket,
      zodiac.source,
      zodiac.dvr_action,
      zodiac.status,
      SIZE(COLLECT_SET(zodiac.account__number_aes256)) AS hhs,
      SUM(zodiac.elapsed_seconds) AS elapsed_seconds,
      SUM(zodiac.count_of_events) AS count_of_events,
      ${hiveconf:BEGIN_DATE} AS partition_date_denver
FROM
      test.zodiac_dvr_by_account_agg_daily zodiac
INNER JOIN
      test.dvr_promo_monthly_by_account_monthly_no_macs dvr_promo
      ON zodiac.account__number_aes256 = dvr_promo.account__number_aes256
WHERE
      zodiac.partition_date_denver BETWEEN ${hiveconf:BEGIN_DATE} AND LAST_DAY(${hiveconf:BEGIN_DATE})
GROUP BY
      zodiac.system__kma_desc,
      zodiac.sg_deployed_type,
      zodiac.account__category,
      dvr_promo.dvr_promo_bucket,
      zodiac.source,
      zodiac.dvr_action,
      zodiac.status

;


-- ============
-- == STEP 7: MONTHLY DVR USAGE WITH DVR PROMO ID BROKEN OUT BY DAYS ON
-- ============
DROP TABLE IF EXISTS test.zodiac_dvr_action_monthly_w_promo_agg_days_on;
CREATE TABLE test.zodiac_dvr_action_monthly_w_promo_agg_days_on AS

SELECT
      zodiac.system__kma_desc,
      zodiac.sg_deployed_type,
      zodiac.account__category,
      dvr_promo.dvr_promo_bucket,
      zodiac.source,
      zodiac.dvr_action,
      zodiac.status,
      zodiac.days_on,
      SIZE(COLLECT_SET(zodiac.account__number_aes256)) AS hhs,
      SUM(elapsed_seconds) AS elapsed_seconds,
      SUM(count_of_events) AS count_of_events,
      zodiac.partition_date_denver
FROM

            (SELECT
                  zodiac.system__kma_desc,
                  zodiac.sg_deployed_type,
                  zodiac.account__category,
                  zodiac.source,
                  zodiac.dvr_action,
                  zodiac.status,
                  zodiac.account__number_aes256,
                  SIZE(COLLECT_SET(partition_date_denver)) AS days_on,
                  SUM(zodiac.elapsed_seconds) AS elapsed_seconds,
                  SUM(zodiac.count_of_events) AS count_of_events,
                  ${hiveconf:BEGIN_DATE} AS partition_date_denver
            FROM
                      test.zodiac_dvr_by_account_agg_daily zodiac
            WHERE
                      zodiac.partition_date_denver BETWEEN ${hiveconf:BEGIN_DATE} AND LAST_DAY(${hiveconf:BEGIN_DATE})
            GROUP BY
                  zodiac.system__kma_desc,
                  zodiac.sg_deployed_type,
                  zodiac.account__category,
                  zodiac.source,
                  zodiac.dvr_action,
                  zodiac.status,
                  zodiac.account__number_aes256
          ) zodiac

          INNER JOIN
                      test.dvr_promo_monthly_by_account_monthly_no_macs dvr_promo
                ON dvr_promo.account__number_aes256 = zodiac.account__number_aes256
                AND dvr_promo.run_date = zodiac.partition_date_denver

GROUP BY
      zodiac.system__kma_desc,
      zodiac.sg_deployed_type,
      zodiac.account__category,
      dvr_promo.dvr_promo_bucket,
      zodiac.source,
      zodiac.dvr_action,
      zodiac.status,
      zodiac.days_on,
      zodiac.partition_date_denver
;

-- ============
-- == STEP 8: VOD USAGE MONTHLY
-- ============
DROP TABLE IF EXISTS test.vod_usage_monthly;
CREATE TABLE test.vod_usage_monthly AS
SELECT

      vod.account__number_aes256,
      SUM(CASE WHEN vod.total_view_duration_in_s >0 THEN vod.total_view_duration_in_s END) AS total_view_duration_in_s,
      SUM(CASE WHEN vod.total_number_of_views >0 THEN vod.total_number_of_views END) AS total_number_of_views,
      ${hiveconf:BEGIN_DATE} AS partition_date_denver
FROM
        prod.VOD_ACCT_LEVEL_USAGE_DAILY_AGG vod
WHERE
          vod.partition_date_time BETWEEN ${hiveconf:BEGIN_DATE} AND LAST_DAY(${hiveconf:BEGIN_DATE})
      AND vod.total_view_duration_in_s > 0
      AND vod.guide_type = 'SG'
      AND vod.account_category = 'DVR'
GROUP BY
      vod.account__number_aes256
;

-- ============
-- == STEP 9: STVA USAGE MONTHLY --> WE ONLY CARE ABOUT watch_time_ms_any, THE BROKEN DOWN TIMES ARE NICE TO HAVE
-- ============
DROP TABLE IF EXISTS test.stva_usage_monthly;
CREATE TABLE test.stva_usage_monthly AS
SELECT
      billing_id,
      SUM(watch_time_ms) AS watch_time_ms_any,
      SUM(CASE WHEN playback_type = 'linear' THEN watch_time_ms END) AS watch_time_ms_linear,
      SUM(CASE WHEN playback_type = 'dvr' THEN watch_time_ms END) AS watch_time_ms_dvr,
      SUM(CASE WHEN playback_type = 'cdvr' THEN watch_time_ms END) AS watch_time_ms_cdvr,
      SUM(CASE WHEN playback_type = 'vod' THEN watch_time_ms END) AS watch_time_ms_vod,
      SUM(CASE WHEN playback_type NOT IN ('linear','dvr', 'cdvr', 'vod') THEN watch_time_ms END) AS watch_time_ms_other,
      ${hiveconf:BEGIN_DATE} AS partition_date_denver
FROM prod.venona_metric_agg
WHERE
              denver_date BETWEEN ${hiveconf:BEGIN_DATE} AND LAST_DAY(${hiveconf:BEGIN_DATE})
          AND watch_time_ms > 0
GROUP BY
      billing_id
;

-- ============
-- == STEP 10: DVR USAGE MONTHLY
-- ============
DROP TABLE IF EXISTS test.dvr_usage_monthly;
CREATE TABLE test.dvr_usage_monthly AS
SELECT
      zodiac.system__kma_desc,
      zodiac.account__category,
      zodiac.account__number_aes256,
      SUM(CASE WHEN zodiac.dvr_action = 'play' AND zodiac.status = 'successful' AND zodiac.source ='any' THEN elapsed_seconds END) AS elapsed_seconds_play,
      SUM(CASE WHEN zodiac.dvr_action = 'record' AND zodiac.status = 'successful' AND zodiac.source ='any' THEN elapsed_seconds END) AS elapsed_seconds_record,
      SUM(CASE WHEN zodiac.dvr_action = 'schedule' AND zodiac.status = 'successful' AND zodiac.source ='any' THEN elapsed_seconds END) AS elapsed_seconds_schedule,
      SUM(CASE WHEN zodiac.dvr_action = 'play' AND zodiac.status = 'successful' AND zodiac.source ='any' THEN count_of_events END) AS count_of_events_play,
      SUM(CASE WHEN zodiac.dvr_action = 'record' AND zodiac.status = 'successful' AND zodiac.source ='any' THEN count_of_events END) AS count_of_events_record,
      SUM(CASE WHEN zodiac.dvr_action = 'schedule' AND zodiac.status = 'successful' AND zodiac.source ='any' THEN count_of_events END) AS count_of_events_schedule,
      ${hiveconf:BEGIN_DATE} AS partition_date_denver
FROM test.zodiac_dvr_by_account_agg_daily zodiac
WHERE
          zodiac.partition_date_denver  BETWEEN ${hiveconf:BEGIN_DATE} AND LAST_DAY(${hiveconf:BEGIN_DATE})
      AND zodiac.source ='any'
      AND zodiac.status = 'successful'
GROUP BY
      zodiac.system__kma_desc,
      zodiac.account__category,
      zodiac.account__number_aes256
;


-- ============
-- == STEP 11: CONSOLIDATED DVR VOD STVA USAGE MONTHLY BY ACCOUNT
-- ============

DROP TABLE IF EXISTS test.vod_dvr_usage_monthly_by_account_agg;
CREATE TABLE test.vod_dvr_usage_monthly_by_account_agg AS

SELECT
      dvr_promo.system__kma_desc,
      dvr_promo.account__category,
      dvr_promo.dvr_promo_bucket,
      dvr_promo.dvr_promo_tenure_bucket,
      dvr_promo.account__number_aes256,
      SUM(zodiac.elapsed_seconds_play) AS elapsed_seconds_play,
      SUM(zodiac.elapsed_seconds_record) AS elapsed_seconds_record,
      SUM(zodiac.elapsed_seconds_schedule) AS elapsed_seconds_schedule,
      SUM(zodiac.count_of_events_play) AS count_of_events_play,
      SUM(zodiac.count_of_events_record) AS count_of_events_record,
      SUM(zodiac.count_of_events_schedule) AS count_of_events_schedule,
      SUM(vod.total_view_duration_in_s) AS total_view_duration_in_s,
      SUM(vod.total_number_of_views) AS total_number_of_views,
      SUM(stva.watch_time_ms_any)/1000 AS watch_time_seconds_any,
      SUM(stva.watch_time_ms_dvr)/1000 AS watch_time_seconds_dvr,
      SUM(stva.watch_time_ms_vod)/1000 AS watch_time_seconds_vod,
      ${hiveconf:BEGIN_DATE} AS partition_date_denver
FROM
      test.dvr_promo_monthly_by_account_monthly_no_macs dvr_promo
LEFT JOIN
        test.dvr_usage_monthly zodiac
ON
            dvr_promo.account__number_aes256 = zodiac.account__number_aes256
        AND dvr_promo.run_date = zodiac.partition_date_denver
LEFT JOIN
        test.vod_usage_monthly vod
ON
            dvr_promo.account__number_aes256 = vod.account__number_aes256
        AND dvr_promo.run_date = vod.partition_date_denver
LEFT JOIN
        test.stva_usage_monthly stva
ON
            prod.aes_encrypt(prod.aes_decrypt256(dvr_promo.account__number_aes256)) = stva.billing_id
        AND dvr_promo.run_date = stva.partition_date_denver

GROUP BY
      dvr_promo.system__kma_desc,
      dvr_promo.account__category,
      dvr_promo.dvr_promo_bucket,
      dvr_promo.dvr_promo_tenure_bucket,
      dvr_promo.account__number_aes256

;

-- ============
-- == STEP 12: CONSOLIDATED DVR VOD STVA USAGE MONTHLY AGG WITH USAGE BUCKETS
-- ============
DROP TABLE IF EXISTS test.vod_dvr_usage_monthly_agg;
CREATE TABLE test.vod_dvr_usage_monthly_agg AS

SELECT
      system__kma_desc,
      account__category,
      dvr_promo_bucket,
      dvr_promo_tenure_bucket,
      usage_bucket,
      SIZE(COLLECT_SET(account__number_aes256)) AS hhs,
      SUM(elapsed_seconds_play) AS elapsed_seconds_play,
      SUM(total_view_duration_in_s) AS total_view_duration_in_s,
      SUM(watch_time_seconds_any) AS watch_time_seconds_any,
      SUM(dvr_play_vod_any_stva_any_seconds) AS dvr_play_vod_any_stva_any_seconds,
      partition_date_denver
FROM
(
      SELECT
            system__kma_desc,
            account__category,
            dvr_promo_bucket,
            dvr_promo_tenure_bucket,
            account__number_aes256,
            CASE
                  WHEN elapsed_seconds_play > 0 AND total_view_duration_in_s > 0 AND watch_time_seconds_any > 0 THEN 'vod_dvr_stva_usage'
                  WHEN elapsed_seconds_play > 0 AND total_view_duration_in_s > 0 AND (watch_time_seconds_any <= 0 OR watch_time_seconds_any IS NULL) THEN 'dvr_vod_usage'
                  WHEN (elapsed_seconds_play <= 0 OR elapsed_seconds_play IS NULL)  AND total_view_duration_in_s > 0 AND watch_time_seconds_any > 0  THEN 'vod_stva_usage'
                  WHEN elapsed_seconds_play > 0  AND (total_view_duration_in_s IS NULL OR total_view_duration_in_s <= 0) AND watch_time_seconds_any > 0 THEN 'stva_dvr_usage'
                  WHEN elapsed_seconds_play > 0 AND (total_view_duration_in_s IS NULL OR total_view_duration_in_s <= 0) AND (watch_time_seconds_any <= 0 OR watch_time_seconds_any IS NULL) THEN 'dvr_usage_only'
                  WHEN (elapsed_seconds_play <= 0 OR elapsed_seconds_play IS NULL) AND total_view_duration_in_s > 0  AND (watch_time_seconds_any <= 0 OR watch_time_seconds_any IS NULL) THEN 'vod_usage_only'
                  WHEN watch_time_seconds_any > 0 AND ((total_view_duration_in_s IS NULL OR total_view_duration_in_s <= 0) AND (elapsed_seconds_play <= 0 OR elapsed_seconds_play IS NULL) ) THEN 'stva_usage_only'
                  WHEN (watch_time_seconds_any <= 0 OR watch_time_seconds_any IS NULL) AND ((total_view_duration_in_s IS NULL OR total_view_duration_in_s <= 0) OR (elapsed_seconds_play <= 0 OR elapsed_seconds_play IS NULL) ) THEN 'no_dvr_vod_stva_usage'
                  ELSE 'other'
            END AS usage_bucket,
            elapsed_seconds_play,
            total_view_duration_in_s,
            watch_time_seconds_any,
            COALESCE(elapsed_seconds_play, 0) + COALESCE(total_view_duration_in_s, 0) + COALESCE(watch_time_seconds_any,0) AS dvr_play_vod_any_stva_any_seconds,
            partition_date_denver
      FROM
            test.vod_dvr_usage_monthly_by_account_agg

) a

GROUP BY
      system__kma_desc,
      account__category,
      dvr_promo_bucket,
      dvr_promo_tenure_bucket,
      usage_bucket,
      partition_date_denver
;
