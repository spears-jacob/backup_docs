DROP TABLE IF EXISTS TMP.ERR_TO_DATE;
---Create table to capture error metrics for an account from beginning to date.

CREATE TABLE TMP.ERR_TO_DATE AS

SELECT  account_number,
        '${run_date}' AS partition_date_denver,
        SUM(CASE WHEN is_vod + is_dvr + is_guide > 0 THEN 1 ELSE 0 END) AS error_days,
        SUM(CASE WHEN is_vod > 0 THEN 1 ELSE 0 END) AS vod_error_days,
        SUM(CASE WHEN is_dvr > 0 THEN 1 ELSE 0 END) AS dvr_error_days,
        SUM(CASE WHEN is_guide > 0 THEN 1 ELSE 0 END) AS guide_error_days,
        SUM(is_vod + is_dvr + is_guide) AS error_count,
        SUM(is_vod) AS vod_error_count,
        SUM(is_dvr) AS dvr_error_count,
        SUM(is_guide) AS guide_error_count
FROM
(
SELECT account_number,
       partition_date_denver,
SUM(CASE WHEN application__error__type REGEXP '.*(3014|3012|3016|3006|3001|9003).*'
      THEN count_of_error ELSE 0 END) AS is_vod,
SUM(CASE WHEN application__error__type REGEXP '.*(0004|0002|2001|6001|0009|2003|6002|0006).*'
      THEN count_of_error ELSE 0 END) as is_guide,
SUM(CASE WHEN application__error__type REGEXP '.*(8001|8002|8003|8004|8005|8006|8007).*'
      THEN count_of_error ELSE 0 END) as is_dvr

FROM
( SELECT partition_date_denver,
       account_number,
       application__error__type,
       SUM(count_of_error) AS count_of_error
FROM PROD.SG_P2_ERRORS_BY_ACCOUNT
WHERE account_type = 'SUBSCRIBER'
AND partition_date_denver <= '${run_date}'
AND application__error__type RLIKE
    '.*(0002|0004|2001|6001|0009|2003|6002|0006|3014|3016|3006|3001|8001|8002|8003|8004|8005|8006|8007).*'
GROUP BY partition_date_denver,
        account_number,
        application__error__type) errors

GROUP BY partition_date_denver,
       account_number ) error_counts

GROUP BY account_number;

DROP TABLE IF EXISTS TMP.ACTIVITY_TO_DATE;

-- Counts guide launch attempts, launch errors, and active days on zodiac/adobe

CREATE TABLE TMP.ACTIVITY_TO_DATE AS

SELECT  '${run_date}' AS partition_date_denver,
        account_number,
        SUM(launch_attempts) AS launch_attempts,
        SUM(launch_errors) AS launch_errors,
        SUM(zodiac_active) AS zodiac_active_days,
        SUM(adobe_active) AS adobe_active_days
FROM
(
SELECT  partition_date_denver,
        account_number,
        SUM(launch_attempts) AS launch_attempts,
        SUM(launch_error) AS launch_errors,
        CASE WHEN SUM(zodiac_activity_count) > 0 THEN 1
             ELSE 0 END AS zodiac_active,
        CASE WHEN SUM(adobe_activity_count) > 0 THEN 1
             ELSE 0 END AS adobe_active
FROM prod.SPECTRUM_BY_DEPLOYMENT
WHERE partition_date_denver <= '${run_date}'
AND account_type = 'SUBSCRIBER'
GROUP BY partition_date_denver, account_number ) d
GROUP BY account_number;

--- Combines activity and error to date values
DROP TABLE IF EXISTS TMP.ERRORS_AND_ACTIVITY_TO_DATE;
CREATE TABLE TMP.ERRORS_AND_ACTIVITY_TO_DATE AS
SELECT activity.partition_date_denver,
       activity.account_number,
       activity.launch_attempts,
       activity.launch_errors,
       activity.zodiac_active_days,
       activity.adobe_active_days,
       err.error_days,
       err.guide_error_days,
       err.dvr_error_days,
       err.vod_error_days,
       err.error_count,
       err.guide_error_count,
       err.dvr_error_count,
       err.vod_error_count
FROM TMP.ACTIVITY_TO_DATE activity
FULL OUTER JOIN TMP.ERR_TO_DATE err
ON err.partition_date_denver = activity.partition_date_denver
AND err.account_number = activity.account_number;

--- drop table cleanup
DROP TABLE TMP.ACTIVITY_TO_DATE;
DROP TABLE TMP.ERR_TO_DATE;
--- Capture only Households that are currently deployed as of partition_date_denver

DROP TABLE IF EXISTS TMP.ACTIVITY_ERRS_IS_CURRENT;

CREATE TABLE TMP.ACTIVITY_ERRS_IS_CURRENT AS

SELECT activity.partition_date_denver,
       activity.account_number,
       accounts.account__category,
       accounts.system__kma_desc,
       activity.launch_attempts,
       activity.launch_errors,
       activity.zodiac_active_days,
       activity.adobe_active_days,
       activity.error_days,
       activity.guide_error_days,
       activity.dvr_error_days,
       activity.vod_error_days,
       activity.error_count,
       activity.guide_error_count,
       activity.dvr_error_count,
       activity.vod_error_count
FROM TMP.ERRORS_AND_ACTIVITY_TO_DATE activity
JOIN
(SELECT account__number_aes256,
        account__category,
        system__kma_desc
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date = '${run_date}'
AND account__type='SUBSCRIBER'
GROUP BY account__number_aes256,
        account__category,
        system__kma_desc ) accounts

ON accounts.account__number_aes256 = activity.account_number;

-- Add proper sg_deployed type and insert into table

--INSERT INTO TABLE TEST.ACTIVITY_ERRORS_TO_DATE PARTITION(partition_date_denver)

SELECT activity.account_number,
       accounts.deployed_date,
       activity.system__kma_desc,
       activity.account__category,
       accounts.sg_deployed_type,
       DATEDIFF('${run_date}',accounts.deployed_date) + 1 AS tenure_days,
       activity.launch_attempts,
       activity.launch_errors,
       activity.zodiac_active_days,
       activity.adobe_active_days,
       activity.error_days,
       activity.guide_error_days,
       activity.dvr_error_days,
       activity.vod_error_days,
       activity.error_count,
       activity.guide_error_count,
       activity.dvr_error_count,
       activity.vod_error_count,
       partition_date_denver
FROM TMP.ACTIVITY_ERRS_IS_CURRENT activity
JOIN
(SELECT account__number_aes256,
        FROM_UNIXTIME(MAX(sg_account_deployed_timestamp), 'yyyy-MM-dd') AS deployed_date,
        MAX(sg_deployed_type) AS sg_deployed_type
        FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
        WHERE run_date >= '2016-01-05'
        GROUP BY account__number_aes256
        ) accounts
ON accounts.account__number_aes256 = activity.account_number;

DROP TABLE TMP.ACTIVITY_ERRS_IS_CURRENT;
