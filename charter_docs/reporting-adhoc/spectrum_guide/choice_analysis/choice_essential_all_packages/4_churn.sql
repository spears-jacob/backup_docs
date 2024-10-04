-- ============
-- CHURN: CHOICE ESSENTIAL
-- ============
DROP TABLE IF EXISTS test_tmp.churn_choice_essential;
CREATE TABLE test_tmp.churn_choice_essential AS

SELECT
    month_start AS timeframe,
    'churned' AS status,
    account__number_aes256,
    system__kma_desc,
    account__category,
    product__video_package_type

FROM

(
  SELECT
    a.month_start,
    a.account__number_aes256,
    a.system__kma_desc,
    a.account__category,
    a.product__video_package_type

  FROM

  (
    SELECT
        ah.account__number_aes256,
        ah.system__kma_desc,
        ah.product__video_package_type,
        ah.account__category,
        ah.partition_date_time AS month_start
    FROM test.choice_essential_accounts ah
     WHERE
           ah.partition_date_time = CONCAT(SUBSTRING(ah.partition_date_time,0,7), '-', '01') -- START OF MONTH
  ) a

LEFT OUTER JOIN

--GENERATE A LIST OF ALL CUSTOMERS ACTIVE ONE DAY AFTER ABOVE TIMEFRAME
--IF AN ACCOUNT DOES NOT SHOWS UP, IT IS NO LONGER A VIDEO CUSTOMER
  (
  SELECT
        ADD_MONTHS(DATE_ADD(LAST_DAY(ah.partition_date_time), 1), -1) AS month_start,
        ah.product__video_package_type,
        ah.account__number_aes256
  FROM test.choice_essential_accounts ah
  WHERE ah.partition_date_time = LAST_DAY(ah.partition_date_time) -- last day of the month
  GROUP BY
        ah.partition_date_time,
        ah.product__video_package_type,
        ah.account__number_aes256
  ) b
ON a.account__number_aes256 = b.account__number_aes256
AND a.month_start = b.month_start
AND a.product__video_package_type = b.product__video_package_type
WHERE b.account__number_aes256 IS NULL

) d

UNION ALL

SELECT
    month_start AS timeframe,
    'not churned' AS status,
    account__number_aes256,
    system__kma_desc,
    account__category,
    product__video_package_type

FROM

(
  SELECT
    a.month_start,
    a.account__number_aes256,
    a.system__kma_desc,
    a.account__category,
    a.product__video_package_type

  FROM

  (
    SELECT
        ah.account__number_aes256,
        ah.system__kma_desc,
        ah.product__video_package_type,
        ah.account__category,
        ah.partition_date_time AS month_start
    FROM test.choice_essential_accounts ah
     WHERE
           ah.partition_date_time = CONCAT(SUBSTRING(ah.partition_date_time,0,7), '-', '01') -- START OF MONTH
  ) a

LEFT OUTER JOIN

--GENERATE A LIST OF ALL CUSTOMERS ACTIVE ONE DAY AFTER ABOVE TIMEFRAME
--IF AN ACCOUNT DOES NOT SHOWS UP, IT IS NO LONGER A VIDEO CUSTOMER
  (
  SELECT
        ADD_MONTHS(DATE_ADD(LAST_DAY(ah.partition_date_time), 1), -1) AS month_start,
        ah.product__video_package_type,
        ah.account__number_aes256
  FROM test.choice_essential_accounts ah
  WHERE ah.partition_date_time = LAST_DAY(ah.partition_date_time) -- last day of the month
  GROUP BY
        ah.partition_date_time,
        ah.product__video_package_type,
        ah.account__number_aes256
  ) b
ON a.account__number_aes256 = b.account__number_aes256
AND a.month_start = b.month_start
AND a.product__video_package_type = b.product__video_package_type
WHERE b.account__number_aes256 IS NOT NULL

) d
;

-- ============
-- CHURN: ALL VIDEO ACCOUNTS
-- ============
DROP TABLE IF EXISTS test_tmp.churn_all_accounts;
CREATE TABLE test_tmp.churn_all_accounts AS

SELECT
   month_start AS timeframe,
   'churned' AS status,
   account__number_aes256,
   system__kma_desc,
   account__category

FROM

(
 SELECT
   a.month_start,
   a.account__number_aes256,
   a.system__kma_desc,
   a.account__category

 FROM

 (
   SELECT
       ah.account__number_aes256,
       ah.system__kma_desc,
       ah.account__category,
       ah.partition_date_time AS month_start
   FROM test.all_video_accounts ah
    WHERE
          ah.partition_date_time = CONCAT(SUBSTRING(ah.partition_date_time,0,7), '-', '01') -- START OF MONTH
 ) a

LEFT OUTER JOIN

--GENERATE A LIST OF ALL CUSTOMERS ACTIVE ONE DAY AFTER ABOVE TIMEFRAME
--IF AN ACCOUNT DOES NOT SHOWS UP, IT IS NO LONGER A VIDEO CUSTOMER
 (
 SELECT
       ADD_MONTHS(DATE_ADD(LAST_DAY(ah.partition_date_time), 1), -1) AS month_start,
       ah.account__number_aes256
 FROM test.all_video_accounts ah
 WHERE ah.partition_date_time = LAST_DAY(ah.partition_date_time) -- last day of the month
 GROUP BY
       ah.partition_date_time,
       ah.account__number_aes256
 ) b
ON a.account__number_aes256 = b.account__number_aes256
AND a.month_start = b.month_start
WHERE b.account__number_aes256 IS NULL

) d

UNION ALL

SELECT
   month_start AS timeframe,
   'not churned' AS status,
   account__number_aes256,
   system__kma_desc,
   account__category

FROM

(
 SELECT
   a.month_start,
   a.account__number_aes256,
   a.system__kma_desc,
   a.account__category

 FROM

 (
   SELECT
       ah.account__number_aes256,
       ah.system__kma_desc,
       ah.account__category,
       ah.partition_date_time AS month_start
   FROM test.all_video_accounts ah
    WHERE
          ah.partition_date_time = CONCAT(SUBSTRING(ah.partition_date_time,0,7), '-', '01') -- START OF MONTH
 ) a

LEFT OUTER JOIN

--GENERATE A LIST OF ALL CUSTOMERS ACTIVE ONE DAY AFTER ABOVE TIMEFRAME
--IF AN ACCOUNT DOES NOT SHOWS UP, IT IS NO LONGER A VIDEO CUSTOMER
 (
 SELECT
       ADD_MONTHS(DATE_ADD(LAST_DAY(ah.partition_date_time), 1), -1) AS month_start,
       --ah.product__video_package_type,
       ah.account__number_aes256
 FROM test.all_video_accounts ah
 WHERE ah.partition_date_time = LAST_DAY(ah.partition_date_time) -- last day of the month
 GROUP BY
       ah.partition_date_time,
       ah.account__number_aes256
 ) b
ON a.account__number_aes256 = b.account__number_aes256
AND a.month_start = b.month_start
WHERE b.account__number_aes256 IS NOT NULL

) d
;
