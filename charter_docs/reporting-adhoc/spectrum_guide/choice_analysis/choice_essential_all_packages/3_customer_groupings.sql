-- ============
-- == CUSTOMER GROUPING CHOICE ESSENTIAL
-- ============
DROP TABLE IF EXISTS test.choice_essential_accounts;
CREATE TABLE test.choice_essential_accounts AS
SELECT
    ah.account__number_aes256,
    ah.system__kma_desc,
    ah.product__package_category,
    ah.product__video_package_type,
    eqp.account__category,
    ah.partition_date_time
FROM prod.account_history ah
INNER JOIN test_tmp.account_equipment_base_1 eqp
ON ah.account__number_aes256 = eqp.account__number_aes256
   WHERE
      ah.account__type IN ('SUBSCRIBER')
   AND ah.customer__type IN ('Residential')
   AND ah.meta__file_type IN ('Residential')
   AND ah.product__package_category LIKE '%Video%' --> PACKAGE MUST INCORPORATE VIDEO COMPONENT
   AND ah.product__video_package_type IS NOT NULL --> IF A CUSTOMER DOES NOT HAVE A VIDEO PACKAGE, IT IS NOT LINKED TO VALIDA SG OR LEGACY SERVIC
   AND ah.product__video_package_type IN ('SPP Choice', 'Choice', 'SPP Essentials', 'Essentials')
   AND ah.partition_date_time IN (CONCAT(SUBSTRING(ah.partition_date_time,0,7), '-', '01'), LAST_DAY(ah.partition_date_time))
   AND ah.partition_date_time BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
   ;

-- ============
-- == CUSTOMER GROUPING ALL VIDEO ACCOUNTS
-- ============

DROP TABLE IF EXISTS test.all_video_accounts;
CREATE TABLE test.all_video_accounts AS
SELECT
  ah.account__number_aes256,
  ah.system__kma_desc,
  ah.product__package_category,
  ah.product__video_package_type,
  eqp.account__category,
  ah.partition_date_time
FROM prod.account_history ah
INNER JOIN test_tmp.account_equipment_base_1 eqp
ON ah.account__number_aes256 = eqp.account__number_aes256
 WHERE
    ah.account__type IN ('SUBSCRIBER')
 AND ah.customer__type IN ('Residential')
 AND ah.meta__file_type IN ('Residential')
 AND ah.product__package_category LIKE '%Video%' --> PACKAGE MUST INCORPORATE VIDEO COMPONENT
 AND ah.product__video_package_type IS NOT NULL --> IF A CUSTOMER DOES NOT HAVE A VIDEO PACKAGE, IT IS NOT LINKED TO VALIDA SG OR LEGACY SERVIC
 AND ah.product__video_package_type NOT IN  ('TV Stream', 'SPP Stream', 'TV Stream Plus', 'SPP Stream Plus', 'N/A')
 AND ah.partition_date_time IN (CONCAT(SUBSTRING(ah.partition_date_time,0,7), '-', '01'), LAST_DAY(ah.partition_date_time))
 AND ah.partition_date_time BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
 ;
