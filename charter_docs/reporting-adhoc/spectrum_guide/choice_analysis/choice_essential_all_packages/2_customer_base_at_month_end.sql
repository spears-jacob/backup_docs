SET BEGIN_DATE = '2017-09-01';
SET END_DATE = '2017-09-30';

-- ============
-- == CUSTOMER BASE AT MONTH END: CHOICE ESSENTIAL AND ALL VIDEO PACKAGES
-- ============

DROP TABLE IF EXISTS test.accounts_month_end;
CREATE TABLE test.accounts_month_end AS
SELECT
 LAST_DAY(ah.partition_date_time) AS month_end,
 --ah.system__kma_desc,
 --ah.product__package_category,
 ah.product__video_package_type,
 eqp.account__category,
 SIZE (COLLECT_SET(ah.account__number_aes256)) account_count
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
AND ah.partition_date_time = LAST_DAY(ah.partition_date_time)
AND ah.partition_date_time = ${hiveconf:END_DATE}
GROUP BY
LAST_DAY(ah.partition_date_time),
--ah.system__kma_desc,
--ah.product__package_category,
ah.product__video_package_type,
eqp.account__category

UNION ALL

SELECT
   LAST_DAY(ah.partition_date_time) AS month_end,
   --ah.system__kma_desc,
   --ah.product__package_category,
   'ALL PACKAGES' AS product__video_package_type,
   eqp.account__category,
   SIZE (COLLECT_SET(ah.account__number_aes256)) account_count
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
  AND ah.partition_date_time = LAST_DAY(ah.partition_date_time)
  AND ah.partition_date_time = ${hiveconf:END_DATE}
GROUP BY
    LAST_DAY(ah.partition_date_time),
    --ah.system__kma_desc,
    --ah.product__package_category,
    'ALL PACKAGES',
    eqp.account__category
  ;
