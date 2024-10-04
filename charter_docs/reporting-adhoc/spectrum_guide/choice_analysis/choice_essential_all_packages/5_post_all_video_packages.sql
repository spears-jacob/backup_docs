SET BEGIN_DATE = '2017-09-01';
SET END_DATE = '2017-09-30';

-- ============
-- YOU DO NOT NEED TO LOOK AT THE LAST DAY OF A RECORD FOR ALL ACCOUNT B/C THERE LAST DAY WOULD STILL BE CONSIDERED A CHURN
-- ===========
-- SET BEGIN_DATE = '2017-09-01';
-- SET END_DATE = '2017-09-30';
--
-- DROP TABLE test_tmp.churn_dates_all;
-- CREATE TABLE test_tmp.churn_dates_all AS
--
-- SELECT
--       churn.account__number_aes256,
--       MAX(ah.partition_date_time) partition_date_time
-- FROM test_tmp.churn_all_accounts churn
-- INNER JOIN prod.account_history ah
-- ON churn.account__number_aes256 = ah.account__number_aes256
-- WHERE
--     ah.partition_date_time BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
--     AND status = 'churned'
-- GROUP BY
--     churn.account__number_aes256
-- ;

DROP TABLE test.ah;
CREATE TABLE test.ah AS
SELECT
churn.timeframe,
churn.account__number_aes256,
ah.product__package_category,
ah.customer__disconnect_date
FROM prod.account_history ah
RIGHT JOIN test_tmp.churn_all_accounts churn
ON churn.account__number_aes256 = ah.account__number_aes256
WHERE
    ah.account__type IN ('SUBSCRIBER')
AND ah.customer__type IN ('Residential')
AND ah.meta__file_type IN ('Residential')
AND ah.partition_date_time = '2017-09-30'
AND churn.status = 'churned'

GROUP BY
churn.timeframe,
churn.account__number_aes256,
ah.product__package_category,
ah.customer__disconnect_date
;
-- ============
-- POST CHURN ANALYSIS FOR ALL ACCOUNTS
-- ===========
DROP TABLE test.churn_all_accounts_post;
CREATE TABLE test.churn_all_accounts_post AS
SELECT
   churn.timeframe,
   'ALL PACKAGES' AS product__video_package_type,
   churn.account__number_aes256,
   CASE
     WHEN ah.product__package_category = 'Internet Only' THEN 'Internet Only'
     WHEN ah.product__package_category = 'Voice Only' THEN 'Voice Only'
     WHEN ah.product__package_category LIKE '%Internet%' AND ah.product__package_category LIKE '%Voice%'
                                                         AND ah.product__package_category NOT LIKE '%Video%'
                                                         THEN 'Internet and Voice'
     WHEN ah.product__package_category LIKE '%Disconnect%' AND ah.customer__disconnect_date IS NOT NULL THEN 'Disconnect'
     ELSE 'Disconnect'
   END status

FROM test_tmp.churn_all_accounts churn --> customers identified as churn
LEFT JOIN  (
            SELECT account__number_aes256, product__package_category, customer__disconnect_date
            FROM prod.account_history ah
            WHERE 
                ah.account__type IN ('SUBSCRIBER')
            AND ah.customer__type IN ('Residential')
            AND ah.meta__file_type IN ('Residential')
            AND ah.partition_date_time = ${hiveconf:END_DATE}
          ) ah
ON churn.account__number_aes256 = ah.account__number_aes256

WHERE churn.status = 'churned'
GROUP BY
     churn.timeframe,
     'ALL PACKAGES',
     churn.account__number_aes256,
     CASE
       WHEN ah.product__package_category = 'Internet Only' THEN 'Internet Only'
       WHEN ah.product__package_category = 'Voice Only' THEN 'Voice Only'
       WHEN ah.product__package_category LIKE '%Internet%' AND ah.product__package_category LIKE '%Voice%'
                                                           AND ah.product__package_category NOT LIKE '%Video%'
                                                           THEN 'Internet and Voice'
       WHEN ah.product__package_category LIKE '%Disconnect%' AND ah.customer__disconnect_date IS NOT NULL THEN 'Disconnect'
       ELSE 'Disconnect'
     END
  ;
