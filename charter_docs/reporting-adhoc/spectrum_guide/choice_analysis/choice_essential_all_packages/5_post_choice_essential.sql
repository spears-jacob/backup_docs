SET BEGIN_DATE = '2017-09-01';
SET END_DATE = '2017-09-30';

-- -- ============
-- -- LAST DAY A RECORD WAS AVAILABLE FOR CHOICE ESSENTIAL ACCOUNTS THAT CHURNED --> not needed, if a cx does not have any record on the last of the month, they are a true disconnect
-- -- ===========
-- DROP TABLE test_tmp.churn_dates;
-- CREATE TABLE test_tmp.churn_dates AS
--
-- SELECT
--       churn.account__number_aes256,
--       MAX(ah.partition_date_time) partition_date_time
-- FROM test_tmp.churn_choice_essential churn
-- INNER JOIN prod.account_history ah
-- ON churn.account__number_aes256 = ah.account__number_aes256
-- WHERE
--     ah.partition_date_time BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
--     AND status = 'churned'
-- GROUP BY
--     churn.account__number_aes256
-- ;
-- ============
-- POST CHURN ANALYSIS FOR CHURNED CHOICE OR ESSENTIAL PACKAGE HOLDERS
-- ===========
--CHURNED ACCOUNTS NOT CAPTURED BY THIS TABLE MEANS THAT THE CUSTOMER TURNED TO AN EMPLOYEE OR TEST ACCOUNT FOR THE LAST DAY WE HAVE A RECORD
--THIS DOESN'T HAPPEN A LOT BUT IT DOES HAPPEN
-- SELECT product__package_category, product__is_video_package, account__number_aes256, account__type, customer__type, product__package_category,
--    product__is_video_package, product__is_basic, product__is_tenant_basic, product__is_tenant, product__video_package_type, partition_date_time
--    FROM prod.account_history
--    WHERE partition_date_time IN ('2017-09-01', '2017-09-30')
--    AND account__number_aes256 IN (
--    '1LeaMKnLA9sFcEcYCa0Bt7LPdUv0Qpd2dBFtxzVFf6o=',
-- 'PNJjyBYUpbEErBxg3mIPdwhLulM9zX6F5P/XjI71TpE=',
-- 'cStubBLN3dClk2Lrt1nFXmHJ3NjYClbDWYac8IQVO7U=',
-- 'xDn//gMmmdcxD0D9K39SpcDrvujBxm3jkGgkAhlX7Q0=')
DROP TABLE test.churn_choice_essential_post;
CREATE TABLE test.churn_choice_essential_post AS
SELECT
   churn.timeframe,
   churn.product__video_package_type,
   churn.account__number_aes256,
   ah.partition_date_time,
   CASE
     WHEN ah.product__video_package_type IN
                                                ('TV Stream', 'SPP Stream','TV Stream Plus',
                                                'SPP Stream Plus', 'Spectrum TV Stream') THEN 'Stream TV'
     WHEN ah.product__package_category LIKE '%Video%' AND ah.product__video_package_type IN
                                                ('Gold', 'SPP Gold',
                                                'Mi Plan Latino', 'SPP Mi Plan Latino',
                                                'Mi Plan Latino Gold',
                                                'SPP Mi Plan Latino Gold',
                                                'Mi Plan Latino Silver',
                                                'SPP Mi Plan Latino Silver',
                                                'Seasonal', 'SPP Seasonal',
                                                'Select', 'SPP Select',
                                                'Silver', 'SPP Silver'
                                                ) THEN 'Upgrade'
     WHEN ah.product__package_category LIKE '%Video%' AND ah.product__video_package_type IN
                                                 ('Legacy Limited Basic',
                                                 'Legacy Seasonal',
                                                 'Legacy Standard',
                                                 'Limited',
                                                 'NULL',
                                                 'Bulk Video'
                                               ) THEN 'Other Video'
    WHEN ah.product__package_category LIKE '%Video%' AND ah.product__video_package_type IN
                                            ('SPP Choice', 'Choice', 'SPP Essentials', 'Essentials') THEN ah.product__video_package_type
     WHEN ah.product__package_category = 'Internet Only' THEN 'Internet Only'
     WHEN ah.product__package_category = 'Voice Only' THEN 'Voice Only'
     WHEN ah.product__package_category LIKE '%Internet%' AND ah.product__package_category LIKE '%Voice%'
                                                         AND ah.product__package_category NOT LIKE '%Video%'
                                                         THEN 'Internet and Voice'
     WHEN ah.product__package_category LIKE '%Disconnect%' AND ah.customer__disconnect_date IS NOT NULL THEN 'Disconnect'
     ELSE 'Disconnect'
   END status

FROM test_tmp.churn_choice_essential churn --> customers identified as churn
LEFT JOIN prod.account_history ah
ON churn.account__number_aes256 = ah.account__number_aes256
-- INNER JOIN test_tmp.churn_dates dates
-- ON dates.account__number_aes256 = ah.account__number_aes256
-- AND dates.partition_date_time = ah.partition_date_time

WHERE churn.status = 'churned'
  AND ah.account__type IN ('SUBSCRIBER')
  AND ah.customer__type IN ('Residential')
  AND ah.meta__file_type IN ('Residential')
  AND ah.partition_date_time = ${hiveconf:END_DATE}
GROUP BY
     churn.timeframe,
     churn.product__video_package_type,
     churn.account__number_aes256,
     ah.partition_date_time,
     CASE
       WHEN ah.product__video_package_type IN
                                                  ('TV Stream', 'SPP Stream','TV Stream Plus',
                                                  'SPP Stream Plus', 'Spectrum TV Stream') THEN 'Stream TV'
       WHEN ah.product__package_category LIKE '%Video%' AND ah.product__video_package_type IN
                                                  ('Gold', 'SPP Gold',
                                                  'Mi Plan Latino', 'SPP Mi Plan Latino',
                                                  'Mi Plan Latino Gold',
                                                  'SPP Mi Plan Latino Gold',
                                                  'Mi Plan Latino Silver',
                                                  'SPP Mi Plan Latino Silver',
                                                  'Seasonal', 'SPP Seasonal',
                                                  'Select', 'SPP Select',
                                                  'Silver', 'SPP Silver'
                                                  ) THEN 'Upgrade'
       WHEN ah.product__package_category LIKE '%Video%' AND ah.product__video_package_type IN
                                                   ('Legacy Limited Basic',
                                                   'Legacy Seasonal',
                                                   'Legacy Standard',
                                                   'Limited',
                                                   'NULL',
                                                   'Bulk Video'
                                                 ) THEN 'Other Video'
       WHEN ah.product__package_category LIKE '%Video%' AND ah.product__video_package_type IN
                                              ('SPP Choice', 'Choice', 'SPP Essentials', 'Essentials') THEN ah.product__video_package_type
       WHEN ah.product__package_category = 'Internet Only' THEN 'Internet Only'
       WHEN ah.product__package_category = 'Voice Only' THEN 'Voice Only'
       WHEN ah.product__package_category LIKE '%Internet%' AND ah.product__package_category LIKE '%Voice%'
                                                           AND ah.product__package_category NOT LIKE '%Video%'
                                                           THEN 'Internet and Voice'
       WHEN ah.product__package_category LIKE '%Disconnect%' AND ah.customer__disconnect_date IS NOT NULL THEN 'Disconnect'
       ELSE 'Disconnect'
     END
  ;
