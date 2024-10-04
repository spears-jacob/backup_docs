-- use ${env:ENVIRONMENT};
--STEPS TO RUN:
    1)adjust the deployment window dates if needed
      --sg_account_deployed_timestamp
      --last__customer_connect_date
    2)adjust the run date for the customer groupings as more data becomes available
      --run_date
    3)adjust the partition_date_time in the VOD_ACCT_LEVEL_USAGE_DAILY_AGG table
      --partition_date_time
    4)be conscious of the region filters and the sg deployed type
    5)run script

-- REQUIREMENTS:
--         1) Customers must be broken out by deployment windows known as customer groupings
--         2) VOD metrics should only be reported on for customers present through the entire month
-- NOTES:
--       --ACCOUNT:
--         1) An account must be present throughout the entire month to be present
--         2) An account can have FREE | TRANSACTIONAL | PREMIUM VOD usage
--         3) An account must be present within the specified customer deployment windows
--         4) You must include both new connects and force migrations
--
-- DATE FILTERS:
--         1) 'sg_new_connect_date_on_account IS NOT NULL'
--           -The field derived from the sgr_dma_final_report table, which is a daily snap shot.
--           -The sgr_dma_final_report table has equipment and account status filters that cause accounts to no longer show in this table.
--           -VOD_ACCT_LEVEL_USAGE_DAILY_AGG table only filters based on account status.
--           -As a result, there are instances where there is still vod usage for an account in the VOD_ACCT_LEVEL_USAGE_DAILY_AGG and the account has not been labeled as a disconnect yet in BI.
--           -However, the sgr_dma_final_report table considers it a disconnect due to the account and/or equipment status.
--           -In short, this would explain why an account could have a record for an entire month in the VOD_ACCT_LEVEL_USAGE_DAILY_AGG table (potentially tied to VOD usage as well) but not show up in the VOD monthly table below.
-- TESTING:
--         1) account__number_aes256 IN ('+0VAiii0PcXEFpP41B9t8Hs/EzjOirUnHQ75/d3pth0=')
--         2) SELECT * FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY WHERE account__number_aes256 IN ('+0VAiii0PcXEFpP41B9t8Hs/EzjOirUnHQ75/d3pth0=')
--         3) SELECT * FROM PROD.VOD_ACCT_LEVEL_USAGE_DAILY_AGG WHERE account__number_aes256 IN ('+0VAiii0PcXEFpP41B9t8Hs/EzjOirUnHQ75/d3pth0=')
--
-- KEY INPUT TABLES:
--         1) PROD.account_history
--         2) PROD.account_equipment_history
--         3) PROD.VOD_ACCT_LEVEL_USAGE_DAILY_AGG --> Customers that disconnect or downgrade will no longer have a record
--         4) PROD.sgr_dma_final_report --> gives you the nc_install date that is used to derive sg_new_connect_date_on_account which can filter out your records if the account disconnected
-- =============
-- CUSTOMER GROUPINGS
-- =============

DROP TABLE IF EXISTS test.customer_base_first_grouping;
CREATE TABLE test.customer_base_first_grouping AS

SELECT '2017-01-01 to 2017-04-30' AS customer_group, run_date, account__category, sg_account_deployed_timestamp, sg_deployed_type, record_inactive_date,system__kma_desc, account__type, account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date BETWEEN '2017-01-01' AND '2017-07-31' --> We want to see how this grouping performed for the rest of the year
AND (
    (FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') BETWEEN '2017-01-01' AND '2017-04-30')
    OR (last__customer_connect_date BETWEEN '2017-01-01' AND '2017-04-30')
    )

;

DROP TABLE IF EXISTS test.customer_base_second_grouping;
CREATE TABLE test.customer_base_second_grouping AS

SELECT '2017-05-01 to 2017-07-31' AS customer_group, run_date, account__category, sg_account_deployed_timestamp, sg_deployed_type, record_inactive_date,system__kma_desc, account__type, account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date BETWEEN '2017-05-01' AND '2017-07-31'
AND (
    (FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') BETWEEN '2017-05-01' AND '2017-07-31')
    OR (last__customer_connect_date BETWEEN '2017-05-01' AND '2017-07-31')
    )
;

DROP TABLE IF EXISTS test.customer_base_overall;
CREATE TABLE test.customer_base_overall AS

SELECT '2016-05-01 to 2017-07-31' AS customer_group, run_date, account__category, sg_account_deployed_timestamp, sg_deployed_type, record_inactive_date,system__kma_desc, account__type, account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date BETWEEN '2016-05-01' AND '2017-07-31'
AND (
    (FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') BETWEEN '2016-05-01' AND '2017-07-31')
    OR (last__customer_connect_date BETWEEN '2016-05-01' AND '2017-07-31')
    )
;


-- =============
-- FILTER FOR SG CUSTOMERS THAT APPEARED THROUGHOUT THE ENTIRE MOTNH + WERE PRESENT IN A DEPLOYMENT GROUPING
-- =============

-- =============
-- FIRST CUSTOMER GROUPING
-- =============
DROP TABLE IF EXISTS dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS_first_grouping;
CREATE TABLE dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS_first_grouping AS
WITH temp AS
(SELECT
    LAST_DAY(a.partition_date_time) as end_date,
    min(days_after_customer_connect) as days_after_cust_connect_on_start_of_month,
    a.account__number_aes256,
    a.system__kma_desc,
    a.guide_type,
    DAY(LAST_DAY(a.partition_date_time))
FROM PROD.VOD_ACCT_LEVEL_USAGE_DAILY_AGG a
WHERE a.account__type='SUBSCRIBER'
-- AND a.account__number_aes256 IN ('+0VAiii0PcXEFpP41B9t8Hs/EzjOirUnHQ75/d3pth0=')
AND a.account__number_aes256 IN
  (SELECT account__number_aes256 FROM test.customer_base_first_grouping )
-- AND ((guide_type='LEGACY') OR (guide_type='SG' AND sg_new_connect_date_on_account is not NULL)  )
AND (guide_type='SG')
-- AND sg_new_connect_date_on_account IS NOT NULL)
AND a.partition_date_time BETWEEN '2017-01-01' AND  '2017-07-31'
-- AND customer_base.account__number_aes256 IS NOT NULL
GROUP BY
    LAST_DAY(a.partition_date_time),
    a.account__number_aes256,
    a.system__kma_desc,
    guide_type,
    DAY(LAST_DAY(a.partition_date_time))
Having count(*)=DAY(LAST_DAY(a.partition_date_time))
)

SELECT
    'Jan to April 2017' AS customer_group,
    a.partition_date_time,
    a.account__type,
    a.account_category,
    a.account__number_aes256,
    a.system__kma_desc,
    a.guide_type,
    a.days_after_customer_connect,
    b.days_after_cust_connect_on_start_of_month,
    a.customer_connect_date,
    a.total_view_duration_in_s,
    a.free_view_duration_in_s,
    a.trans_view_duration_in_s,
    a.prem_view_duration_in_s,
    a.total_number_of_views,
    a.free_views,
    a.trans_views,
    a.prem_views
FROM
  PROD.VOD_ACCT_LEVEL_USAGE_DAILY_AGG a
INNER JOIN
  temp b
  ON a.account__number_aes256 = b.account__number_aes256
  AND a.guide_type = b.guide_type
  AND a.system__kma_desc = b.system__kma_desc

WHERE LAST_DAY(partition_date_time)=end_date
-- AND a.account__number_aes256 IN ('+0VAiii0PcXEFpP41B9t8Hs/EzjOirUnHQ75/d3pth0=')
AND account__type='SUBSCRIBER';

-- =============
-- SECOND CUSTOMER GROUPING
-- =============
DROP TABLE IF EXISTS dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS_second_grouping;
CREATE TABLE dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS_second_grouping AS
with temp as
(SELECT
    LAST_DAY(a.partition_date_time) as end_date,
    min(days_after_customer_connect) as days_after_cust_connect_on_start_of_month,
    a.account__number_aes256,
    a.system__kma_desc,
    a.guide_type,
    DAY(LAST_DAY(a.partition_date_time))
FROM PROD.VOD_ACCT_LEVEL_USAGE_DAILY_AGG a
WHERE a.account__type='SUBSCRIBER'
-- AND a.account__number_aes256 IN ('+0VAiii0PcXEFpP41B9t8Hs/EzjOirUnHQ75/d3pth0=')
AND a.account__number_aes256 IN
  (SELECT account__number_aes256 FROM test.customer_base_second_grouping )
-- AND ((guide_type='LEGACY') OR (guide_type='SG' AND sg_new_connect_date_on_account is not NULL)  )
AND guide_type='SG'
-- AND sg_new_connect_date_on_account IS NOT NULL
AND a.partition_date_time BETWEEN '2017-05-01' AND  '2017-07-31'
-- AND customer_base.account__number_aes256 IS NOT NULL
GROUP BY
    LAST_DAY(a.partition_date_time),
    a.account__number_aes256,
    a.system__kma_desc,
    guide_type,
    DAY(LAST_DAY(a.partition_date_time))
Having count(*)=DAY(LAST_DAY(a.partition_date_time))
)

SELECT
    'May to July 2017' AS customer_group,
    a.partition_date_time,
    a.account__type,
    a.account_category,
    a.account__number_aes256,
    a.system__kma_desc,
    a.guide_type,
    a.days_after_customer_connect,
    b.days_after_cust_connect_on_start_of_month,
    a.customer_connect_date,
    a.total_view_duration_in_s,
    a.free_view_duration_in_s,
    a.trans_view_duration_in_s,
    a.prem_view_duration_in_s,
    a.total_number_of_views,
    a.free_views,
    a.trans_views,
    a.prem_views
FROM
  PROD.VOD_ACCT_LEVEL_USAGE_DAILY_AGG a
INNER JOIN
  temp b
  ON a.account__number_aes256 = b.account__number_aes256
  AND a.guide_type = b.guide_type
  AND a.system__kma_desc = b.system__kma_desc

WHERE LAST_DAY(partition_date_time)=end_date
-- AND a.account__number_aes256 IN ('+0VAiii0PcXEFpP41B9t8Hs/EzjOirUnHQ75/d3pth0=')
AND account__type='SUBSCRIBER';

-- =============
-- OVERALL CUSTOMER GROUPING
-- =============
DROP TABLE IF EXISTS dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS_overall_grouping;
CREATE TABLE dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS_overall_grouping AS
with temp as
(SELECT
    LAST_DAY(a.partition_date_time) as end_date,
    min(days_after_customer_connect) as days_after_cust_connect_on_start_of_month,
    a.account__number_aes256,
    a.system__kma_desc,
    a.guide_type,
    DAY(LAST_DAY(a.partition_date_time))
FROM PROD.VOD_ACCT_LEVEL_USAGE_DAILY_AGG a
WHERE a.account__type='SUBSCRIBER'
-- AND a.account__number_aes256 IN ('+0VAiii0PcXEFpP41B9t8Hs/EzjOirUnHQ75/d3pth0=')
AND a.account__number_aes256 IN
  (SELECT account__number_aes256 FROM test.customer_base_overall )
AND (guide_type='SG' )
-- AND sg_new_connect_date_on_account IS NOT NULL
AND a.partition_date_time BETWEEN '2017-01-01' AND  '2017-07-31'
GROUP BY
    LAST_DAY(a.partition_date_time),
    a.account__number_aes256,
    a.system__kma_desc,
    guide_type,
    DAY(LAST_DAY(a.partition_date_time))
Having count(*)=DAY(LAST_DAY(a.partition_date_time))
)

SELECT
    'Jan to July 2017' AS customer_group,
    a.partition_date_time,
    a.account__type,
    a.account_category,
    a.account__number_aes256,
    a.system__kma_desc,
    a.guide_type,
    a.days_after_customer_connect,
    b.days_after_cust_connect_on_start_of_month,
    a.customer_connect_date,
    a.total_view_duration_in_s,
    a.free_view_duration_in_s,
    a.trans_view_duration_in_s,
    a.prem_view_duration_in_s,
    a.total_number_of_views,
    a.free_views,
    a.trans_views,
    a.prem_views
FROM
  PROD.VOD_ACCT_LEVEL_USAGE_DAILY_AGG a
INNER JOIN
  temp b
  ON a.account__number_aes256 = b.account__number_aes256
  AND a.guide_type = b.guide_type
  AND a.system__kma_desc = b.system__kma_desc

WHERE LAST_DAY(partition_date_time)=end_date
-- AND a.account__number_aes256 IN ('+0VAiii0PcXEFpP41B9t8Hs/EzjOirUnHQ75/d3pth0=')
AND account__type='SUBSCRIBER';

-- =============
-- COMBINE ALL GROUPINGS
-- =============

DROP TABLE IF EXISTS dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS;
CREATE TABLE dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS AS

SELECT *  FROM dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS_first_grouping

UNION ALL

SELECT *  FROM dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS_second_grouping

UNION ALL

SELECT *  FROM dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS_overall_grouping
;


-- =============
-- CURRENT DEPLOYED COUNTS
-- =============

DROP TABLE IF EXISTS dev_tmp.DEPLOYED_HH_MONTHLY_NC ;
CREATE TABLE dev_tmp.DEPLOYED_HH_MONTHLY_NC AS
Select
  customer_group,
  last_day(partition_date_time) MONTH_END_DATE,
  IF(days_after_cust_connect_on_start_of_month <= 90,'usage_before_90_days','usage_after_90_days') as usage_period,
  account_category,
  system__kma_desc,
  guide_type,
  size(collect_set(account__number_aes256)) deployed_hhs
FROM
  dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS
GROUP BY
  customer_group,
  last_day(partition_date_time),
  IF(days_after_cust_connect_on_start_of_month <= 90,'usage_before_90_days','usage_after_90_days'),
  account_category,
  system__kma_desc,
  guide_type
;


-- =============
-- VOD USAGE BREAK DOWN
-- =============

DROP TABLE IF EXISTS dev_tmp.VOD_MONTHLY_AGG_TMP_NC;
CREATE TABLE  dev_tmp.VOD_MONTHLY_AGG_TMP_NC  AS
SELECT customer_group,
       last_day(partition_date_time) AS MONTH_END_DATE,
       IF(days_after_cust_connect_on_start_of_month <= 90,'usage_before_90_days','usage_after_90_days') as usage_period,
       guide_type,
       system__kma_desc,
       account_category,
       'Any' AS vod_type,
       SIZE(COLLECT_SET(account__number_aes256)) AS distinct_households,
       SUM(total_number_of_views) AS views,
       SUM(total_view_duration_in_s) AS duration_in_s
FROM dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS
WHERE account__type='SUBSCRIBER'
  AND total_number_of_views>0
GROUP BY customer_group,
         last_day(partition_date_time),
         IF(days_after_cust_connect_on_start_of_month <= 90,'usage_before_90_days','usage_after_90_days'),
         guide_type,
         system__kma_desc,
         account_category
UNION ALL

SELECT customer_group,
       last_day(partition_date_time) AS MONTH_END_DATE,
        IF(days_after_cust_connect_on_start_of_month <= 90,'usage_before_90_days','usage_after_90_days') as usage_period,
       guide_type,
       system__kma_desc,
       account_category,
       'Free' AS vod_type,
       SIZE(COLLECT_SET(account__number_aes256)) AS distinct_households,
       SUM(free_views) AS views,
       SUM(free_view_duration_in_s) AS duration_in_s
FROM dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS
WHERE account__type='SUBSCRIBER'
  AND free_views>0
GROUP BY customer_group,
         last_day(partition_date_time),
         IF(days_after_cust_connect_on_start_of_month <= 90,'usage_before_90_days','usage_after_90_days'),
         guide_type,
         system__kma_desc,
         account_category

UNION ALL

SELECT customer_group,
       last_day(partition_date_time) AS MONTH_END_DATE,
        IF(days_after_cust_connect_on_start_of_month <= 90,'usage_before_90_days','usage_after_90_days') as usage_period,
       guide_type,
       system__kma_desc,
       account_category,
       'Premium' AS vod_type,
       SIZE(COLLECT_SET(account__number_aes256)) AS distinct_households,
       SUM(prem_views) AS views,
       SUM(prem_view_duration_in_s) AS duration_in_s
FROM dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS
WHERE account__type='SUBSCRIBER'
  AND prem_views>0
GROUP BY customer_group,
         last_day(partition_date_time),
         IF(days_after_cust_connect_on_start_of_month <= 90,'usage_before_90_days','usage_after_90_days'),
         guide_type,
         system__kma_desc,
         account_category

UNION ALL

SELECT customer_group,
       last_day(partition_date_time) AS MONTH_END_DATE,
        IF(days_after_cust_connect_on_start_of_month <= 90,'usage_before_90_days','usage_after_90_days') as usage_period,
       guide_type,
       system__kma_desc,
       account_category,
       'Transactional' AS vod_type,
       SIZE(COLLECT_SET(account__number_aes256)) AS distinct_households,
       SUM(trans_views) AS views,
       SUM(total_view_duration_in_s) AS duration_in_s
FROM dev_tmp.VOD_DAILY_AGG_NC_ANALYSIS
WHERE account__type='SUBSCRIBER'
  AND trans_views>0
GROUP BY customer_group,
         last_day(partition_date_time),
         IF(days_after_cust_connect_on_start_of_month <= 90,'usage_before_90_days','usage_after_90_days'),
         guide_type,
         system__kma_desc,
         account_category
;

-- =============
-- AGG TABLE CREATATION
-- =============
--VOD USAGE monthly counts are stored in this table once per month
-- INSERT OVERWRITE TABLE VOD_USAGE_NEW_CONNECT_MONTHLY PARTITION(MONTH_END_DATE)
DROP TABLE IF EXISTS dev_tmp.VOD_USAGE_NEW_CONNECT_MONTHLY;
CREATE TABLE dev_tmp.VOD_USAGE_NEW_CONNECT_MONTHLY AS

SELECT
       A.customer_group,
       A.usage_period as usage_period_by_start_of_month ,
       A.guide_type,
       A.system__kma_desc,
       A.account_category,
       A.vod_type,
       A.distinct_households,
       A.views,
       A.duration_in_s,
       B.deployed_hhs,
       A.month_end_date
FROM dev_tmp.VOD_MONTHLY_AGG_TMP_NC A
JOIN dev_tmp.DEPLOYED_HH_MONTHLY_NC B
    ON A.month_end_date = B.month_end_date
    AND A.guide_type = B.guide_type
    AND A.system__kma_desc = B.system__kma_desc
    AND A.usage_period = B.usage_period

    AND A.account_category = B.account_category
    AND A.customer_group = B.customer_group
;
