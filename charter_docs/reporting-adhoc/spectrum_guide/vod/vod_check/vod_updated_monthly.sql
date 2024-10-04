-- use ${env:ENVIRONMENT};

DROP TABLE IF EXISTS test_tmp.VOD_DAILY_AGG_NC_ANALYSIS;
CREATE TABLE test_tmp.VOD_DAILY_AGG_NC_ANALYSIS AS
with temp as
(SELECT
    LAST_DAY(partition_date_time) as end_date,
    min(days_after_customer_connect) as days_after_cust_connect_on_start_of_month,
    account__number_aes256,
    system__kma_desc,
    guide_type,
    DAY(LAST_DAY(partition_date_time))
FROM prod.VOD_ACCT_LEVEL_USAGE_DAILY_AGG
WHERE account__type='SUBSCRIBER'
AND ((guide_type='LEGACY') OR (guide_type='SG' AND sg_new_connect_date_on_account is not NULL)  )
AND partition_date_time between '2017-05-01' AND '2017-09-01'
GROUP BY
    LAST_DAY(partition_date_time),
    account__number_aes256,
    system__kma_desc,
    guide_type,
    DAY(LAST_DAY(partition_date_time))
Having count(*)=DAY(LAST_DAY(partition_date_time))
)

SELECT
    a.partition_date_time,
    a.account__type,
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
  prod.VOD_ACCT_LEVEL_USAGE_DAILY_AGG a
INNER JOIN
  temp b
ON a.account__number_aes256=b.account__number_aes256 AND a.guide_type=b.guide_type  AND
   a.system__kma_desc=b.system__kma_desc
WHERE LAST_DAY(partition_date_time)=end_date
AND account__type='SUBSCRIBER';

--use day2 table change to day2-old as required
DROP TABLE IF EXISTS test_tmp.DEPLOYED_HH_MONTHLY_NC ;
CREATE TABLE test_tmp.DEPLOYED_HH_MONTHLY_NC AS
Select
  last_day(partition_date_time) MONTH_END_DATE,
  CASE
    WHEN days_after_cust_connect_on_start_of_month BETWEEN 0 AND 30 THEN '0 to 30 days'
    WHEN days_after_cust_connect_on_start_of_month BETWEEN 31 AND 60 THEN '31 to 60 days'
    WHEN days_after_cust_connect_on_start_of_month BETWEEN 61 AND 90 THEN '61 to 90 days'
    ELSE '90+ Days'
  END AS usage_period,
  system__kma_desc,
  guide_type,
  size(collect_set(account__number_aes256)) deployed_hhs
FROM
  test_tmp.VOD_DAILY_AGG_NC_ANALYSIS
GROUP BY
  last_day(partition_date_time),
  CASE
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 0 AND 30 THEN '0 to 30 days'
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 31 AND 60 THEN '31 to 60 days'
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 61 AND 90 THEN '61 to 90 days'
  ELSE '90+ Days'
END,
  system__kma_desc,
  guide_type;

DROP TABLE IF EXISTS test_tmp.VOD_MONTHLY_AGG_TMP_NC;
CREATE TABLE  test_tmp.VOD_MONTHLY_AGG_TMP_NC  AS
SELECT last_day(partition_date_time) AS MONTH_END_DATE,
CASE
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 0 AND 30 THEN '0 to 30 days'
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 31 AND 60 THEN '31 to 60 days'
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 61 AND 90 THEN '61 to 90 days'
  ELSE '90+ Days'
END AS usage_period,
       guide_type,
       system__kma_desc,
       'Any' AS vod_type,
       count(DISTINCT account__number_aes256) AS distinct_households,
       sum(total_number_of_views) AS views,
       sum(total_view_duration_in_s) AS duration_in_s
FROM test_tmp.VOD_DAILY_AGG_NC_ANALYSIS
WHERE account__type='SUBSCRIBER'
  AND total_number_of_views>0
GROUP BY last_day(partition_date_time),
CASE
WHEN days_after_cust_connect_on_start_of_month BETWEEN 0 AND 30 THEN '0 to 30 days'
WHEN days_after_cust_connect_on_start_of_month BETWEEN 31 AND 60 THEN '31 to 60 days'
WHEN days_after_cust_connect_on_start_of_month BETWEEN 61 AND 90 THEN '61 to 90 days'
ELSE '90+ Days'
END,
         guide_type,
         system__kma_desc
UNION ALL
SELECT last_day(partition_date_time) AS MONTH_END_DATE,
CASE
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 0 AND 30 THEN '0 to 30 days'
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 31 AND 60 THEN '31 to 60 days'
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 61 AND 90 THEN '61 to 90 days'
  ELSE '90+ Days'
END AS usage_period,
       guide_type,
       system__kma_desc,
       'Free' AS vod_type,
       count(DISTINCT account__number_aes256) AS distinct_households,
       sum(free_views) AS views,
       sum(free_view_duration_in_s) AS duration_in_s
FROM test_tmp.VOD_DAILY_AGG_NC_ANALYSIS
WHERE account__type='SUBSCRIBER'
  AND free_views>0
GROUP BY last_day(partition_date_time),
CASE
WHEN days_after_cust_connect_on_start_of_month BETWEEN 0 AND 30 THEN '0 to 30 days'
WHEN days_after_cust_connect_on_start_of_month BETWEEN 31 AND 60 THEN '31 to 60 days'
WHEN days_after_cust_connect_on_start_of_month BETWEEN 61 AND 90 THEN '61 to 90 days'
ELSE '90+ Days'
END,
         guide_type,
         system__kma_desc
UNION ALL
SELECT last_day(partition_date_time) AS MONTH_END_DATE,
CASE
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 0 AND 30 THEN '0 to 30 days'
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 31 AND 60 THEN '31 to 60 days'
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 61 AND 90 THEN '61 to 90 days'
  ELSE '90+ Days'
END AS usage_period,
       guide_type,
       system__kma_desc,
       'Premium' AS vod_type,
       count(DISTINCT account__number_aes256) AS distinct_households,
       sum(prem_views) AS views,
       sum(prem_view_duration_in_s) AS duration_in_s
FROM test_tmp.VOD_DAILY_AGG_NC_ANALYSIS
WHERE account__type='SUBSCRIBER'
  AND prem_views>0
GROUP BY last_day(partition_date_time),
CASE
WHEN days_after_cust_connect_on_start_of_month BETWEEN 0 AND 30 THEN '0 to 30 days'
WHEN days_after_cust_connect_on_start_of_month BETWEEN 31 AND 60 THEN '31 to 60 days'
WHEN days_after_cust_connect_on_start_of_month BETWEEN 61 AND 90 THEN '61 to 90 days'
ELSE '90+ Days'
END,
         guide_type,
         system__kma_desc
UNION ALL
SELECT last_day(partition_date_time) AS MONTH_END_DATE,
CASE
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 0 AND 30 THEN '0 to 30 days'
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 31 AND 60 THEN '31 to 60 days'
  WHEN days_after_cust_connect_on_start_of_month BETWEEN 61 AND 90 THEN '61 to 90 days'
  ELSE '90+ Days'
END AS usage_period,
       guide_type,
       system__kma_desc,
       'Transactional' AS vod_type,
       count(DISTINCT account__number_aes256) AS distinct_households,
       sum(trans_views) AS views,
       sum(total_view_duration_in_s) AS duration_in_s
FROM test_tmp.VOD_DAILY_AGG_NC_ANALYSIS
WHERE account__type='SUBSCRIBER'
  AND trans_views>0
GROUP BY last_day(partition_date_time),
CASE
WHEN days_after_cust_connect_on_start_of_month BETWEEN 0 AND 30 THEN '0 to 30 days'
WHEN days_after_cust_connect_on_start_of_month BETWEEN 31 AND 60 THEN '31 to 60 days'
WHEN days_after_cust_connect_on_start_of_month BETWEEN 61 AND 90 THEN '61 to 90 days'
ELSE '90+ Days'
END,
         guide_type,
         system__kma_desc;

--VOD USAGE monthly counts are stored in this table once per month
-- INSERT OVERWRITE TABLE VOD_USAGE_NEW_CONNECT_MONTHLY PARTITION(MONTH_END_DATE)

DROP TABLE IF EXISTS test_tmp.VOD_USAGE_NEW_CONNECT_MONTHLY;
CREATE TABLE test_tmp.VOD_USAGE_NEW_CONNECT_MONTHLY AS
SELECT
       A.usage_period as usage_period_by_start_of_month ,
       A.guide_type,
       A.system__kma_desc,
       A.vod_type,
       A.distinct_households,
       A.views,
       A.duration_in_s,
       B.deployed_hhs,
       A.month_end_date
FROM test_tmp.VOD_MONTHLY_AGG_TMP_NC A
JOIN test_tmp.DEPLOYED_HH_MONTHLY_NC B
ON A.month_end_date=B.month_end_date  AND A.guide_type=B.guide_type AND A.system__kma_desc=B.system__kma_desc
    AND a.usage_period=b.usage_period;
