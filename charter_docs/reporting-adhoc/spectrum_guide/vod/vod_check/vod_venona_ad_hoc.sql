-- =============
-- FOR NEW CONNECTS IN JULY WHAT ERRORS AND VENONA USAGE DID THEY ENCOUNTER
-- =============

DROP TABLE IF EXISTS test_tmp.customer_base_july;
CREATE TABLE test_tmp.customer_base_july AS

SELECT  run_date, account__category, sg_account_deployed_timestamp, sg_deployed_type, record_inactive_date,system__kma_desc, account__type, account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date BETWEEN '2017-07-01' AND '2017-09-04' --> We want to see how this grouping performed for the rest of the year
AND (
    (FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') BETWEEN '2017-07-01' AND '2017-07-31')
    OR (last__customer_connect_date BETWEEN '2017-07-01' AND '2017-07-31')
    )

;

DROP TABLE IF EXISTS test_tmp.customer_base_june;
CREATE TABLE test_tmp.customer_base_june AS

SELECT  run_date, account__category, sg_account_deployed_timestamp, sg_deployed_type, record_inactive_date,system__kma_desc, account__type, account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date BETWEEN '2017-06-01' AND '2017-09-04' --> We want to see how this grouping performed for the rest of the year
AND (
    (FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') BETWEEN '2017-06-01' AND '2017-06-30')
    OR (last__customer_connect_date BETWEEN '2017-06-01' AND '2017-06-30')
    )

;

DROP TABLE IF EXISTS test_tmp.customer_base_july;
CREATE TABLE test_tmp.customer_base_july AS

SELECT  run_date, account__category, sg_account_deployed_timestamp, sg_deployed_type, record_inactive_date,system__kma_desc, account__type, account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date BETWEEN '2017-07-01' AND '2017-09-04' --> We want to see how this grouping performed for the rest of the year
AND (
    (FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') BETWEEN '2017-07-01' AND '2017-07-31')
    OR (last__customer_connect_date BETWEEN '2017-07-01' AND '2017-07-31')
    )

;

DROP TABLE IF EXISTS test_tmp.customer_base;
CREATE TABLE test_tmp.customer_base AS

SELECT 'July 2017' AS customer_grouping, run_date AS partition_date_denver, system__kma_desc, SIZE(COLLECT_SET(account__number_aes256)) accounts
FROM test_tmp.customer_base_july
WHERE account__type = 'SUBSCRIBER'
-- AND customer__type = 'Residential'
GROUP BY 'July 2017', run_date, system__kma_desc

UNION ALL

SELECT 'June 2017' AS customer_grouping, run_date AS partition_date_denver, system__kma_desc, SIZE(COLLECT_SET(account__number_aes256)) accounts
FROM test_tmp.customer_base_june
WHERE account__type = 'SUBSCRIBER'
-- AND customer__type = 'Residential'
GROUP BY 'June 2017', run_date, system__kma_desc
;



-- =============
-- CHECK ERRORS
-- =============

DROP TABLE IF EXISTS test_tmp.customer_base_july_errors;
CREATE TABLE test_tmp.customer_base_july_errors AS

SELECT
      'July 2017' AS customer_grouping,
      'errors' AS metric,
      venona.partition_date_denver,
      venona.system__kma_desc,
      venona.metric_name AS error_type,
      SIZE(COLLECT_SET(venona.account__number_aes256)) accounts,
      SUM(venona.event_counts) instance_count

FROM prod.venona_sg_p2_errors_pageviews_by_account_daily venona
WHERE venona.account_type = 'SUBSCRIBER'
      -- AND venona.customer__type = 'Residential'
      AND venona.category_type = 'error'
      AND partition_date_denver >= '2017-07-01'
      AND venona.account__number_aes256 IN
        (SELECT account__number_aes256 FROM test_tmp.customer_base_july GROUP BY account__number_aes256)

GROUP BY
      'July 2017',
      'errors',
       venona.partition_date_denver,
      venona.system__kma_desc,
      venona.metric_name
;


DROP TABLE IF EXISTS test_tmp.customer_base_june_errors;
CREATE TABLE test_tmp.customer_base_june_errors AS

SELECT
      'June 2017' AS customer_grouping,
      'errors' AS metric,
      venona.partition_date_denver,
      venona.system__kma_desc,
      venona.metric_name AS error_type,
      SIZE(COLLECT_SET(venona.account__number_aes256)) accounts,
      SUM(venona.event_counts) instance_count

FROM prod.venona_sg_p2_errors_pageviews_by_account_daily venona
WHERE venona.account_type = 'SUBSCRIBER'
      -- AND venona.customer__type = 'Residential'
      AND venona.category_type = 'error'
      AND partition_date_denver >= '2017-06-01'
      AND venona.account__number_aes256 IN
        (SELECT account__number_aes256 FROM test_tmp.customer_base_june GROUP BY account__number_aes256)

GROUP BY
      'June 2017',
      'errors',
       venona.partition_date_denver,
      venona.system__kma_desc,
      venona.metric_name
;

DROP TABLE IF EXISTS test_tmp.customer_base_errors;
CREATE TABLE test_tmp.customer_base_errors AS

SELECT * FROM test_tmp.customer_base_july_errors

UNION ALL

SELECT * FROM test_tmp.customer_base_june_errors

;

-- =============
-- CHECK ACTIVITY: ASSUME THEY HAVE TO HAVE USED SOME PAGE IN THE CLOUD UI
-- =============
DROP TABLE IF EXISTS test_tmp.customer_base_july_pageviews;
CREATE TABLE test_tmp.customer_base_july_pageviews AS

SELECT
          'July 2017' AS customer_grouping,
          'pageviews' AS metric,
          venona.partition_date_denver,
          venona.system__kma_desc,
          SIZE(COLLECT_SET(venona.account__number_aes256)) accounts,
          SUM(venona.event_counts) AS total_page_view_count

        FROM prod.venona_sg_p2_errors_pageviews_by_account_daily venona
        WHERE venona.category_type = 'pageview'
          AND venona.account_type ='SUBSCRIBER'
          -- AND venona.customer__type = 'Residential'
          AND venona.partition_date_denver >= '2017-07-01'
          AND venona.account__number_aes256 IN
            (SELECT account__number_aes256 FROM test_tmp.customer_base_july GROUP BY account__number_aes256)

GROUP BY
          'July 2017',
          'pageviews',
          venona.partition_date_denver,
          venona.system__kma_desc
;


DROP TABLE IF EXISTS test_tmp.customer_base_june_pageviews;
CREATE TABLE test_tmp.customer_base_june_pageviews AS

SELECT
          'June 2017' AS customer_grouping,
          'pageviews' AS metric,
          venona.partition_date_denver,
          venona.system__kma_desc,
          SIZE(COLLECT_SET(venona.account__number_aes256)) accounts,
          SUM(venona.event_counts) AS total_page_view_count

        FROM prod.venona_sg_p2_errors_pageviews_by_account_daily venona
        WHERE venona.category_type = 'pageview'
          AND venona.account_type ='SUBSCRIBER'
          -- AND venona.customer__type = 'Residential'
          AND venona.partition_date_denver >= '2017-06-01'
          AND venona.account__number_aes256 IN
            (SELECT account__number_aes256 FROM test_tmp.customer_base_june GROUP BY account__number_aes256)

GROUP BY
          'June 2017',
          'pageviews',
          venona.partition_date_denver,
          venona.system__kma_desc
;


DROP TABLE IF EXISTS test_tmp.customer_base_pageviews;
CREATE TABLE test_tmp.customer_base_pageviews AS

SELECT * FROM test_tmp.customer_base_july_pageviews

UNION ALL

SELECT * FROM test_tmp.customer_base_june_pageviews

;
