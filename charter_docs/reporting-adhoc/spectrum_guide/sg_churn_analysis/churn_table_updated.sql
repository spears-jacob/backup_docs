STEPS TO RUN:
    1)adjust the deployment window dates if needed
      --sg_account_deployed_timestamp
      --last__customer_connect_date
    2)adjust the run date for the customer groupings as more data becomes available
      --run_date
    3)be conscious of the region filters
    4)run script
NOTES:
    --CUSTOMER TYPE IS NOT READILY AVAILABLE
    --THERE WAS A KMA CHANGE IN FEB 2017 THAT YOU HAVE TO ACCOUNT FOR
    --NEW CONNECT TO FORCE MIGRATION SWITCHING:
        --AT THIS TIME, WE OBSERVE THAT SOME CUSTOMERS START OFF AS NEW CONNECT CUSTOMERS BUT LATER BECOME FORCE MIGRATED CUSTOMERS. THE CAUSE IS UNKOWN.
        --2017 GROUPINGS: REGARDLESS OF WHAT IS LISTED IN THE BI SG DEPLOYED TYPE COLUMN, WE WILL CONSIDER ALL CUSTOMERS IN 2017 AS NEW CONNECTS - WE SHOULD NOT BE PERFORMING FORCE MIGRATIONS IN 2017.
        --WE WILL NOT FOCUS ON SG DEPLOYED STATUS. PLEASE PULL IN ALL ACCOUNTS REGARDLESS OF WHETHER THEY STARTED OFF AS A NEW CONNECT OR FORCE MIGRATION.

    --CUSTOMERS WITH FUTURE DEPLOYED DATES : TENURE IS NEGATIVE:
        --THERE ARE SG DEPLOYED TIMESTAMPS THAT ARE GREATER THAN THE ACTUAL DATE A CUSTOMER GOT SPEDCTRUM GUIDE SERVICE
        --THIS IS DUE TO THE FACT THAT WE GET THE DEPLOYED DATE INFORMATION FROM THE DMA TABLE WHICH CAN HAVE FUTURE DEPLOYED DATES
        --WE WILL CONSIDER THESE ACCOUNTS B/C THEY SHOW UP IN THE BI FEED AND THUS HAVE BEEN LABELED WITH SPECTRUM GUIDE.
        --DEPLOYED TIMESTAMP OR LAST_CUSTOMER_CONNECT_DATE MUST BE WITHING THE GROUPING TIMEFRAME.
    --CUSTOMER REQUIREMENTS:
        --A CUSTOMER MUST BE PRESENT ON THE FIRST OF EACH MONTH
        --CUSTOMERS THAT ONBOARD IN THE MIDDLE OF THE MONTH WILL NOT BE COUNTED IN THE CURRENT MONTH. THEY WILL BE COUNTED IN THE NEXT MONTH IF THEY ARE PRESENT ON THE FIRST DAY OF THE NEXT MONTH.
        --IF A CUSTOMER IS ONBOARDED IN THE MIDDLE OF THE MONTH, IT IS NOT PRESENT IN THAT MONTH
        --IF A CUSTOMER IS NOT PRESENT ON THE LAST DAY OF THE MONTH, IT IS CHURNED.

TESTING:
    -- SELECT *
    -- FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
    -- WHERE account__number_aes256 IN ('+++bj7oPakd5dGGbmjAT9OYul2G6IuNSEZDUIHR7yhQ=')

-- =============
-- CUSTOMER GROUPINGS
-- =============
DROP TABLE IF EXISTS test.customer_base_first_grouping;
CREATE TABLE test.customer_base_first_grouping AS

SELECT run_date, account__category, sg_account_deployed_timestamp, sg_deployed_type, record_inactive_date,system__kma_desc, account__type, account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date BETWEEN '2017-01-01' AND '2017-07-31'
AND (
    (FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') BETWEEN '2017-01-01' AND '2017-04-30')
    OR (last__customer_connect_date BETWEEN '2017-01-01' AND '2017-04-30')
    )

;

DROP TABLE IF EXISTS test.customer_base_second_grouping;
CREATE TABLE test.customer_base_second_grouping AS

SELECT run_date, account__category, sg_account_deployed_timestamp, sg_deployed_type, record_inactive_date,system__kma_desc, account__type, account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date BETWEEN '2017-05-01' AND '2017-07-31'
AND (
    (FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') BETWEEN '2017-05-01' AND '2017-07-31')
    OR (last__customer_connect_date BETWEEN '2017-05-01' AND '2017-07-31')
    )
;

DROP TABLE IF EXISTS test.customer_base_overall;
CREATE TABLE test.customer_base_overall AS

SELECT run_date, account__category, sg_account_deployed_timestamp, sg_deployed_type, record_inactive_date,system__kma_desc, account__type, account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date BETWEEN '2016-05-01' AND '2017-10-31'
AND (
    (FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') BETWEEN '2016-05-01' AND '2017-07-31')
    OR (last__customer_connect_date BETWEEN '2016-05-01' AND '2017-07-31')
    )
;
-- =============
-- MONTHLY CALCULATIONS
-- =============

-- =============
-- FIRST GROUPING
-- =============

DROP TABLE IF EXISTS test.churn_sg_accounts_monthly_first_grouping;
CREATE TABLE test.churn_sg_accounts_monthly_first_grouping AS

--GENERATE A LIST OF ALL CUSTOMERS IN A TIMEFRAME THAT ARE NO LONGER SPECTRUM GUIDE CUSTOMERS
SELECT
    month_start AS timeframe,
    'churned' AS is_active,
    account__number_aes256,
    account__category,
    DATEDIFF(max_date, deploy_date) AS tenure

FROM

( SELECT
    a.month_start,
    a.account__number_aes256,
    account__category,
    a.max_date,
    FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') AS deploy_date,
    inactive_date

FROM

( SELECT
        run_date AS month_start,
        MAX(run_date) AS max_date,
        MIN(account__category) AS account__category,
        MIN(sg_account_deployed_timestamp) AS sg_account_deployed_timestamp,
        MAX(record_inactive_date) AS inactive_date,
        account__number_aes256
FROM test.customer_base_first_grouping
WHERE run_date = CONCAT(from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'YYYY'), '-', from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'MM'), '-', '01')
AND account__type = 'SUBSCRIBER'
AND sg_account_deployed_timestamp IS NOT NULL
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
GROUP BY
    run_date,
    account__number_aes256
) a

LEFT OUTER JOIN

--GENERATE A LIST OF ALL CUSTOMERS ACTIVE ONE DAY AFTER ABOVE TIMEFRAME
--IF AN ACCOUNT DOES NOT SHOWS UP, IT IS NO LONGER A SPECTRUM GUIDE CUSTOMER
( SELECT
        ADD_MONTHS(DATE_ADD(LAST_DAY(run_date), 1), -1) AS month_start,
        account__number_aes256
FROM test.customer_base_first_grouping
WHERE run_date = LAST_DAY(run_date)
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
GROUP BY
        run_date,
        account__number_aes256
) b
ON a.account__number_aes256 = b.account__number_aes256
AND a.month_start = b.month_start
WHERE b.account__number_aes256 IS NULL

) d

WHERE deploy_date >= '2016-01-05'


UNION ALL

--GENERATE LIST OF CUSTOMERS THAT HAVE NOT CHURNED

SELECT
    month_start AS timeframe,
    'not churned' AS is_active,
    account__number_aes256,
    account__category,
    DATEDIFF(max_date, deploy_date) AS tenure

FROM

( SELECT
    a.month_start,
    a.account__number_aes256,
    account__category,
    a.max_date,
    FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') AS deploy_date,
    inactive_date

FROM
--GENERATE LIST OF CUSTOMERS IN TIMEFRAME
( SELECT
        run_date AS month_start,
        MAX(run_date) AS max_date,
        MIN(account__category) AS account__category,
        MIN(sg_account_deployed_timestamp) AS sg_account_deployed_timestamp,
        MAX(record_inactive_date) AS inactive_date,
        account__number_aes256
FROM test.customer_base_first_grouping
WHERE run_date = CONCAT(from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'YYYY'), '-', from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'MM'), '-', '01')
AND account__type='SUBSCRIBER'
-- AND sg_deployed_type = 'NEW_CONNECT'
AND sg_account_deployed_timestamp IS NOT NULL
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
-- AND account__number_aes256 IN ('+++bj7oPakd5dGGbmjAT9OYul2G6IuNSEZDUIHR7yhQ=')
GROUP BY
        run_date,
        account__number_aes256
) a

LEFT OUTER JOIN

--GENERATE A LIST OF ALL CUSTOMERS ACTIVE ONE DAY AFTER ABOVE TIMEFRAME
--IF AN ACCOUNT SHOWS UP, IT IS STILL A SPECTRUM GUIDE CUSTOMER
( SELECT
        ADD_MONTHS(DATE_ADD(LAST_DAY(run_date), 1), -1) AS month_start,
        account__number_aes256
FROM test.customer_base_first_grouping
-- WHERE run_date = '2017-02-01'
WHERE run_date = LAST_DAY(run_date)
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
-- AND account__number_aes256 IN ('+++bj7oPakd5dGGbmjAT9OYul2G6IuNSEZDUIHR7yhQ=')
GROUP BY
        run_date,
        account__number_aes256
) b
ON a.account__number_aes256 = b.account__number_aes256
AND a.month_start = b.month_start
WHERE b.account__number_aes256 IS NOT NULL

) d

WHERE deploy_date >= '2016-01-05'
;

-- =============
-- SECOND GROUPING
-- =============

DROP TABLE IF EXISTS test.churn_sg_accounts_monthly_second_grouping;
CREATE TABLE test.churn_sg_accounts_monthly_second_grouping AS


SELECT
    month_start AS timeframe,
    'churned' AS is_active,
    account__number_aes256,
    account__category,
    DATEDIFF(max_date, deploy_date) AS tenure

FROM

( SELECT
    a.month_start,
    a.account__number_aes256,
    account__category,
    a.max_date,
    FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') AS deploy_date,
    inactive_date

FROM

( SELECT
        run_date AS month_start,
        MAX(run_date) AS max_date,
        MIN(account__category) AS account__category,
        MIN(sg_account_deployed_timestamp) AS sg_account_deployed_timestamp,
        MAX(record_inactive_date) AS inactive_date,
        account__number_aes256
FROM test.customer_base_second_grouping
WHERE run_date = CONCAT(from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'YYYY'), '-', from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'MM'), '-', '01')
AND account__type = 'SUBSCRIBER'
AND sg_account_deployed_timestamp IS NOT NULL
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
GROUP BY
    run_date,
    account__number_aes256
) a

LEFT OUTER JOIN

--GENERATE A LIST OF ALL CUSTOMERS ACTIVE ONE DAY AFTER ABOVE TIMEFRAME
--IF AN ACCOUNT DOES NOT SHOWS UP, IT IS NO LONGER A SPECTRUM GUIDE CUSTOMER
( SELECT
        ADD_MONTHS(DATE_ADD(LAST_DAY(run_date), 1), -1) AS month_start,
        account__number_aes256
FROM test.customer_base_second_grouping
WHERE run_date = LAST_DAY(run_date)
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
GROUP BY
        run_date,
        account__number_aes256
) b
ON a.account__number_aes256 = b.account__number_aes256
AND a.month_start = b.month_start
WHERE b.account__number_aes256 IS NULL

) d

WHERE deploy_date >= '2016-01-05'


UNION ALL

--GENERATE LIST OF CUSTOMERS THAT HAVE NOT CHURNED

SELECT
    month_start AS timeframe,
    'not churned' AS is_active,
    account__number_aes256,
    account__category,
    DATEDIFF(max_date, deploy_date) AS tenure

FROM

( SELECT
    a.month_start,
    a.account__number_aes256,
    account__category,
    a.max_date,
    FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') AS deploy_date,
    inactive_date

FROM
--GENERATE LIST OF CUSTOMERS IN TIMEFRAME
( SELECT
        run_date AS month_start,
        MAX(run_date) AS max_date,
        MIN(account__category) AS account__category,
        MIN(sg_account_deployed_timestamp) AS sg_account_deployed_timestamp,
        MAX(record_inactive_date) AS inactive_date,
        account__number_aes256
FROM test.customer_base_second_grouping
WHERE run_date = CONCAT(from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'YYYY'), '-', from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'MM'), '-', '01')
AND account__type='SUBSCRIBER'
-- AND sg_deployed_type = 'NEW_CONNECT'
AND sg_account_deployed_timestamp IS NOT NULL
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
-- AND account__number_aes256 IN ('+++bj7oPakd5dGGbmjAT9OYul2G6IuNSEZDUIHR7yhQ=')
GROUP BY
        run_date,
        account__number_aes256
) a

LEFT OUTER JOIN

--GENERATE A LIST OF ALL CUSTOMERS ACTIVE ONE DAY AFTER ABOVE TIMEFRAME
--IF AN ACCOUNT SHOWS UP, IT IS STILL A SPECTRUM GUIDE CUSTOMER
( SELECT
        ADD_MONTHS(DATE_ADD(LAST_DAY(run_date), 1), -1) AS month_start,
        account__number_aes256
FROM test.customer_base_second_grouping
-- WHERE run_date = '2017-02-01'
WHERE run_date = LAST_DAY(run_date)
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
-- AND account__number_aes256 IN ('+++bj7oPakd5dGGbmjAT9OYul2G6IuNSEZDUIHR7yhQ=')
GROUP BY
        run_date,
        account__number_aes256
) b
ON a.account__number_aes256 = b.account__number_aes256
AND a.month_start = b.month_start
WHERE b.account__number_aes256 IS NOT NULL

) d

WHERE deploy_date >= '2016-01-05'
;

-- =============
-- OVERALL GROUPING:
-- =============

DROP TABLE IF EXISTS test.churn_sg_accounts_monthly_overall;
CREATE TABLE test.churn_sg_accounts_monthly_overall AS


SELECT
    month_start AS timeframe,
    'churned' AS is_active,
    account__number_aes256,
    account__category,
    DATEDIFF(max_date, deploy_date) AS tenure

FROM

( SELECT
    a.month_start,
    a.account__number_aes256,
    account__category,
    a.max_date,
    FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') AS deploy_date,
    inactive_date

FROM

( SELECT
        run_date AS month_start,
        MAX(run_date) AS max_date,
        MIN(account__category) AS account__category,
        MIN(sg_account_deployed_timestamp) AS sg_account_deployed_timestamp,
        MAX(record_inactive_date) AS inactive_date,
        account__number_aes256
FROM test.customer_base_overall
WHERE run_date = CONCAT(from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'YYYY'), '-', from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'MM'), '-', '01')
AND account__type = 'SUBSCRIBER'
-- AND sg_deployed_type = 'NEW_CONNECT'
AND sg_account_deployed_timestamp IS NOT NULL
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
GROUP BY
    run_date,
    account__number_aes256
) a

LEFT OUTER JOIN

--GENERATE A LIST OF ALL CUSTOMERS ACTIVE ONE DAY AFTER ABOVE TIMEFRAME
--IF AN ACCOUNT DOES NOT SHOWS UP, IT IS NO LONGER A SPECTRUM GUIDE CUSTOMER
--YOU DO NOT SPECIFY NEW CONNECTS HERE B/C SOMETIMES ACCOUNTS GO FROM NEW CONNECT TO FM IN THE DATA WHICH SHOULD NOT HAPPEN
( SELECT
        ADD_MONTHS(DATE_ADD(LAST_DAY(run_date), 1), -1) AS month_start,
        account__number_aes256
FROM test.customer_base_overall
WHERE run_date = LAST_DAY(run_date)
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
GROUP BY
        run_date,
        account__number_aes256
) b
ON a.account__number_aes256 = b.account__number_aes256
AND a.month_start = b.month_start
WHERE b.account__number_aes256 IS NULL

) d

WHERE deploy_date >= '2016-01-05'


UNION ALL

--GENERATE LIST OF CUSTOMERS THAT HAVE NOT CHURNED

SELECT
    month_start AS timeframe,
    'not churned' AS is_active,
    account__number_aes256,
    account__category,
    DATEDIFF(max_date, deploy_date) AS tenure

FROM

( SELECT
    a.month_start,
    a.account__number_aes256,
    account__category,
    a.max_date,
    FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') AS deploy_date,
    inactive_date

FROM
--GENERATE LIST OF CUSTOMERS IN TIMEFRAME
( SELECT
        run_date AS month_start,
        MAX(run_date) AS max_date,
        MIN(account__category) AS account__category,
        MIN(sg_account_deployed_timestamp) AS sg_account_deployed_timestamp,
        MAX(record_inactive_date) AS inactive_date,
        account__number_aes256
FROM test.customer_base_overall
WHERE run_date = CONCAT(from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'YYYY'), '-', from_unixtime(unix_timestamp(run_date,'yyyy-MM-dd'),'MM'), '-', '01') -- checks first day in a month 
AND account__type='SUBSCRIBER'
-- AND sg_deployed_type = 'NEW_CONNECT'
AND sg_account_deployed_timestamp IS NOT NULL
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
-- AND account__number_aes256 IN ('+++bj7oPakd5dGGbmjAT9OYul2G6IuNSEZDUIHR7yhQ=')
GROUP BY
        run_date,
        account__number_aes256
) a

LEFT OUTER JOIN

--GENERATE A LIST OF ALL CUSTOMERS ACTIVE ONE DAY AFTER ABOVE TIMEFRAME
--IF AN ACCOUNT SHOWS UP, IT IS STILL A SPECTRUM GUIDE CUSTOMER
--YOU DO NOT SPECIFY NEW CONNECTS HERE B/C SOMETIMES ACCOUNTS GO FROM NEW CONNECT TO FM IN THE DATA WHICH SHOULD NOT HAPPEN
( SELECT
        ADD_MONTHS(DATE_ADD(LAST_DAY(run_date), 1), -1) AS month_start,
        account__number_aes256
FROM test.customer_base_overall
-- WHERE run_date = '2017-02-01'
WHERE run_date = LAST_DAY(run_date) -- checks end of month day
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin')
-- AND account__number_aes256 IN ('+++bj7oPakd5dGGbmjAT9OYul2G6IuNSEZDUIHR7yhQ=')
GROUP BY
        run_date,
        account__number_aes256
) b
ON a.account__number_aes256 = b.account__number_aes256
AND a.month_start = b.month_start
WHERE b.account__number_aes256 IS NOT NULL

) d

WHERE deploy_date >= '2016-01-05'
;


-- =============
-- AGG TABLE AGGREGATION
-- =============

DROP TABLE IF EXISTS test.churn_sg_accounts_monthly_groupings;
CREATE TABLE test.churn_sg_accounts_monthly_groupings AS

SELECT
    'jan to april' AS customer_grouping,
    *
FROM test.churn_sg_accounts_monthly_first_grouping

UNION ALL

SELECT
    'may to july' AS customer_grouping,
    *
FROM test.churn_sg_accounts_monthly_second_grouping

UNION ALL

SELECT
    'jan to july' AS customer_grouping,
    *
FROM test.churn_sg_accounts_monthly_overall
;


SELECT customer_grouping, timeframe, is_active, account__category, SIZE (COLLECT_SET(account__number_aes256)) accounts
FROM test.churn_sg_accounts_monthly_groupings
GROUP BY customer_grouping, timeframe, is_active, account__category
;
