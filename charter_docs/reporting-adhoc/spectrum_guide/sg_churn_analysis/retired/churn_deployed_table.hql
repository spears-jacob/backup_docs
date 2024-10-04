CREATE TABLE test.churn_sg_accounts AS 
SELECT 'churned' AS is_active, 
    account__number_aes256,
    account__category,
    DATEDIFF(max_date, deploy_date) AS tenure
FROM 
( SELECT a.account__number_aes256, 
account__category,
a.max_date,
FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') AS deploy_date,
inactive_date
FROM
( SELECT MAX(run_date) AS max_date,
        MIN(account__category) AS account__category,
        MIN(sg_account_deployed_timestamp) AS sg_account_deployed_timestamp,
        MAX(record_inactive_date) AS inactive_date,
        account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date BETWEEN '2016-11-01' AND '2017-03-31'
AND account__type='SUBSCRIBER'
AND sg_deployed_type = 'NEW_CONNECT'
AND sg_account_deployed_timestamp IS NOT NULL
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin') 
GROUP BY account__number_aes256
) a

LEFT OUTER JOIN
( SELECT 
        account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date = '2017-04-01'
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin') 
GROUP BY 
        account__number_aes256
) b
ON a.account__number_aes256 = b.account__number_aes256
WHERE b.account__number_aes256 IS NULL

) d
WHERE deploy_date >= '2016-01-05'

UNION ALL 


SELECT 'not_churn' AS is_active,
        account__number_aes256,
        account__category,
        DATEDIFF(run_date, deploy_date) AS tenure
FROM
(
SELECT a.account__number_aes256, 
        account__category,
        run_date,
FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') AS deploy_date
FROM
( SELECT run_date,
        account__category,
        MIN(sg_account_deployed_timestamp) AS sg_account_deployed_timestamp,
        account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date = '2017-04-01'
AND account__type='SUBSCRIBER'
AND sg_deployed_type = 'NEW_CONNECT'
AND sg_account_deployed_timestamp IS NOT NULL
AND TRIM(system__kma_desc) IN ('Central States', 'Michigan', 'Northwest', 'Pacific Northwest', 'Sierra Nevada', 'Minnesota', 'Wisconsin') 
GROUP BY account__number_aes256,
        run_date,
        account__category ) a
) d
