DROP TABLE IF EXISTS tmp.churn_daily;

CREATE TABLE tmp.churn_daily AS

SELECT a.run_date AS churn_date,
       a.account__number_aes256,
       a.account__category,
       a.system__kma_desc,
       FROM_UNIXTIME(a.sg_account_deployed_timestamp, 'yyyy-MM-dd') AS deploy_date,
       DATEDIFF(run_date, FROM_UNIXTIME(a.sg_account_deployed_timestamp, 'yyyy-MM-dd')) + 1  AS tenure_days
FROM
(SELECT run_date,
        account__category,
        MAX(sg_account_deployed_timestamp) AS sg_account_deployed_timestamp,
        account__number_aes256,
        system__kma_desc
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date = DATE_ADD('${run_date}', -1)
AND account__type='SUBSCRIBER'
AND sg_account_deployed_timestamp IS NOT NULL
GROUP BY account__number_aes256, run_date, account__category, system__kma_desc ) a

LEFT OUTER JOIN
( SELECT
        account__number_aes256
FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE run_date = '${run_date}'
GROUP BY
        account__number_aes256
) b
ON a.account__number_aes256 = b.account__number_aes256
WHERE b.account__number_aes256 IS NULL;

INSERT INTO TABLE test.sg_churn_deployed

SELECT churn.churn_date,
        churn.account__number_aes256,
        churn.account__category,
        deployment.sg_deployed_type,
        churn.system__kma_desc,
        churn.deploy_date,
        churn.tenure_days
FROM tmp.churn_daily churn
JOIN
(SELECT account__number_aes256,
                MAX(sg_deployed_type) AS sg_deployed_type
                FROM PROD_LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
                WHERE run_date >= '2016-01-05'
                GROUP BY account__number_aes256
                ) deployment
ON churn.account__number_aes256 = deployment.account__number_aes256;

DROP TABLE IF EXISTS tmp.churn_daily;
